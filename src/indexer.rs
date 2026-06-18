use crate::server::{PathItem, PathType};
use crate::utils::get_file_name;

use anyhow::{bail, Result};
use duckdb::{params, Connection};
use ignore::WalkBuilder;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Serialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

const INDEX_SCAN_BATCH_SIZE: usize = 128;
const INDEX_SCAN_TARGET_LATENCY_MS: u64 = 100;
const INDEX_SCAN_MAX_DELAY_MS: u64 = 100;

#[derive(Debug, Default)]
pub struct ServerLoad {
    active_requests: AtomicUsize,
    active_file_streams: Arc<AtomicUsize>,
    latency_ewma_ms: AtomicU64,
}

impl ServerLoad {
    pub fn active_file_streams(&self) -> Arc<AtomicUsize> {
        self.active_file_streams.clone()
    }

    pub fn begin_request(&self) {
        self.active_requests.fetch_add(1, Ordering::SeqCst);
    }

    pub fn end_request(&self, elapsed: Duration) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
        let elapsed_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let current = self.latency_ewma_ms.load(Ordering::SeqCst);
        let next = if current == 0 {
            elapsed_ms
        } else {
            (current * 7 + elapsed_ms) / 8
        };
        self.latency_ewma_ms.store(next, Ordering::SeqCst);
    }

    fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
    }

    fn active_file_stream_count(&self) -> usize {
        self.active_file_streams.load(Ordering::SeqCst)
    }

    fn latency_ewma_ms(&self) -> u64 {
        self.latency_ewma_ms.load(Ordering::SeqCst)
    }
}

#[derive(Clone)]
pub struct Indexer {
    tx: mpsc::Sender<IndexCommand>,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

enum IndexCommand {
    FullScan,
    ScanPath(PathBuf),
    UpsertPath(PathBuf),
    RemovePath(PathBuf),
    MovePath {
        from: PathBuf,
        to: PathBuf,
    },
    Search {
        base: PathBuf,
        q: String,
        limit: u64,
        reply: oneshot::Sender<Result<Vec<PathItem>>>,
    },
    Query {
        sql: String,
        limit: u64,
        path_filters: Vec<String>,
        reply: oneshot::Sender<Result<QueryResult>>,
    },
}

impl Indexer {
    pub fn new(
        serve_path: PathBuf,
        db_path: PathBuf,
        hidden: Vec<String>,
        follow_symlinks: bool,
        watch: bool,
        scan_interval: u64,
        running: Arc<AtomicBool>,
        load: Arc<ServerLoad>,
    ) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let (tx, rx) = mpsc::channel();
        let watch_path = serve_path.clone();
        let worker_tx = tx.clone();
        let worker_running = running.clone();
        thread::spawn(move || {
            if let Err(err) = run_worker(
                rx,
                serve_path.clone(),
                db_path,
                hidden,
                follow_symlinks,
                load,
                worker_running,
            ) {
                error!("indexer stopped: {err}");
            }
        });
        let indexer = Self { tx };
        indexer.full_scan();
        if watch {
            indexer.start_watcher(watch_path, worker_tx.clone(), running.clone());
        }
        if scan_interval > 0 {
            start_periodic_scan(worker_tx, scan_interval, running);
        }
        Ok(indexer)
    }

    pub fn default_db_path(serve_path: &Path) -> PathBuf {
        serve_path.join(".dufs").join("index.duckdb")
    }

    pub fn full_scan(&self) {
        let _ = self.tx.send(IndexCommand::FullScan);
    }

    pub fn scan_path<P: AsRef<Path>>(&self, path: P) {
        let _ = self
            .tx
            .send(IndexCommand::ScanPath(path.as_ref().to_path_buf()));
    }

    pub fn upsert_path<P: AsRef<Path>>(&self, path: P) {
        let _ = self
            .tx
            .send(IndexCommand::UpsertPath(path.as_ref().to_path_buf()));
    }

    pub fn remove_path<P: AsRef<Path>>(&self, path: P) {
        let _ = self
            .tx
            .send(IndexCommand::RemovePath(path.as_ref().to_path_buf()));
    }

    pub fn move_path<P: AsRef<Path>>(&self, from: P, to: P) {
        let _ = self.tx.send(IndexCommand::MovePath {
            from: from.as_ref().to_path_buf(),
            to: to.as_ref().to_path_buf(),
        });
    }

    pub async fn search<P: AsRef<Path>>(
        &self,
        base: P,
        q: String,
        limit: u64,
    ) -> Result<Vec<PathItem>> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(IndexCommand::Search {
            base: base.as_ref().to_path_buf(),
            q,
            limit,
            reply,
        })?;
        rx.await?
    }

    pub async fn query(
        &self,
        sql: String,
        limit: u64,
        path_filters: Vec<String>,
    ) -> Result<QueryResult> {
        let (reply, rx) = oneshot::channel();
        self.tx.send(IndexCommand::Query {
            sql,
            limit,
            path_filters,
            reply,
        })?;
        rx.await?
    }

    fn start_watcher(
        &self,
        watch_path: PathBuf,
        tx: mpsc::Sender<IndexCommand>,
        running: Arc<AtomicBool>,
    ) {
        thread::spawn(move || {
            let callback_tx = tx.clone();
            let mut watcher = match RecommendedWatcher::new(
                move |res: notify::Result<notify::Event>| match res {
                    Ok(event) => handle_notify_event(&callback_tx, event),
                    Err(err) => warn!("index watcher error: {err}"),
                },
                notify::Config::default(),
            ) {
                Ok(watcher) => watcher,
                Err(err) => {
                    error!("failed to create index watcher: {err}");
                    return;
                }
            };
            if let Err(err) = watcher.watch(&watch_path, RecursiveMode::Recursive) {
                error!("failed to watch {}: {err}", watch_path.display());
                return;
            }
            while running.load(Ordering::SeqCst) {
                thread::sleep(Duration::from_secs(1));
            }
        });
    }
}

fn start_periodic_scan(
    tx: mpsc::Sender<IndexCommand>,
    scan_interval: u64,
    running: Arc<AtomicBool>,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(scan_interval));
        while running.load(Ordering::SeqCst) {
            interval.tick().await;
            let _ = tx.send(IndexCommand::FullScan);
        }
    });
}

fn run_worker(
    rx: mpsc::Receiver<IndexCommand>,
    serve_path: PathBuf,
    db_path: PathBuf,
    hidden: Vec<String>,
    follow_symlinks: bool,
    load: Arc<ServerLoad>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut db = IndexDb::new(serve_path, db_path, hidden, follow_symlinks, load)?;
    while running.load(Ordering::SeqCst) {
        let Ok(cmd) = rx.recv_timeout(Duration::from_secs(1)) else {
            continue;
        };
        match cmd {
            IndexCommand::FullScan => {
                if let Err(err) = db.full_scan() {
                    warn!("failed to scan index: {err}");
                }
            }
            IndexCommand::ScanPath(path) => {
                if let Err(err) = db.scan_path(&path) {
                    warn!("failed to scan {}: {err}", path.display());
                }
            }
            IndexCommand::UpsertPath(path) => {
                if let Err(err) = db.upsert_path(&path) {
                    warn!("failed to index {}: {err}", path.display());
                }
            }
            IndexCommand::RemovePath(path) => {
                if let Err(err) = db.remove_path(&path) {
                    warn!("failed to remove {} from index: {err}", path.display());
                }
            }
            IndexCommand::MovePath { from, to } => {
                if let Err(err) = db.remove_path(&from).and_then(|_| db.scan_path(&to)) {
                    warn!(
                        "failed to move index entry {} -> {}: {err}",
                        from.display(),
                        to.display()
                    );
                }
            }
            IndexCommand::Search {
                base,
                q,
                limit,
                reply,
            } => {
                let _ = reply.send(db.search(&base, &q, limit));
            }
            IndexCommand::Query {
                sql,
                limit,
                path_filters,
                reply,
            } => {
                let _ = reply.send(db.query(&sql, limit, &path_filters));
            }
        }
    }
    Ok(())
}

struct IndexDb {
    conn: Connection,
    serve_path: PathBuf,
    hidden: Vec<String>,
    follow_symlinks: bool,
    generation: u64,
    throttle: IndexThrottle,
}

struct IndexThrottle {
    load: Arc<ServerLoad>,
    batch_size: usize,
    processed: usize,
}

impl IndexThrottle {
    fn new(load: Arc<ServerLoad>) -> Self {
        Self {
            load,
            batch_size: INDEX_SCAN_BATCH_SIZE,
            processed: 0,
        }
    }

    fn step(&mut self) {
        self.processed += 1;
        if self.processed < self.batch_size {
            return;
        }
        self.processed = 0;
        let delay = self.delay();
        if delay > Duration::ZERO {
            thread::sleep(delay);
        } else {
            thread::yield_now();
        }
    }

    fn delay(&self) -> Duration {
        let active_requests = self.load.active_requests() as u64;
        let active_file_streams = self.load.active_file_stream_count() as u64;
        let latency = self.load.latency_ewma_ms();
        let mut delay_ms = 0;
        if active_requests > 0 {
            delay_ms += 5 * active_requests.min(4);
        }
        if active_file_streams > 0 {
            delay_ms += 25 * active_file_streams.min(4);
        }
        if latency > INDEX_SCAN_TARGET_LATENCY_MS {
            delay_ms += (latency - INDEX_SCAN_TARGET_LATENCY_MS).min(INDEX_SCAN_MAX_DELAY_MS);
        }
        Duration::from_millis(delay_ms.min(INDEX_SCAN_MAX_DELAY_MS))
    }
}

impl IndexDb {
    fn new(
        serve_path: PathBuf,
        db_path: PathBuf,
        hidden: Vec<String>,
        follow_symlinks: bool,
        load: Arc<ServerLoad>,
    ) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS files (
                path TEXT PRIMARY KEY,
                parent TEXT NOT NULL,
                name TEXT NOT NULL,
                path_type TEXT NOT NULL,
                size UBIGINT NOT NULL,
                mtime UBIGINT NOT NULL,
                hidden BOOLEAN NOT NULL,
                scan_generation UBIGINT NOT NULL DEFAULT 0,
                indexed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
            );
            CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent);
            CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);",
        )?;
        let _ = conn.execute(
            "ALTER TABLE files ADD COLUMN scan_generation UBIGINT DEFAULT 0",
            [],
        );
        let generation = conn.query_row(
            "SELECT coalesce(max(scan_generation), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        Ok(Self {
            conn,
            serve_path,
            hidden,
            follow_symlinks,
            generation,
            throttle: IndexThrottle::new(load),
        })
    }

    fn full_scan(&mut self) -> Result<()> {
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;
        self.scan_path_with_generation(&self.serve_path.clone(), generation)?;
        self.conn.execute(
            "DELETE FROM files WHERE scan_generation <> ?",
            params![generation],
        )?;
        Ok(())
    }

    fn scan_path(&mut self, path: &Path) -> Result<()> {
        self.scan_path_with_generation(path, self.generation)
    }

    fn scan_path_with_generation(&mut self, path: &Path, generation: u64) -> Result<()> {
        if path.is_dir() {
            for result in WalkBuilder::new(path)
                .hidden(false)
                .follow_links(true)
                .git_ignore(true)
                .git_global(true)
                .ignore(true)
                .build()
            {
                let Ok(entry) = result else {
                    continue;
                };
                let entry_path = entry.path();
                if entry_path == self.serve_path {
                    continue;
                }
                self.upsert_path_with_generation(entry_path, generation)?;
                self.throttle.step();
            }
        } else {
            self.upsert_path_with_generation(path, generation)?;
            self.throttle.step();
        }
        Ok(())
    }

    fn upsert_path(&mut self, path: &Path) -> Result<()> {
        self.upsert_path_with_generation(path, self.generation)
    }

    fn upsert_path_with_generation(&mut self, path: &Path, generation: u64) -> Result<()> {
        if !path.exists() {
            return self.remove_path(path);
        }
        if path.starts_with(self.serve_path.join(".dufs")) {
            return Ok(());
        }
        let meta = std::fs::metadata(path)?;
        let symlink_meta = std::fs::symlink_metadata(path)?;
        if symlink_meta.is_symlink() && !self.follow_symlinks {
            let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            if !canonical.starts_with(&self.serve_path) {
                return Ok(());
            }
        }
        let is_dir = meta.is_dir();
        let name = get_file_name(path);
        if crate::server::is_hidden_path(&self.hidden, name, is_dir) {
            if is_dir {
                self.remove_path(path)?;
            }
            return Ok(());
        }
        let path_type = match (symlink_meta.is_symlink(), is_dir) {
            (true, true) => "SymlinkDir",
            (false, true) => "Dir",
            (true, false) => "SymlinkFile",
            (false, false) => "File",
        };
        let rel = normalize_path(path.strip_prefix(&self.serve_path)?);
        if rel.is_empty() {
            return Ok(());
        }
        let parent = normalize_path(Path::new(&rel).parent().unwrap_or_else(|| Path::new("")));
        let size = if is_dir {
            dir_child_count(path, &self.hidden)?
        } else {
            meta.len()
        };
        let mtime = meta
            .modified()
            .ok()
            .or_else(|| meta.created().ok())
            .map(to_timestamp)
            .unwrap_or_default();
        self.conn.execute(
            "INSERT OR REPLACE INTO files (path, parent, name, path_type, size, mtime, hidden, scan_generation, indexed_at)
             VALUES (?, ?, ?, ?, ?, ?, false, ?, current_timestamp)",
            params![rel, parent, name, path_type, size, mtime, generation],
        )?;
        Ok(())
    }

    fn remove_path(&mut self, path: &Path) -> Result<()> {
        let rel = match path.strip_prefix(&self.serve_path) {
            Ok(rel) => normalize_path(rel),
            Err(_) => return Ok(()),
        };
        self.conn.execute(
            "DELETE FROM files WHERE path = ? OR path LIKE ?",
            params![rel, format!("{rel}/%")],
        )?;
        Ok(())
    }

    fn search(&self, base: &Path, q: &str, limit: u64) -> Result<Vec<PathItem>> {
        let base_rel = normalize_path(base.strip_prefix(&self.serve_path)?);
        let path_like = if base_rel.is_empty() {
            "%".to_string()
        } else {
            format!("{base_rel}/%")
        };
        let q_like = format!("%{}%", q.to_lowercase());
        let mut stmt = self.conn.prepare(
            "SELECT path, path_type, size, mtime FROM files
             WHERE path LIKE ? AND (lower(name) LIKE ? OR lower(path) LIKE ?)
             ORDER BY CASE WHEN path_type IN ('Dir', 'SymlinkDir') THEN 0 ELSE 1 END, lower(path)
             LIMIT ?",
        )?;
        let rows = stmt.query_map(params![path_like, q_like, q_like, limit], |row| {
            let path: String = row.get(0)?;
            let path_type: String = row.get(1)?;
            let size: u64 = row.get(2)?;
            let mtime: u64 = row.get(3)?;
            let name = if base_rel.is_empty() {
                path
            } else {
                path.strip_prefix(&format!("{base_rel}/"))
                    .unwrap_or(&path)
                    .to_string()
            };
            Ok(PathItem {
                path_type: parse_path_type(&path_type),
                name,
                mtime,
                size,
            })
        })?;
        rows.collect::<duckdb::Result<Vec<_>>>().map_err(Into::into)
    }

    fn query(&self, sql: &str, limit: u64, path_filters: &[String]) -> Result<QueryResult> {
        ensure_select(sql)?;
        let filter = if path_filters.is_empty() {
            "true".to_string()
        } else {
            path_filters
                .iter()
                .map(|path| {
                    if path.is_empty() {
                        "true".to_string()
                    } else {
                        let escaped = path.replace('\'', "''");
                        format!("path = '{escaped}' OR path LIKE '{escaped}/%'")
                    }
                })
                .collect::<Vec<_>>()
                .join(" OR ")
        };
        let wrapped = format!("SELECT * FROM ({sql}) AS dufs_query WHERE {filter} LIMIT {limit}");
        let mut stmt = self.conn.prepare(&wrapped)?;
        let mut rows = stmt.query([])?;
        let stmt = rows
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invalid query"))?;
        let column_count = stmt.column_count();
        let columns = (0..column_count)
            .map(|i| stmt.column_name(i).map(|v| v.to_string()))
            .collect::<duckdb::Result<Vec<_>>>()?;
        let mut output = vec![];
        while let Some(row) = rows.next()? {
            let mut values = vec![];
            for i in 0..column_count {
                values.push(Value::String(format!("{:?}", row.get_ref(i)?)));
            }
            output.push(values);
        }
        Ok(QueryResult {
            columns,
            rows: output,
        })
    }
}

fn handle_notify_event(tx: &mpsc::Sender<IndexCommand>, event: notify::Event) {
    if event.kind.is_create() || event.kind.is_modify() {
        for path in event.paths {
            let _ = tx.send(IndexCommand::ScanPath(path));
        }
    } else if event.kind.is_remove() {
        for path in event.paths {
            let _ = tx.send(IndexCommand::RemovePath(path));
        }
    } else if matches!(event.kind, EventKind::Modify(_)) {
        for path in event.paths {
            let _ = tx.send(IndexCommand::ScanPath(path));
        }
    }
}

fn ensure_select(sql: &str) -> Result<()> {
    let sql = sql.trim();
    if sql.contains(';') {
        bail!("Only a single SELECT statement is allowed");
    }
    let lower = sql.to_lowercase();
    if !lower.starts_with("select") && !lower.starts_with("with") {
        bail!("Only SELECT statements are allowed");
    }
    for word in [
        "insert", "update", "delete", "copy", "attach", "install", "load", "pragma", "create",
        "drop", "alter",
    ] {
        if lower
            .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
            .any(|v| v == word)
        {
            bail!("Only SELECT statements are allowed");
        }
    }
    Ok(())
}

fn parse_path_type(value: &str) -> PathType {
    match value {
        "Dir" => PathType::Dir,
        "SymlinkDir" => PathType::SymlinkDir,
        "SymlinkFile" => PathType::SymlinkFile,
        _ => PathType::File,
    }
}

fn dir_child_count(path: &Path, hidden: &[String]) -> Result<u64> {
    let mut count = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let is_dir = entry.file_type().map(|v| v.is_dir()).unwrap_or_default();
        if crate::server::is_hidden_path(hidden, get_file_name(&entry_path), is_dir) {
            continue;
        }
        count += 1;
        if count >= crate::server::MAX_SUBPATHS_COUNT {
            break;
        }
    }
    Ok(count)
}

fn normalize_path<P: AsRef<Path>>(path: P) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
}

fn to_timestamp(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
