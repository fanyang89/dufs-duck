use crate::server::{PathItem, PathType};
use crate::utils::get_file_name;

use anyhow::Result;
use duckdb::{params, Connection};
use ignore::WalkBuilder;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Serialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::oneshot;

const INDEX_SCAN_BATCH_SIZE: usize = 128;
const INDEX_SCAN_TARGET_LATENCY_MS: u64 = 100;
const INDEX_SCAN_MAX_DELAY_MS: u64 = 100;
const INDEX_WATCH_DEBOUNCE: Duration = Duration::from_millis(500);
const INDEX_SCHEMA_VERSION: u64 = 1;

#[derive(Clone, Copy)]
enum WatchAction {
    Scan,
    Remove,
}

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
    queue: IndexQueue,
    snapshot_path: PathBuf,
    status: Arc<Mutex<IndexStatus>>,
}

#[derive(Clone)]
struct IndexQueue {
    tx: mpsc::Sender<IndexCommand>,
    queued: Arc<AtomicUsize>,
    full_scan_queued: Arc<AtomicBool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct IndexStatus {
    pub schema_version: u64,
    pub ready: bool,
    pub scanning: bool,
    pub indexed_count: u64,
    pub snapshot_dirty: bool,
    pub watch_enabled: bool,
    pub scan_interval: u64,
    pub snapshot_interval: u64,
    pub queued_commands: usize,
    pub last_scan_at: Option<u64>,
    pub last_snapshot_at: Option<u64>,
    pub last_scan_duration_ms: Option<u64>,
    pub last_snapshot_duration_ms: Option<u64>,
    pub last_error: Option<String>,
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
        access_paths: Vec<String>,
        reply: oneshot::Sender<Result<Vec<PathItem>>>,
    },
}

impl IndexQueue {
    fn new(tx: mpsc::Sender<IndexCommand>) -> Self {
        Self {
            tx,
            queued: Arc::new(AtomicUsize::new(0)),
            full_scan_queued: Arc::new(AtomicBool::new(false)),
        }
    }

    fn queued(&self) -> usize {
        self.queued.load(Ordering::SeqCst)
    }

    fn send(&self, cmd: IndexCommand) {
        if matches!(cmd, IndexCommand::FullScan)
            && self.full_scan_queued.swap(true, Ordering::SeqCst)
        {
            return;
        }
        self.queued.fetch_add(1, Ordering::SeqCst);
        if self.tx.send(cmd).is_err() {
            self.queued.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn complete(&self, cmd: &IndexCommand) {
        self.queued.fetch_sub(1, Ordering::SeqCst);
        if matches!(cmd, IndexCommand::FullScan) {
            self.full_scan_queued.store(false, Ordering::SeqCst);
        }
    }
}

impl Indexer {
    pub fn new(
        serve_path: PathBuf,
        db_path: PathBuf,
        hidden: Vec<String>,
        follow_symlinks: bool,
        watch: bool,
        scan_interval: u64,
        snapshot_interval: u64,
        running: Arc<AtomicBool>,
        load: Arc<ServerLoad>,
    ) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let snapshot_path = Self::snapshot_path(&db_path);
        let worker_snapshot_path = snapshot_path.clone();
        let status = Arc::new(Mutex::new(IndexStatus::default()));
        update_status(&status, |status| {
            status.watch_enabled = watch;
            status.scan_interval = scan_interval;
            status.snapshot_interval = snapshot_interval;
        });
        let worker_status = status.clone();
        let worker_error_status = status.clone();
        let (tx, rx) = mpsc::channel();
        let queue = IndexQueue::new(tx);
        let watch_path = serve_path.clone();
        let worker_queue = queue.clone();
        let worker_running = running.clone();
        thread::spawn(move || {
            if let Err(err) = run_worker(
                rx,
                serve_path.clone(),
                db_path,
                worker_snapshot_path,
                hidden,
                follow_symlinks,
                load,
                snapshot_interval,
                worker_queue,
                worker_status,
                worker_running,
            ) {
                update_status(&worker_error_status, |status| {
                    status.ready = false;
                    status.scanning = false;
                    status.last_error = Some(err.to_string());
                });
                error!("indexer stopped: {err}");
            }
        });
        let indexer = Self {
            queue,
            snapshot_path,
            status,
        };
        indexer.full_scan();
        if watch {
            indexer.start_watcher(watch_path, indexer.queue.clone(), running.clone());
        }
        if scan_interval > 0 {
            start_periodic_scan(indexer.queue.clone(), scan_interval, running);
        }
        Ok(indexer)
    }

    pub fn default_db_path(serve_path: &Path) -> PathBuf {
        serve_path.join(".dufs").join("index.duckdb")
    }

    pub fn snapshot_path(db_path: &Path) -> PathBuf {
        db_path.with_file_name("index.readonly.duckdb")
    }

    pub fn readonly_snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    pub fn status(&self) -> IndexStatus {
        self.status
            .lock()
            .map(|status| {
                let mut status = status.clone();
                status.queued_commands = self.queue.queued();
                status
            })
            .unwrap_or_default()
    }

    pub fn full_scan(&self) {
        self.queue.send(IndexCommand::FullScan);
    }

    pub fn scan_path<P: AsRef<Path>>(&self, path: P) {
        self.queue
            .send(IndexCommand::ScanPath(path.as_ref().to_path_buf()));
    }

    pub fn upsert_path<P: AsRef<Path>>(&self, path: P) {
        self.queue
            .send(IndexCommand::UpsertPath(path.as_ref().to_path_buf()));
    }

    pub fn remove_path<P: AsRef<Path>>(&self, path: P) {
        self.queue
            .send(IndexCommand::RemovePath(path.as_ref().to_path_buf()));
    }

    pub fn move_path<P: AsRef<Path>>(&self, from: P, to: P) {
        self.queue.send(IndexCommand::MovePath {
            from: from.as_ref().to_path_buf(),
            to: to.as_ref().to_path_buf(),
        });
    }

    pub async fn search<P: AsRef<Path>>(
        &self,
        base: P,
        q: String,
        limit: u64,
        access_paths: Vec<String>,
    ) -> Result<Vec<PathItem>> {
        let (reply, rx) = oneshot::channel();
        self.queue.send(IndexCommand::Search {
            base: base.as_ref().to_path_buf(),
            q,
            limit,
            access_paths,
            reply,
        });
        rx.await?
    }

    fn start_watcher(&self, watch_path: PathBuf, queue: IndexQueue, running: Arc<AtomicBool>) {
        thread::spawn(move || {
            let (event_tx, event_rx) = mpsc::channel();
            let mut watcher = match RecommendedWatcher::new(
                move |res: notify::Result<notify::Event>| match res {
                    Ok(event) => {
                        let _ = event_tx.send(event);
                    }
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
            let mut pending = HashMap::new();
            while running.load(Ordering::SeqCst) {
                match event_rx.recv_timeout(INDEX_WATCH_DEBOUNCE) {
                    Ok(event) => collect_notify_event(&mut pending, event),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        flush_watch_events(&queue, &mut pending)
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            flush_watch_events(&queue, &mut pending);
        });
    }
}

fn start_periodic_scan(queue: IndexQueue, scan_interval: u64, running: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(scan_interval));
        while running.load(Ordering::SeqCst) {
            interval.tick().await;
            queue.send(IndexCommand::FullScan);
        }
    });
}

fn run_worker(
    rx: mpsc::Receiver<IndexCommand>,
    serve_path: PathBuf,
    db_path: PathBuf,
    snapshot_path: PathBuf,
    hidden: Vec<String>,
    follow_symlinks: bool,
    load: Arc<ServerLoad>,
    snapshot_interval: u64,
    queue: IndexQueue,
    status: Arc<Mutex<IndexStatus>>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut db = IndexDb::new(
        serve_path,
        db_path,
        snapshot_path,
        hidden,
        follow_symlinks,
        load,
    )?;
    update_status(&status, |status| {
        status.schema_version = INDEX_SCHEMA_VERSION;
    });
    let mut snapshot_dirty = false;
    let mut snapshot_dirty_at = Instant::now();
    while running.load(Ordering::SeqCst) {
        let Ok(cmd) = rx.recv_timeout(Duration::from_millis(200)) else {
            if snapshot_interval > 0
                && snapshot_dirty
                && snapshot_dirty_at.elapsed() >= Duration::from_secs(snapshot_interval)
            {
                let start = Instant::now();
                if let Err(err) = db.refresh_snapshot() {
                    update_status(&status, |status| {
                        status.last_error = Some(err.to_string());
                    });
                    warn!("failed to refresh index snapshot: {err}");
                } else {
                    let indexed_count = db.indexed_count().unwrap_or_default();
                    update_status(&status, |status| {
                        status.ready = true;
                        status.indexed_count = indexed_count;
                        status.snapshot_dirty = false;
                        status.last_snapshot_at = Some(now_millis());
                        status.last_snapshot_duration_ms = Some(duration_millis(start.elapsed()));
                        status.last_error = None;
                    });
                    snapshot_dirty = false;
                }
            }
            continue;
        };
        queue.complete(&cmd);
        update_status(&status, |status| {
            status.queued_commands = queue.queued();
        });
        match cmd {
            IndexCommand::FullScan => {
                let start = Instant::now();
                update_status(&status, |status| {
                    status.scanning = true;
                    status.last_error = None;
                });
                let scan = db.full_scan_with_yield(|db| {
                    drain_scan_commands(
                        db,
                        &rx,
                        &queue,
                        &mut snapshot_dirty,
                        &mut snapshot_dirty_at,
                    );
                    update_status(&status, |status| {
                        status.queued_commands = queue.queued();
                    });
                });
                if let Err(err) = scan {
                    update_status(&status, |status| {
                        status.scanning = false;
                        status.last_error = Some(err.to_string());
                    });
                    warn!("failed to scan index: {err}");
                } else {
                    let indexed_count = db.indexed_count().unwrap_or_default();
                    update_status(&status, |status| {
                        status.ready = true;
                        status.scanning = false;
                        status.indexed_count = indexed_count;
                        status.snapshot_dirty = false;
                        status.last_scan_at = Some(now_millis());
                        status.last_snapshot_at = Some(now_millis());
                        status.last_scan_duration_ms = Some(duration_millis(start.elapsed()));
                        status.last_snapshot_duration_ms = None;
                        status.last_error = None;
                    });
                    snapshot_dirty = false;
                }
            }
            IndexCommand::ScanPath(path) => {
                if let Err(err) = db.scan_path(&path) {
                    warn!("failed to scan {}: {err}", path.display());
                } else {
                    snapshot_dirty = true;
                    snapshot_dirty_at = Instant::now();
                    update_status(&status, |status| status.snapshot_dirty = true);
                }
            }
            IndexCommand::UpsertPath(path) => {
                if let Err(err) = db.upsert_path(&path) {
                    warn!("failed to index {}: {err}", path.display());
                } else {
                    snapshot_dirty = true;
                    snapshot_dirty_at = Instant::now();
                    update_status(&status, |status| status.snapshot_dirty = true);
                }
            }
            IndexCommand::RemovePath(path) => {
                if let Err(err) = db.remove_path(&path) {
                    warn!("failed to remove {} from index: {err}", path.display());
                } else {
                    snapshot_dirty = true;
                    snapshot_dirty_at = Instant::now();
                    update_status(&status, |status| status.snapshot_dirty = true);
                }
            }
            IndexCommand::MovePath { from, to } => {
                if let Err(err) = db.remove_path(&from).and_then(|_| db.scan_path(&to)) {
                    warn!(
                        "failed to move index entry {} -> {}: {err}",
                        from.display(),
                        to.display()
                    );
                } else {
                    snapshot_dirty = true;
                    snapshot_dirty_at = Instant::now();
                    update_status(&status, |status| status.snapshot_dirty = true);
                }
            }
            IndexCommand::Search {
                base,
                q,
                limit,
                access_paths,
                reply,
            } => {
                let _ = reply.send(db.search(&base, &q, limit, &access_paths));
            }
        }
    }
    Ok(())
}

fn update_status(status: &Arc<Mutex<IndexStatus>>, update: impl FnOnce(&mut IndexStatus)) {
    if let Ok(mut status) = status.lock() {
        update(&mut status);
    }
}

fn drain_scan_commands(
    db: &mut IndexDb,
    rx: &mpsc::Receiver<IndexCommand>,
    queue: &IndexQueue,
    snapshot_dirty: &mut bool,
    snapshot_dirty_at: &mut Instant,
) {
    while let Ok(cmd) = rx.try_recv() {
        queue.complete(&cmd);
        match cmd {
            IndexCommand::FullScan => {
                // Coalesce full scans requested while one is already in progress.
            }
            IndexCommand::ScanPath(path) => {
                if let Err(err) = db.scan_path(&path) {
                    warn!("failed to scan {}: {err}", path.display());
                } else {
                    *snapshot_dirty = true;
                    *snapshot_dirty_at = Instant::now();
                }
            }
            IndexCommand::UpsertPath(path) => {
                if let Err(err) = db.upsert_path(&path) {
                    warn!("failed to index {}: {err}", path.display());
                } else {
                    *snapshot_dirty = true;
                    *snapshot_dirty_at = Instant::now();
                }
            }
            IndexCommand::RemovePath(path) => {
                if let Err(err) = db.remove_path(&path) {
                    warn!("failed to remove {} from index: {err}", path.display());
                } else {
                    *snapshot_dirty = true;
                    *snapshot_dirty_at = Instant::now();
                }
            }
            IndexCommand::MovePath { from, to } => {
                if let Err(err) = db.remove_path(&from).and_then(|_| db.scan_path(&to)) {
                    warn!(
                        "failed to move index entry {} -> {}: {err}",
                        from.display(),
                        to.display()
                    );
                } else {
                    *snapshot_dirty = true;
                    *snapshot_dirty_at = Instant::now();
                }
            }
            IndexCommand::Search {
                base,
                q,
                limit,
                access_paths,
                reply,
            } => {
                let _ = reply.send(db.search(&base, &q, limit, &access_paths));
            }
        }
    }
}

struct IndexDb {
    conn: Connection,
    db_path: PathBuf,
    snapshot_path: PathBuf,
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

    fn is_batch_boundary(&self) -> bool {
        self.processed == 0
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
        snapshot_path: PathBuf,
        hidden: Vec<String>,
        follow_symlinks: bool,
        load: Arc<ServerLoad>,
    ) -> Result<Self> {
        let conn = Connection::open(&db_path)?;
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
            CREATE INDEX IF NOT EXISTS idx_files_path ON files(path);
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );",
        )?;
        let _ = conn.execute(
            "ALTER TABLE files ADD COLUMN scan_generation UBIGINT DEFAULT 0",
            [],
        );
        conn.execute(
            "INSERT INTO metadata (key, value)
             SELECT 'created_at', ?
             WHERE NOT EXISTS (SELECT 1 FROM metadata WHERE key = 'created_at')",
            params![now_millis().to_string()],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES ('schema_version', ?), ('updated_at', ?)",
            params![INDEX_SCHEMA_VERSION.to_string(), now_millis().to_string()],
        )?;
        let generation = conn.query_row(
            "SELECT coalesce(max(scan_generation), 0) FROM files",
            [],
            |row| row.get(0),
        )?;
        Ok(Self {
            conn,
            db_path,
            snapshot_path,
            serve_path,
            hidden,
            follow_symlinks,
            generation,
            throttle: IndexThrottle::new(load),
        })
    }

    fn full_scan_with_yield(&mut self, mut on_batch: impl FnMut(&mut Self)) -> Result<()> {
        self.generation = self.generation.saturating_add(1);
        let generation = self.generation;
        self.scan_path_with_generation(&self.serve_path.clone(), generation, &mut on_batch)?;
        self.conn.execute(
            "DELETE FROM files WHERE scan_generation <> ?",
            params![generation],
        )?;
        self.refresh_snapshot()?;
        Ok(())
    }

    fn scan_path(&mut self, path: &Path) -> Result<()> {
        self.scan_path_with_generation(path, self.generation, &mut |_| {})
    }

    fn scan_path_with_generation(
        &mut self,
        path: &Path,
        generation: u64,
        on_batch: &mut impl FnMut(&mut Self),
    ) -> Result<()> {
        let is_dir = std::fs::symlink_metadata(path)
            .map(|meta| meta.is_dir())
            .unwrap_or_else(|_| path.is_dir());
        if is_dir {
            for result in WalkBuilder::new(path)
                .hidden(false)
                .follow_links(self.follow_symlinks)
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
                if self.throttle.is_batch_boundary() {
                    on_batch(self);
                }
            }
        } else {
            self.upsert_path_with_generation(path, generation)?;
            self.throttle.step();
            on_batch(self);
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

    fn refresh_snapshot(&mut self) -> Result<()> {
        self.conn.execute_batch("CHECKPOINT")?;
        if let Some(parent) = self.snapshot_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = self.snapshot_path.with_extension("duckdb.tmp");
        std::fs::copy(&self.db_path, &tmp_path)?;
        std::fs::rename(tmp_path, &self.snapshot_path)?;
        Ok(())
    }

    fn search(
        &self,
        base: &Path,
        q: &str,
        limit: u64,
        access_paths: &[String],
    ) -> Result<Vec<PathItem>> {
        let base_rel = normalize_path(base.strip_prefix(&self.serve_path)?);
        let path_like = if base_rel.is_empty() {
            "%".to_string()
        } else {
            format!("{base_rel}/%")
        };
        let q_like = format!("%{}%", q.to_lowercase());
        let access_filter = build_access_filter(access_paths);
        let sql = format!(
            "SELECT path, path_type, size, mtime FROM files
             WHERE path LIKE ? AND (lower(name) LIKE ? OR lower(path) LIKE ?) AND ({access_filter})
             ORDER BY CASE WHEN path_type IN ('Dir', 'SymlinkDir') THEN 0 ELSE 1 END, lower(path)
             LIMIT ?"
        );
        let mut stmt = self.conn.prepare(&sql)?;
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

    fn indexed_count(&self) -> Result<u64> {
        self.conn
            .query_row("SELECT count(*) FROM files", [], |row| row.get(0))
            .map_err(Into::into)
    }
}

fn collect_notify_event(pending: &mut HashMap<PathBuf, WatchAction>, event: notify::Event) {
    if event.kind.is_create() || event.kind.is_modify() {
        for path in event.paths {
            pending.insert(path, WatchAction::Scan);
        }
    } else if event.kind.is_remove() {
        for path in event.paths {
            pending.insert(path, WatchAction::Remove);
        }
    } else if matches!(event.kind, EventKind::Modify(_)) {
        for path in event.paths {
            pending.insert(path, WatchAction::Scan);
        }
    }
}

fn flush_watch_events(queue: &IndexQueue, pending: &mut HashMap<PathBuf, WatchAction>) {
    for (path, action) in pending.drain() {
        let cmd = match action {
            WatchAction::Scan => IndexCommand::ScanPath(path),
            WatchAction::Remove => IndexCommand::RemovePath(path),
        };
        queue.send(cmd);
    }
}

fn build_access_filter(access_paths: &[String]) -> String {
    if access_paths.iter().any(|path| path.is_empty()) {
        return "true".to_string();
    }
    if access_paths.is_empty() {
        return "false".to_string();
    }
    access_paths
        .iter()
        .map(|path| {
            let escaped = path.replace('\'', "''");
            format!("path = '{escaped}' OR path LIKE '{escaped}/%'")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
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

fn now_millis() -> u64 {
    to_timestamp(SystemTime::now())
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}
