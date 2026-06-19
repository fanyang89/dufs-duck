mod fixtures;
mod utils;

use assert_fs::fixture::TempDir;
use fixtures::{server, tmpdir, Error, TestServer};
use rstest::rstest;
use std::thread::sleep;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::symlink as symlink_dir;
#[cfg(windows)]
use std::os::windows::fs::symlink_dir;

#[rstest]
fn default_not_allow_symlink(server: TestServer, tmpdir: TempDir) -> Result<(), Error> {
    // Create symlink directory "foo" to point outside the root
    let dir = "foo";
    symlink_dir(tmpdir.path(), server.path().join(dir)).expect("Couldn't create symlink");
    let resp = reqwest::blocking::get(format!("{}{}", server.url(), dir))?;
    assert_eq!(resp.status(), 404);
    let resp = reqwest::blocking::get(format!("{}{}/index.html", server.url(), dir))?;
    assert_eq!(resp.status(), 404);
    let resp = reqwest::blocking::get(server.url())?;
    let paths = utils::retrieve_index_paths(&resp.text()?);
    assert!(!paths.is_empty());
    assert!(!paths.contains(&format!("{dir}/")));
    Ok(())
}

#[rstest]
fn allow_symlink(
    #[with(&["--allow-symlink"])] server: TestServer,
    tmpdir: TempDir,
) -> Result<(), Error> {
    // Create symlink directory "foo" to point outside the root
    let dir = "foo";
    symlink_dir(tmpdir.path(), server.path().join(dir)).expect("Couldn't create symlink");
    let resp = reqwest::blocking::get(format!("{}{}", server.url(), dir))?;
    assert_eq!(resp.status(), 200);
    let resp = reqwest::blocking::get(format!("{}{}/index.html", server.url(), dir))?;
    assert_eq!(resp.status(), 200);
    let resp = reqwest::blocking::get(server.url())?;
    let paths = utils::retrieve_index_paths(&resp.text()?);
    assert!(!paths.is_empty());
    assert!(paths.contains(&format!("{dir}/")));
    Ok(())
}

#[rstest]
fn indexed_search_does_not_follow_symlink_by_default(
    #[with(&["-A", "--enable-index", "--index-scan-interval", "0"])] server: TestServer,
    tmpdir: TempDir,
) -> Result<(), Error> {
    let dir = "foo";
    std::fs::write(tmpdir.path().join("outside-only.txt"), b"outside")?;
    symlink_dir(tmpdir.path(), server.path().join(dir)).expect("Couldn't create symlink");

    let text = wait_text(|| {
        reqwest::blocking::get(format!("{}?q={}&simple", server.url(), "outside-only.txt"))
    })?;
    assert!(!text.contains("outside-only.txt"));
    Ok(())
}

fn wait_text<F>(mut fetch: F) -> Result<String, Error>
where
    F: FnMut() -> reqwest::Result<reqwest::blocking::Response>,
{
    let start = Instant::now();
    loop {
        let resp = fetch()?;
        assert_eq!(resp.status(), 200);
        let text = resp.text()?;
        if !text.contains("outside-only.txt") || start.elapsed() > Duration::from_secs(5) {
            return Ok(text);
        }
        sleep(Duration::from_millis(100));
    }
}
