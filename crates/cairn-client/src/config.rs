//! Client configuration (TOML) and per-root policies (DESIGN §10).
//!
//! ```toml
//! server    = "192.0.2.10:7443"
//! creds_dir = "/etc/cairn"          # ca.pem host.pem host.key
//! cache_dir = "/var/lib/cairn"
//! # host_id = 3                      # optional; learned from the server otherwise
//!
//! [root.projects]
//! path = "/home/daniel/Projects"    # mountpoint (used by cairn-fuse)
//! [[root.projects.policy]]          # first match wins
//! match = ["**/target/**"]
//! mode  = "local_only"
//! ```

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PolicyMode {
    #[default]
    Sync,
    SyncOnClose,
    Pinned,
    LocalOnly,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PolicyRule {
    #[serde(rename = "match")]
    pub patterns: Vec<String>,
    pub mode: PolicyMode,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RootConfig {
    /// Mountpoint on this host (consumed by the FUSE layer, not by the core).
    pub path: Option<PathBuf>,
    /// Name of the root on the server; defaults to the table key.
    pub server_name: Option<String>,
    #[serde(default)]
    pub policy: Vec<PolicyRule>,
}

impl RootConfig {
    /// Policy for a root-relative path (`a/b/c`, no leading slash). First match wins.
    pub fn policy_for(&self, rel: &str) -> PolicyMode {
        for r in &self.policy {
            if r.patterns.iter().any(|p| glob_match(p, rel)) {
                return r.mode;
            }
        }
        PolicyMode::Sync
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    /// `host:port` of the cairn server. Empty = offline (never connect).
    #[serde(default)]
    pub server: String,
    pub creds_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub host_id: Option<u64>,
    /// Never connect (tests / offline operation).
    #[serde(default)]
    pub offline: bool,
    /// Start evicting CAS objects when free space on the cache drops below this.
    #[serde(default = "d_high")]
    pub evict_below_free: u64,
    /// ...and evict until this much is free.
    #[serde(default = "d_low")]
    pub evict_to_free: u64,
    /// Clean idle live files kept for reuse.
    #[serde(default = "d_live")]
    pub idle_live_files: usize,
    /// State DB group-commit interval (ms) and op count.
    #[serde(default = "d_ms")]
    pub commit_interval_ms: u64,
    #[serde(default = "d_ops")]
    pub commit_max_ops: usize,
    /// Pipeline worker threads per root (hash / publish / commit).
    #[serde(default = "d_pipe")]
    pub pipeline_threads: usize,
    /// A published version's CAS commit and upload wait this long after its last save (ms), so
    /// short-lived files fold away. A relay request (NeedContent) bypasses it.
    #[serde(default = "d_hold")]
    pub upload_hold_ms: u64,
    /// Minimum spacing of publishes of a file that still has writers (fsync/checkpoint saves), ms.
    #[serde(default = "d_hot")]
    pub hot_save_min_ms: u64,
    /// Checkpoint a file open for write every this many seconds when its stat changed (0 = off) ...
    #[serde(default = "d_ckpt")]
    pub checkpoint_secs: u64,
    /// ... or after this many bytes of growth.
    #[serde(default = "d_ckpt_bytes")]
    pub checkpoint_bytes: u64,
    /// Files held open for write longer than this (seconds) publish their checkpoints.
    #[serde(default = "d_pub_secs")]
    pub checkpoint_publish_secs: u64,
    /// How often the checkpoint scan looks at open-for-write files (ms).
    #[serde(default = "d_scan")]
    pub checkpoint_scan_ms: u64,
    /// Op log group-commit fsync interval (ms).
    #[serde(default = "d_logms")]
    pub log_sync_ms: u64,
    #[serde(default, rename = "root")]
    pub roots: BTreeMap<String, RootConfig>,
}

fn d_high() -> u64 {
    5 << 30
}
fn d_low() -> u64 {
    10 << 30
}
fn d_live() -> usize {
    65536
}
fn d_ms() -> u64 {
    5
}
fn d_pipe() -> usize {
    4
}
fn d_hold() -> u64 {
    1000
}
fn d_hot() -> u64 {
    1000
}
fn d_ckpt() -> u64 {
    30
}
fn d_ckpt_bytes() -> u64 {
    64 << 20
}
fn d_pub_secs() -> u64 {
    300
}
fn d_scan() -> u64 {
    2000
}
fn d_logms() -> u64 {
    100
}
fn d_ops() -> usize {
    500
}

impl Config {
    pub fn new(server: &str, creds_dir: &Path, cache_dir: &Path) -> Config {
        Config {
            server: server.to_string(),
            creds_dir: creds_dir.to_path_buf(),
            cache_dir: cache_dir.to_path_buf(),
            host_id: None,
            offline: false,
            evict_below_free: d_high(),
            evict_to_free: d_low(),
            idle_live_files: d_live(),
            commit_interval_ms: d_ms(),
            commit_max_ops: d_ops(),
            log_sync_ms: d_logms(),
            pipeline_threads: d_pipe(),
            upload_hold_ms: d_hold(),
            hot_save_min_ms: d_hot(),
            checkpoint_secs: d_ckpt(),
            checkpoint_bytes: d_ckpt_bytes(),
            checkpoint_publish_secs: d_pub_secs(),
            checkpoint_scan_ms: d_scan(),
            roots: BTreeMap::new(),
        }
    }

    /// Offline config (no server) for tests: only needs a cache dir.
    pub fn offline(cache_dir: &Path) -> Config {
        let mut c = Config::new("", Path::new("/nonexistent"), cache_dir);
        c.offline = true;
        c
    }

    pub fn with_root(mut self, name: &str, rc: RootConfig) -> Config {
        self.roots.insert(name.to_string(), rc);
        self
    }

    pub fn from_toml(s: &str) -> Result<Config> {
        toml::from_str(s).context("parse client config")
    }

    pub fn load(path: &Path) -> Result<Config> {
        let s = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        Self::from_toml(&s)
    }
}

/// Glob over `/`-separated paths: `**` = any number of segments (incl. zero),
/// `*` = any run inside a segment, `?` = one byte.
pub fn glob_match(pat: &str, path: &str) -> bool {
    let p: Vec<&str> = pat.split('/').filter(|s| !s.is_empty()).collect();
    let s: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    seg_match(&p, &s)
}

fn seg_match(p: &[&str], s: &[&str]) -> bool {
    match p.first() {
        None => s.is_empty(),
        Some(&"**") => (0..=s.len()).any(|i| seg_match(&p[1..], &s[i..])),
        Some(seg) => !s.is_empty() && wild(seg.as_bytes(), s[0].as_bytes()) && seg_match(&p[1..], &s[1..]),
    }
}

fn wild(p: &[u8], s: &[u8]) -> bool {
    match p.first() {
        None => s.is_empty(),
        Some(b'*') => (0..=s.len()).any(|i| wild(&p[1..], &s[i..])),
        Some(b'?') => !s.is_empty() && wild(&p[1..], &s[1..]),
        Some(c) => s.first() == Some(c) && wild(&p[1..], &s[1..]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn globs() {
        assert!(glob_match("**/target/**", "a/b/target/debug/x"));
        assert!(glob_match("**/target/**", "target"));
        assert!(!glob_match("**/target/**", "a/targets/x"));
        assert!(glob_match("**/*.tmp", "x/y/z.tmp"));
        assert!(glob_match("**/*.tmp", "z.tmp"));
        assert!(!glob_match("**/*.tmp", "z.tmpx"));
        assert!(glob_match("**/.git/refs/**", "p/.git/refs/heads/main"));
        assert!(glob_match("**", "anything/at/all"));
    }

    #[test]
    fn toml_config() {
        let c = Config::from_toml(
            r#"
server = "1.2.3.4:7443"
creds_dir = "/etc/cairn"
cache_dir = "/var/lib/cairn"
[root.projects]
path = "/p"
[[root.projects.policy]]
match = ["**/target/**", "**/*.tmp"]
mode = "local_only"
[[root.projects.policy]]
match = ["**/.git/HEAD"]
mode = "sync_on_close"
[root."hosts/devbox/docker"]
"#,
        )
        .unwrap();
        let r = &c.roots["projects"];
        assert_eq!(r.policy_for("a/target/x"), PolicyMode::LocalOnly);
        assert_eq!(r.policy_for("a/.git/HEAD"), PolicyMode::SyncOnClose);
        assert_eq!(r.policy_for("a/b"), PolicyMode::Sync);
        assert!(c.roots.contains_key("hosts/devbox/docker"));
    }
}
