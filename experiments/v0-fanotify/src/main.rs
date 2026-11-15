mod client;
mod proto;
mod server;
mod sys;

use anyhow::{bail, Result};
use std::os::fd::AsFd;
use std::path::PathBuf;
use std::time::Duration;

#[macro_export]
macro_rules! log {
    ($tag:expr, $($arg:tt)*) => {{
        let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
        eprintln!("{}.{:03} [{}] {}", t.as_secs() % 100_000, t.subsec_millis(), $tag, format!($($arg)*));
    }};
}

const USAGE: &str = "\
usage:
  leasefs server --store DIR [--listen ADDR] [--grant-delay-ms N] [--recall-timeout-ms N]
  leasefs client --dir DIR --server ADDR [--name NAME] [--cache-max BYTES] [--flush-delay-ms N]
  leasefs watch DIR        (recursive inotify printer, for testing)";

struct Args(Vec<String>);
impl Args {
    fn get(&self, k: &str) -> Option<String> {
        self.0.iter().position(|a| a == k).and_then(|i| self.0.get(i + 1)).cloned()
    }
    fn req(&self, k: &str) -> Result<String> {
        match self.get(k) {
            Some(v) => Ok(v),
            None => bail!("missing {k}\n{USAGE}"),
        }
    }
    fn num(&self, k: &str, d: u64) -> Result<u64> {
        Ok(match self.get(k) {
            Some(v) => parse_size(&v)?,
            None => d,
        })
    }
}

fn parse_size(s: &str) -> Result<u64> {
    let (n, mul) = match s.chars().last() {
        Some('K' | 'k') => (&s[..s.len() - 1], 1u64 << 10),
        Some('M' | 'm') => (&s[..s.len() - 1], 1 << 20),
        Some('G' | 'g') => (&s[..s.len() - 1], 1 << 30),
        _ => (s, 1),
    };
    Ok(n.parse::<u64>()? * mul)
}

fn main() -> Result<()> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = argv.first().cloned() else { bail!("{USAGE}") };
    let a = Args(argv[1..].to_vec());
    match cmd.as_str() {
        "server" => server::run(server::Opts {
            store: PathBuf::from(a.req("--store")?),
            listen: a.get("--listen").unwrap_or_else(|| "0.0.0.0:7070".into()),
            grant_delay: Duration::from_millis(a.num("--grant-delay-ms", 0)?),
            recall_timeout: Duration::from_millis(a.num("--recall-timeout-ms", 30_000)?),
        }),
        "client" => client::run(client::Opts {
            dir: PathBuf::from(a.req("--dir")?),
            server: a.req("--server")?,
            name: a.get("--name").unwrap_or_else(|| "client".into()),
            cache_max: a.num("--cache-max", u64::MAX)?,
            flush_delay: Duration::from_millis(a.num("--flush-delay-ms", 500)?),
        }),
        "watch" => watch(PathBuf::from(a.0.first().cloned().unwrap_or_else(|| ".".into()))),
        _ => bail!("{USAGE}"),
    }
}

/// Recursive inotify watcher printing one line per event: `MASK path`.
fn watch(root: PathBuf) -> Result<()> {
    use std::collections::HashMap;
    let ifd = sys::inotify_init()?;
    let mask = libc::IN_ALL_EVENTS | libc::IN_ONLYDIR | libc::IN_DONT_FOLLOW;
    let mut wds: HashMap<i32, PathBuf> = HashMap::new();
    fn add(ifd: std::os::fd::BorrowedFd, p: &std::path::Path, mask: u32, wds: &mut HashMap<i32, PathBuf>) {
        if let Ok(wd) = sys::inotify_add(ifd, p, mask) {
            wds.insert(wd, p.to_path_buf());
        }
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    add(ifd, &e.path(), mask, wds);
                }
            }
        }
    }
    add(ifd.as_fd(), &root, mask, &mut wds);
    println!("WATCHING {}", root.display());
    use std::io::Write;
    std::io::stdout().flush().ok();
    let names: &[(u32, &str)] = &[
        (libc::IN_ACCESS, "ACCESS"),
        (libc::IN_MODIFY, "MODIFY"),
        (libc::IN_ATTRIB, "ATTRIB"),
        (libc::IN_CLOSE_WRITE, "CLOSE_WRITE"),
        (libc::IN_CLOSE_NOWRITE, "CLOSE_NOWRITE"),
        (libc::IN_OPEN, "OPEN"),
        (libc::IN_MOVED_FROM, "MOVED_FROM"),
        (libc::IN_MOVED_TO, "MOVED_TO"),
        (libc::IN_CREATE, "CREATE"),
        (libc::IN_DELETE, "DELETE"),
        (libc::IN_DELETE_SELF, "DELETE_SELF"),
        (libc::IN_ISDIR, "ISDIR"),
    ];
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        for ev in sys::inotify_read(ifd.as_fd(), &mut buf)? {
            let Some(dir) = wds.get(&ev.wd).cloned() else { continue };
            let p = if ev.name.is_empty() { dir.clone() } else { dir.join(sys::os(&ev.name)) };
            let m: Vec<&str> = names.iter().filter(|(b, _)| ev.mask & b != 0).map(|(_, n)| *n).collect();
            println!("{} {}", m.join("|"), p.strip_prefix(&root).unwrap_or(&p).display());
            std::io::stdout().flush().ok();
            if ev.mask & libc::IN_CREATE != 0 && ev.mask & libc::IN_ISDIR != 0 {
                add(ifd.as_fd(), &p, mask, &mut wds);
            }
        }
    }
}
