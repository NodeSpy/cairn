use anyhow::{bail, Context, Result};
use cairn_server::admin::{self, Admin, HostSet};
use cairn_server::server::{Config, Server};
use std::path::PathBuf;
use std::time::Duration;

const USAGE: &str = "usage:
  cairn-server init <data>
  cairn-server host add <name> --out <dir> [--data <data>]
  cairn-server host revoke <name> [--data <data>]
  cairn-server host set <name> [--allowed-uids R|any] [--allowed-gids R|any] [--root-squash on|off]
                    [--uid-map from:to:len,...|none] [--gid-map ...|none] [--max-frame BYTES] [--max-commits N] [--data <data>]
  cairn-server host list [--data <data>]
  cairn-server root add <name> [--acl host:rw,host2:ro] [--data <data>]
  cairn-server root list [--data <data>]
  cairn-server epoch bump [--data <data>]   (after restoring from backup)
  cairn-server serve --data <data> [--listen 0.0.0.0:7420] [--lease-break-secs 30] [--relay-timeout-secs 60]
                    [--log-retention-days 7] [--log-min-changes 1000000] [--op-window-days 7]
(--data defaults to $CAIRN_DATA)";

struct Args {
    pos: Vec<String>,
    opts: Vec<(String, String)>,
}

impl Args {
    fn parse() -> Result<Args> {
        let mut pos = vec![];
        let mut opts = vec![];
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            if let Some(k) = a.strip_prefix("--") {
                let v = match k.split_once('=') {
                    Some((k, v)) => {
                        opts.push((k.to_string(), v.to_string()));
                        continue;
                    }
                    None => it.next().with_context(|| format!("--{k} needs a value"))?,
                };
                opts.push((k.to_string(), v));
            } else {
                pos.push(a);
            }
        }
        Ok(Args { pos, opts })
    }
    fn opt(&self, k: &str) -> Option<&str> {
        self.opts.iter().rev().find(|(a, _)| a == k).map(|(_, v)| v.as_str())
    }
    fn data(&self) -> Result<PathBuf> {
        self.opt("data").map(PathBuf::from).or_else(|| std::env::var_os("CAIRN_DATA").map(PathBuf::from)).context("no --data given (or $CAIRN_DATA)")
    }
    fn num<T: std::str::FromStr>(&self, k: &str) -> Result<Option<T>>
    where
        T::Err: std::fmt::Display,
    {
        self.opt(k).map(|v| v.parse::<T>().map_err(|e| anyhow::anyhow!("--{k}: {e}"))).transpose()
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let a = Args::parse()?;
    let p: Vec<&str> = a.pos.iter().map(|s| s.as_str()).collect();
    match p.as_slice() {
        ["init", dir] => {
            admin::init(&PathBuf::from(dir))?;
            println!("initialised {dir}");
        }
        ["host", "add", name] => {
            let out = a.opt("out").context("--out <dir> required")?;
            let id = Admin::open(&a.data()?)?.add_host(name, &PathBuf::from(out))?;
            println!("host {name} enrolled: HostId {} (credentials in {out})", id.0);
        }
        ["host", "revoke", name] => match Admin::open(&a.data()?)?.revoke_host(name)? {
            Some(id) => println!("host {name} (HostId {}) revoked", id.0),
            None => bail!("no such host {name}"),
        },
        ["host", "set", name] => {
            let opt_or_none = |k: &str| a.opt(k).map(|v| if v == "none" || v == "any" { None } else { Some(v.to_string()) });
            let s = HostSet {
                allowed_uids: opt_or_none("allowed-uids"),
                allowed_gids: opt_or_none("allowed-gids"),
                root_squash: a.opt("root-squash").map(|v| matches!(v, "on" | "true" | "yes" | "1")),
                uid_map: opt_or_none("uid-map"),
                gid_map: opt_or_none("gid-map"),
                max_frame: a.num::<u64>("max-frame")?.map(Some),
                max_commits: a.num::<u64>("max-commits")?.map(Some),
            };
            Admin::open(&a.data()?)?.set_host(name, &s)?;
            println!("host {name} updated");
        }
        ["host", "list"] => {
            for l in Admin::open(&a.data()?)?.list_hosts()? {
                println!("{l}");
            }
        }
        ["root", "add", name] => {
            let mut acl = vec![];
            for e in a.opt("acl").unwrap_or("").split(',').filter(|e| !e.is_empty()) {
                let (h, m) = e.rsplit_once(':').with_context(|| format!("bad ACL entry {e:?} (want host:rw|ro)"))?;
                acl.push((h.to_string(), match m { "rw" => true, "ro" => false, _ => bail!("bad ACL mode {m:?}") }));
            }
            Admin::open(&a.data()?)?.add_root(name, &acl)?;
            println!("root {name} ready");
        }
        ["epoch", "bump"] => {
            let e = Admin::open(&a.data()?)?.bump_epoch()?;
            println!("epoch is now {}", e.0);
        }
        ["root", "list"] => {
            for l in Admin::open(&a.data()?)?.list_roots()? {
                println!("{l}");
            }
        }
        ["serve"] => {
            let data = a.data()?;
            let listen = a.opt("listen").unwrap_or("0.0.0.0:7420").parse().context("--listen")?;
            let mut cfg = Config::new(data, listen);
            if let Some(n) = a.num::<u64>("lease-break-secs")? {
                cfg.lease_break = Duration::from_secs(n);
            }
            if let Some(n) = a.num::<u64>("relay-timeout-secs")? {
                cfg.relay_timeout = Duration::from_secs(n);
            }
            if let Some(n) = a.num::<u64>("log-retention-days")? {
                cfg.retention.log_secs = n * 86400;
            }
            if let Some(n) = a.num::<u64>("log-min-changes")? {
                cfg.retention.log_min_changes = n;
            }
            if let Some(n) = a.num::<u64>("op-window-days")? {
                cfg.retention.op_window_secs = n * 86400;
            }
            tracing_subscriber::fmt().with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into())).init();
            let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
            rt.block_on(async move {
                let srv = Server::start(cfg).await?;
                tracing::info!("cairn-server listening on {}", srv.addr());
                let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
                tracing::info!("shutting down");
                srv.shutdown().await;
                Ok::<(), anyhow::Error>(())
            })?;
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
    Ok(())
}
