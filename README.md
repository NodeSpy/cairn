<p align="center"><img src="assets/cairn-256.png" width="128" alt="cairn"></p>

# cairn

A network filesystem for a home lab. It gives several Linux machines one shared namespace backed by a NAS, with
local-disk-speed I/O, real inotify for changes made on other machines, and content-addressed deduplication
across the whole store.

> **Status: early and under active development. Do not trust it with data you can't afford to lose yet.**

## How it works

- **Clients** run `cairnd`, a FUSE filesystem that uses kernel passthrough. File data is read and written
  directly on a local btrfs cache, so data I/O runs at native speed. Only namespace operations go through the
  daemon.
- **The server** (`cairn-server`) runs on the NAS, natively as a Synology DSM package or in a container. It
  keeps the namespace and an ordered change log in SQLite, and stores content as BLAKE3-addressed chunks
  (FastCDC) on btrfs.
- **Remote changes** stream to every client and are replayed through the local mount, so editors, dev servers
  and `inotifywait` see them as ordinary local events.
- **Concurrency** uses git-style optimistic merging per directory entry: no directory locks, deterministic
  conflict resolution, and every conflict logged. File content has per-file write leases.
- **Offline**, clients keep working: changes queue in a local write-ahead log and are rebased on reconnect.
- **Security:** mutual TLS with a private CA and per-host identity, per-root and per-host ACLs, and uid/gid
  assertion rules.

## Docs

- [`docs/DESIGN.md`](docs/DESIGN.md): goals, architecture and decisions
- [`docs/CLIENT.md`](docs/CLIENT.md): client architecture (op log, write pipeline, passthrough, recovery)
- [`docs/PROTOCOL.md`](docs/PROTOCOL.md): wire protocol and merge semantics
- [`docs/experiments/`](docs/experiments/): the measurements behind the design (E1–E19)
- [`packaging/synology/`](packaging/synology/): building and installing the DSM package

## Layout

| Path | What |
|---|---|
| `crates/cairn-proto` | wire types, framing, TLS/PKI helpers |
| `crates/cairn-server` | server: merge engine, change log, CAS, leases, admin CLI |
| `crates/cairn-client` | client core: state DB, op log, write pipeline, sync, recovery |
| `crates/cairn-fuse` | `cairnd`: the FUSE layer and remote-change applier |
| `crates/cairn-testkit` | reference model, fault-injecting simulation, e2e and benchmark scripts |
| `vendor/fuser` | vendored `fuser` with an io_uring transport |
| `experiments/` | throwaway prototypes from the feasibility phase |

## Building

```sh
cargo build --release
cargo test
```

The client needs Linux ≥ 6.14 with FUSE passthrough, run as root, and a btrfs cache directory. The VM-based
e2e tests read host addresses from `tests/hosts.txt` (see `tests/hosts.txt.example`).

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT), at your option.
