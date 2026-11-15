//! E10: randomized multi-client simulation against the real server and real
//! client cores. ROUNDS=1 for deterministic partition-based concurrency. SEEDS (default 3), STEPS (120), CLIENTS (3), FAULTS=0, BIG=1,
//! SEED_START (0). On failure prints seed + minimized trace.

use cairn_testkit::gen::GenCfg;
use cairn_testkit::real::RealEnv;
use cairn_testkit::sim::run_seed;

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d)
}

#[test]
fn e10_random_multi_client() {
    let cfg = GenCfg {
        clients: env_u64("CLIENTS", 3) as usize,
        steps: env_u64("STEPS", 120) as usize,
        faults: std::env::var("FAULTS").map(|v| v != "0").unwrap_or(true),
        big_files: std::env::var("BIG").is_ok(),
        rounds: std::env::var("ROUNDS").is_ok(),
    };
    let start = env_u64("SEED_START", 0);
    let mut fails = vec![];
    for seed in start..start + env_u64("SEEDS", 3) {
        if let Err(f) = run_seed(seed, &cfg, || RealEnv::new(cfg.clients)) {
            eprintln!("{f}");
            fails.push(f.to_string());
        }
    }
    assert!(fails.is_empty(), "{} seeds failed:\n{}", fails.len(), fails.join("\n"));
}
