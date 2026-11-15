//! cairn client core: state DB, local CAS cache, outbound journal and
//! offline queue, server connection, leases, conflict handling, recovery.
//! See `api.rs` for the contract with the FUSE layer.

pub mod api;
pub mod config;
pub mod tree;
pub mod oplog;
pub mod store;
pub mod cas;
pub mod client;
pub mod conn;
pub mod core;
pub mod testing;

pub use client::Client;
pub use config::{Config, PolicyMode, PolicyRule, RootConfig};
