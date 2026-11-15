//! cairn wire protocol and shared types.
//!
//! This crate is the contract between `cairn-server`, `cairn-client`
//! and `cairn-fuse`. Change it deliberately: every change here touches
//! every workstream. See `docs/PROTOCOL.md` for the prose version.

pub mod aio;
pub mod frame;
pub mod ids;
pub mod msg;
pub mod tls;

pub use ids::*;
pub use msg::*;

/// Bumped on any incompatible change to `msg`.
pub const PROTOCOL_VERSION: u32 = 2;

/// Files at or below this size are stored as a single blob; larger files
/// are split with FastCDC into a chunk manifest.
pub const SMALL_FILE_MAX: u64 = 64 * 1024;

/// FastCDC parameters (frozen: changing them changes every content id of
/// large files). `fastcdc::v2020`, normalization level 1.
pub const CDC_MIN: u32 = 16 * 1024;
pub const CDC_AVG: u32 = 64 * 1024;
pub const CDC_MAX: u32 = 256 * 1024;
