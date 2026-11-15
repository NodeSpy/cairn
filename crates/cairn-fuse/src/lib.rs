//! cairn FUSE layer: translates kernel requests into [`cairn_client::api::Core`] calls,
//! keeps the kernel-side inode table, hands live-file fds to the kernel for passthrough, and
//! replays remote changes through the mount (`KernelApplier`) so watchers get real inotify.

pub mod acl;
pub mod applier;
pub mod fs;
pub mod ktable;
pub mod mock;
pub mod mount;
pub mod stats;
pub mod util;

pub use mount::{mount, Mount, MountOpts};
