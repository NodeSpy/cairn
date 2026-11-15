//! Helpers for tests and the testkit.

use crate::api::{Core, FsResult, KernelApplier};
use cairn_proto::Change;
use std::sync::{Arc, OnceLock};

/// A `KernelApplier` for tests that run Cores without FUSE: it just calls
/// `Core::apply_direct`. Construct it, give it the core, then
/// `core.set_applier(applier)`.
pub struct DirectApplier {
    core: OnceLock<Arc<dyn Core>>,
}

impl DirectApplier {
    pub fn new() -> Arc<DirectApplier> {
        Arc::new(DirectApplier { core: OnceLock::new() })
    }
    /// Convenience: create an applier for `core` and register it.
    pub fn install(core: &Arc<dyn Core>) -> Arc<DirectApplier> {
        let a = DirectApplier::new();
        let _ = a.core.set(core.clone());
        core.set_applier(a.clone());
        a
    }
}

impl KernelApplier for DirectApplier {
    fn apply(&self, change: &Change) -> FsResult<()> {
        match self.core.get() {
            Some(c) => c.apply_direct(change),
            None => Ok(()),
        }
    }
}
