//! Folding of the outbound journal (docs/CLIENT.md "Sender"): ops the server never saw are
//! rewritten or dropped before they are sent.
//!
//! - create -> ... -> delete of a node whose `Create` is still unsent: every unsent op that names
//!   the node is dropped, nothing reaches the server and its bytes are never uploaded;
//! - create -> rename (onto a free name): the unsent `Create` is rewritten to the final name;
//! - a rename that replaces a node that only ever existed locally loses its `replaces`.
//!
//! Only unsent ops are touched (`Pend::sent == false`), under the root lock, so the sender can
//! never be shipping what is being folded.

use super::*;

#[derive(Default)]
struct Refs {
    as_node: bool,
    as_parent: bool,
    as_replaced: bool,
}

fn refs(op: &Op, n: NodeId) -> Refs {
    let mut r = Refs::default();
    match op {
        Op::Create { parent, node, .. } | Op::Link { parent, node, .. } | Op::Unlink { parent, node, .. } => {
            r.as_node = *node == n;
            r.as_parent = *parent == n;
        }
        Op::Rename { src_parent, node, dst_parent, replaces, .. } => {
            r.as_node = *node == n;
            r.as_parent = *src_parent == n || *dst_parent == n;
            r.as_replaced = *replaces == Some(n);
        }
        Op::SetAttr { node, .. } | Op::SetXattr { node, .. } | Op::SetContent { node, .. } => r.as_node = *node == n,
    }
    r
}

impl RootCore {
    /// Fold `op` (just applied to the visible tree, not yet queued) into the pending journal.
    /// Returns true when it was absorbed and must not be queued. `gone` is the node that lost its
    /// last name through this op, if any.
    pub(super) fn fold_op(&self, st: &mut RootState, op: &mut Op, gone: Option<NodeId>) -> bool {
        match op {
            Op::Unlink { node, .. } => {
                let n = *node;
                if gone != Some(n) {
                    return false;
                }
                match self.fold_away(st, n) {
                    None => false,
                    Some(None) => true,
                    Some(Some(inherited)) => {
                        // the dropped node had replaced `inherited` at this name: deleting the
                        // name now deletes that node
                        let base = st.synced.node(inherited).map(|x| x.version).unwrap_or(Seq(0));
                        if let Op::Unlink { node, base: b, .. } = op {
                            *node = inherited;
                            *b = base;
                        }
                        false
                    }
                }
            }
            Op::Rename { node, src_parent, src_name, dst_parent, dst_name, replaces, mode, .. } => {
                if *mode == RenameMode::Exchange {
                    return false;
                }
                if let (Some(r), Some(g)) = (*replaces, gone) {
                    if r == g {
                        // the replaced node only ever existed here: whatever it replaced in turn
                        // is what this rename replaces on the server (chained atomic saves)
                        if let Some(inherited) = self.fold_away(st, r) {
                            *replaces = inherited;
                        }
                    }
                }
                if replaces.is_some() {
                    return false;
                }
                self.fold_create_rename(st, *node, *src_parent, src_name, *dst_parent, dst_name)
            }
            _ => false,
        }
    }

    /// `node` was deleted. If the server never heard of it, forget every unsent op naming it.
    /// Returns `None` if nothing was folded, else the node that the dropped ops' rename had
    /// replaced on the server (if any).
    fn fold_away(&self, st: &mut RootState, node: NodeId) -> Option<Option<NodeId>> {
        let mut idxs = vec![];
        let mut has_create = false;
        let mut inherited: Option<NodeId> = None;
        for (i, p) in st.pending.iter().enumerate() {
            let r = refs(&p.op, node);
            if r.as_parent || r.as_replaced {
                return None;
            }
            if r.as_node {
                if p.sent {
                    return None;
                }
                match &p.op {
                    Op::Create { .. } => has_create = true,
                    Op::Rename { replaces: Some(r), mode, .. } => {
                        if *mode == RenameMode::Exchange || inherited.is_some() {
                            return None;
                        }
                        inherited = Some(*r);
                    }
                    _ => {}
                }
                idxs.push(i);
            }
        }
        if !has_create {
            return None;
        }
        let mut dropped = vec![];
        for i in idxs.into_iter().rev() {
            if let Some(p) = st.pending.remove(i) {
                dropped.push(p.n);
            }
        }
        for n in dropped {
            st.db.send(DbOp::Outbound { root: self.name.clone(), n, row: None });
            st.release_after.remove(&n);
        }
        st.rebuild_pidx();
        self.cancel_jobs_locked(st, node, None);
        self.cleanup_file(st, node);
        Some(inherited)
    }

    /// `Create` of `node` at (sp, sn) is unsent and the node is renamed to (dp, dn), which no
    /// pending op touches: create it at the final name instead.
    fn fold_create_rename(&self, st: &mut RootState, node: NodeId, sp: NodeId, sn: &Name, dp: NodeId, dn: &Name) -> bool {
        if st.pidx.touches_dirent(dp, &dn.0) {
            return false;
        }
        let mut ci = None;
        let mut dp_create = None;
        for (i, p) in st.pending.iter().enumerate() {
            match &p.op {
                Op::Create { node: m, parent, name, .. } if *m == node => {
                    if p.sent || *parent != sp || name != sn {
                        return false;
                    }
                    ci = Some(i);
                }
                Op::Create { node: m, .. } if *m == dp => dp_create = Some(i),
                _ => {}
            }
        }
        let Some(ci) = ci else { return false };
        if dp_create.is_some_and(|d| d > ci) {
            return false; // the new parent is created after the node: keep the order
        }
        // nothing else may have been queued against the old name in between
        if st.pending.iter().enumerate().any(|(i, p)| i > ci && matches!(&p.op, Op::Create { .. } | Op::Link { .. } | Op::Unlink { .. } | Op::Rename { .. }) && {
            let (keys, _) = (footprint(&p.op).keys, ());
            keys.iter().any(|(kp, kn)| *kp == sp && kn == &sn.0)
        }) {
            return false;
        }
        let p = &mut st.pending[ci];
        st.pidx.remove(&p.op);
        if let Op::Create { parent, name, .. } = &mut p.op {
            *parent = dp;
            *name = dn.clone();
        }
        st.pidx.add(&p.op);
        let (n, op, cred, ts) = (p.n, p.op.clone(), p.cred, p.ts);
        st.db.send(DbOp::Outbound { root: self.name.clone(), n, row: Some((op, cred, ts)) });
        true
    }
}
