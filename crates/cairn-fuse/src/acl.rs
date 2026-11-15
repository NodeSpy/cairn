//! POSIX ACL xattr handling: the daemon (not a backing fs) is responsible for default-ACL
//! inheritance on create, the mode/ACL relationship on chmod and on `setxattr(system.posix_acl_access)`.
//! Mirrors `posix_acl_create`, `__posix_acl_chmod` and `posix_acl_update_mode`.

pub const ACCESS: &[u8] = b"system.posix_acl_access";
pub const DEFAULT: &[u8] = b"system.posix_acl_default";

const VERSION: u32 = 2;
pub const USER_OBJ: u16 = 0x01;
pub const USER: u16 = 0x02;
pub const GROUP_OBJ: u16 = 0x04;
pub const GROUP: u16 = 0x08;
pub const MASK: u16 = 0x10;
pub const OTHER: u16 = 0x20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Entry {
    pub tag: u16,
    pub perm: u16,
    pub id: u32,
}

pub fn parse(v: &[u8]) -> Option<Vec<Entry>> {
    if v.len() < 4 || (v.len() - 4) % 8 != 0 || u32::from_le_bytes(v[..4].try_into().ok()?) != VERSION {
        return None;
    }
    Some(
        v[4..]
            .chunks_exact(8)
            .map(|c| Entry {
                tag: u16::from_le_bytes([c[0], c[1]]),
                perm: u16::from_le_bytes([c[2], c[3]]),
                id: u32::from_le_bytes([c[4], c[5], c[6], c[7]]),
            })
            .collect(),
    )
}

pub fn serialize(es: &[Entry]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + es.len() * 8);
    v.extend_from_slice(&VERSION.to_le_bytes());
    for e in es {
        v.extend_from_slice(&e.tag.to_le_bytes());
        v.extend_from_slice(&e.perm.to_le_bytes());
        v.extend_from_slice(&e.id.to_le_bytes());
    }
    v
}

/// Result of applying a parent's default ACL to a new node.
pub struct Created {
    pub mode: u32,
    /// Extended access ACL to store (None: the mode bits say it all).
    pub access: Option<Vec<u8>>,
}

/// `posix_acl_create`: `mode` is the unmasked create mode (permission bits matter). Returns None
/// when the default ACL is malformed.
pub fn create(default: &[u8], mode: u32) -> Option<Created> {
    let mut acl = parse(default)?;
    let mut mode_p = mode;
    let mut m = mode & 0o777;
    let (mut group_obj, mut mask_obj) = (None, None);
    let mut not_equiv = false;
    for (i, pa) in acl.iter_mut().enumerate() {
        match pa.tag {
            USER_OBJ => {
                pa.perm &= ((m >> 6) | !0o7) as u16;
                m &= ((pa.perm as u32) << 6) | !0o700;
            }
            USER | GROUP => not_equiv = true,
            GROUP_OBJ => group_obj = Some(i),
            OTHER => {
                pa.perm &= (m | !0o7) as u16;
                m &= pa.perm as u32 | !0o7;
            }
            MASK => {
                mask_obj = Some(i);
                not_equiv = true;
            }
            _ => return None,
        }
    }
    if let Some(i) = mask_obj {
        acl[i].perm &= ((m >> 3) | !0o7) as u16;
        m &= ((acl[i].perm as u32) << 3) | !0o070;
    } else if let Some(i) = group_obj {
        acl[i].perm &= ((m >> 3) | !0o7) as u16;
        m &= ((acl[i].perm as u32) << 3) | !0o070;
    } else {
        return None;
    }
    mode_p = (mode_p & !0o777) | m;
    Some(Created { mode: mode_p, access: not_equiv.then(|| serialize(&acl)) })
}

/// `__posix_acl_chmod_masq`: rewrite an access ACL after chmod.
pub fn chmod(access: &[u8], mode: u32) -> Option<Vec<u8>> {
    let mut acl = parse(access)?;
    let have_mask = acl.iter().any(|e| e.tag == MASK);
    for e in acl.iter_mut() {
        match e.tag {
            USER_OBJ => e.perm = ((mode >> 6) & 7) as u16,
            GROUP_OBJ if !have_mask => e.perm = ((mode >> 3) & 7) as u16,
            MASK => e.perm = ((mode >> 3) & 7) as u16,
            OTHER => e.perm = (mode & 7) as u16,
            _ => {}
        }
    }
    Some(serialize(&acl))
}

/// Mode bits equivalent to the ACL (`posix_acl_equiv_mode`), plus whether the ACL is exactly
/// representable by the mode (no named entries, no mask).
pub fn equiv_mode(acl: &[Entry], mode: u32) -> (u32, bool) {
    let mut m = mode;
    let mut equiv = true;
    let mut mask = None;
    for e in acl {
        match e.tag {
            USER_OBJ => m = (m & !0o700) | ((e.perm as u32 & 7) << 6),
            GROUP_OBJ => m = (m & !0o070) | ((e.perm as u32 & 7) << 3),
            OTHER => m = (m & !0o007) | (e.perm as u32 & 7),
            MASK => mask = Some(e.perm as u32 & 7),
            _ => equiv = false,
        }
    }
    if let Some(mk) = mask {
        // Extended ACL: group bits show the mask.
        m = (m & !0o070) | (mk << 3);
        equiv = false;
    }
    (m, equiv)
}

/// Validity check applied to ACLs written by the user (`posix_acl_valid`, simplified): required
/// entries present exactly once, mask present when named entries are.
pub fn valid(acl: &[Entry]) -> bool {
    let (mut uo, mut go, mut ot, mut mk, mut named) = (0, 0, 0, 0, false);
    for e in acl {
        if e.perm & !7 != 0 {
            return false;
        }
        match e.tag {
            USER_OBJ => uo += 1,
            GROUP_OBJ => go += 1,
            OTHER => ot += 1,
            MASK => mk += 1,
            USER | GROUP => named = true,
            _ => return false,
        }
    }
    uo == 1 && go == 1 && ot == 1 && mk <= 1 && (!named || mk == 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(tag: u16, perm: u16, id: u32) -> Entry {
        Entry { tag, perm, id }
    }

    #[test]
    fn roundtrip() {
        let a = vec![e(USER_OBJ, 7, 0), e(GROUP_OBJ, 5, 0), e(OTHER, 4, 0)];
        assert_eq!(parse(&serialize(&a)).unwrap(), a);
    }

    #[test]
    fn create_extended() {
        let d = serialize(&[e(USER_OBJ, 7, 0), e(USER, 6, 1000), e(GROUP_OBJ, 5, 0), e(MASK, 7, 0), e(OTHER, 5, 0)]);
        let c = create(&d, 0o666).unwrap();
        // user 6, mask 6 (group 5 & 6 -> mask limited by mode group 6), other 4
        assert_eq!(c.mode & 0o777, 0o664);
        assert!(c.access.is_some());
    }

    #[test]
    fn create_minimal_acl_is_mode_only() {
        let d = serialize(&[e(USER_OBJ, 7, 0), e(GROUP_OBJ, 5, 0), e(OTHER, 0, 0)]);
        let c = create(&d, 0o777).unwrap();
        assert_eq!(c.mode & 0o777, 0o750);
        assert!(c.access.is_none());
    }

    #[test]
    fn chmod_updates_mask() {
        let a = serialize(&[e(USER_OBJ, 7, 0), e(USER, 6, 1000), e(GROUP_OBJ, 5, 0), e(MASK, 7, 0), e(OTHER, 5, 0)]);
        let n = parse(&chmod(&a, 0o640).unwrap()).unwrap();
        assert_eq!(n.iter().find(|x| x.tag == MASK).unwrap().perm, 4);
        assert_eq!(n.iter().find(|x| x.tag == OTHER).unwrap().perm, 0);
    }
}
