//! TLS: mutual TLS with a private cairn CA (DESIGN §5.1).
//!
//! Credentials dir layout (default `/etc/cairn/`):
//!   ca.pem    — cairn CA certificate (trust anchor)
//!   host.pem  — this host's certificate chain
//!   host.key  — this host's private key (0600)
//! The server additionally keeps `ca.key` (only on the server).
//!
//! Host identity = BLAKE3 fingerprint of the peer's end-entity certificate
//! DER. The server's host table maps fingerprint -> (HostId, name, ACLs);
//! enrollment adds a row, revocation deletes it. So a cert signed by the CA
//! but not (or no longer) in the table is rejected.

use crate::ids::Hash;
use anyhow::{anyhow, Context, Result};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use std::path::Path;
use std::sync::Arc;

/// SNI/verification name every server cert carries (clients connect by IP).
pub const SERVER_NAME: &str = "cairn-server";

fn certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let v: Vec<_> = rustls_pemfile::certs(&mut &pem[..]).collect::<Result<_, _>>()?;
    if v.is_empty() {
        return Err(anyhow!("no certificates in {}", path.display()));
    }
    Ok(v)
}

fn key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    rustls_pemfile::private_key(&mut &pem[..])?.ok_or_else(|| anyhow!("no private key in {}", path.display()))
}

fn roots(dir: &Path) -> Result<rustls::RootCertStore> {
    let mut store = rustls::RootCertStore::empty();
    for c in certs(&dir.join("ca.pem"))? {
        store.add(c)?;
    }
    Ok(store)
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Server side: present `host.pem`/`host.key`, require client certs signed by `ca.pem`.
pub fn server_config(dir: &Path) -> Result<Arc<rustls::ServerConfig>> {
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots(dir)?), provider()).build()?;
    let cfg = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs(&dir.join("host.pem"))?, key(&dir.join("host.key"))?)?;
    Ok(Arc::new(cfg))
}

/// Client side: trust only `ca.pem`, present this host's cert.
pub fn client_config(dir: &Path) -> Result<Arc<rustls::ClientConfig>> {
    let cfg = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots(dir)?)
        .with_client_auth_cert(certs(&dir.join("host.pem"))?, key(&dir.join("host.key"))?)?;
    Ok(Arc::new(cfg))
}

pub fn server_name() -> ServerName<'static> {
    ServerName::try_from(SERVER_NAME).expect("valid name")
}

/// Fingerprint of the peer's end-entity certificate (after the handshake).
pub fn peer_fingerprint(conn: &rustls::CommonState) -> Result<Hash> {
    let c = conn.peer_certificates().and_then(|c| c.first()).ok_or_else(|| anyhow!("peer sent no certificate"))?;
    Ok(Hash::of(c.as_ref()))
}

pub fn fingerprint_of_pem(pem: &[u8]) -> Result<Hash> {
    let c = rustls_pemfile::certs(&mut &pem[..]).next().ok_or_else(|| anyhow!("no certificate"))??;
    Ok(Hash::of(c.as_ref()))
}

/// Minimal private CA (used by `cairn-server ca init` / `enroll` and by tests).
pub mod pki {
    use super::*;
    use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};

    pub struct Pem {
        pub cert: String,
        pub key: String,
    }

    pub fn new_ca(name: &str) -> Result<Pem> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.distinguished_name.push(DnType::CommonName, name);
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = p.self_signed(&key)?;
        Ok(Pem { cert: cert.pem(), key: key.serialize_pem() })
    }

    /// Issue a host certificate. The server's own cert should use
    /// `SERVER_NAME` as `host_name`. Every cert gets both client and server auth.
    pub fn issue(ca: &Pem, host_name: &str) -> Result<Pem> {
        let ca_key = KeyPair::from_pem(&ca.key)?;
        let issuer = Issuer::from_ca_cert_pem(&ca.cert, ca_key)?;
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(vec![host_name.to_string()])?;
        p.distinguished_name.push(DnType::CommonName, host_name);
        p.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth, rcgen::ExtendedKeyUsagePurpose::ServerAuth];
        let cert = p.signed_by(&key, &issuer)?;
        Ok(Pem { cert: cert.pem(), key: key.serialize_pem() })
    }

    /// Write `ca.pem`, `host.pem`, `host.key` (0600) into `dir`.
    pub fn write_host_dir(dir: &Path, ca_cert: &str, host: &Pem) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::create_dir_all(dir)?;
        std::fs::write(dir.join("ca.pem"), ca_cert)?;
        std::fs::write(dir.join("host.pem"), &host.cert)?;
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(dir.join("host.key"))?;
        std::io::Write::write_all(&mut f, host.key.as_bytes())?;
        Ok(())
    }
}
