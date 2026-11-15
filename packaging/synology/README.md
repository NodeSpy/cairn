# cairn-server on Synology DSM 7 (SPK)

Tested on RS1221+ (platform `v1000`, x86_64), DSM 7.2.2, glibc 2.36.

## Build
    packaging/synology/build-spk.sh            # builds a Debian-bookworm (glibc 2.36) binary via Docker
    packaging/synology/build-spk.sh --bin PATH # or package an existing binary (must need glibc <= 2.36)
Output: `packaging/synology/out/cairn-server-<version>-<arch>.spk` (`SPK_ARCH` default `v1000`, `SPK_REL` bumps the
package release, e.g. `SPK_REL=0002` for an upgrade with the same crate version).

## Install (first time)
The `cairn` shared folder must exist. The package runs as the unprivileged user `cairn-server`; `conf/resource`
(data-share worker) grants it RW on the `cairn` share. NOTE: the worker rewrites the share ACL to
"administrators + cairn-server" only (any other pre-existing ACL entries on the share are dropped).

    cat cairn-server-0.1.0-v1000.spk | ssh nas 'cat > /volume1/cairn/pkg.spk'   # scp may not work to a share root
    ssh nas 'sudo /usr/syno/bin/synopkg install /volume1/cairn/pkg.spk'
    ssh nas 'sudo -u cairn-server /var/packages/cairn-server/target/bin/cairn-admin init /volume1/cairn/server'
    ssh nas 'sudo /usr/syno/bin/synopkg start cairn-server'
    ssh nas 'rm /volume1/cairn/pkg.spk'

Data dir: `/volume1/cairn/server` (`meta.db`, `cas/`, `pki/` incl. the CA key). Override with
`/var/packages/cairn-server/var/env` (`CAIRN_DATA=...`, `CAIRN_LISTEN=...`, `RUST_LOG=...`).
`start` refuses to run until `init` has been done.

## Upgrade
`synopkg install` the newer SPK over the old one (DSM stops, replaces the binary, restarts). Data is never touched.

## Admin / enroll a client
`cairn-admin` is `cairn-server` with `CAIRN_DATA` preset; always run it as the package user so files stay owned by it:

    A='sudo -u cairn-server /var/packages/cairn-server/target/bin/cairn-admin'
    ssh nas "$A host add myhost --out /volume1/cairn/creds-myhost"    # writes ca.pem host.pem host.key
    ssh nas "$A root add proj --acl myhost:rw"
    ssh nas "$A host list; $A root list"
    ssh nas "$A host revoke myhost"
Copy the creds dir to the client (default `/etc/cairn/`) and delete it from the NAS. Clients connect to `NAS_IP:7420`.

## Logs / control
Log: `/var/packages/cairn-server/var/cairn-server.log` (rotated to `.log.1` at 10 MB, checked at start).
`sudo /usr/syno/bin/synopkg {start,stop,status} cairn-server`, or the DSM Package Center.

## Uninstall
`sudo /usr/syno/bin/synopkg uninstall cairn-server` removes the package and its `var` (log). `/volume1/cairn` is never
deleted by any package script. Note DSM may also drop the package user's ACL entry on the share.

## Firewall
The package declares TCP 7420 (`port_conf/cairn-server.sc`, shown as "cairn-server" in Control Panel > Security > Firewall
rules). If the DSM firewall is enabled, allow 7420/TCP from the LAN. (At install time the firewall was disabled.)
