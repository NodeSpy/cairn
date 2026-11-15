#!/bin/sh
# Build cairn-server-<version>-<arch>.spk for DSM 7 (RS1221+ = platform v1000, x86_64).
# Usage: build-spk.sh [--bin /path/to/cairn-server]   (default: build a glibc-2.36 binary in Debian bookworm via Docker)
# Env: SPK_ARCH (default v1000), SPK_REL (package release counter, default 0001), OUT (default ./out)
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
arch=${SPK_ARCH:-v1000}
rel=${SPK_REL:-0001}
out=${OUT:-$here/out}
ver=$(sed -n 's/^version *= *"\(.*\)"/\1/p' "$root/Cargo.toml" | head -1)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
bin=
if [ "${1:-}" = "--bin" ]; then bin=$2; fi
if [ -z "$bin" ]; then
  mkdir -p "$work/b"
  sh "$root/crates/cairn-server/build-image.sh" cairn-server:spk --extract "$work/b"
  bin=$work/b/cairn-server
fi
# DSM 7.2 ships glibc 2.36: refuse a binary needing newer symbols.
need=$(objdump -T "$bin" | grep -o 'GLIBC_[0-9.]*' | sed 's/GLIBC_//' | sort -V | tail -1)
echo "binary needs glibc <= $need"
[ "$(printf '%s\n2.36\n' "$need" | sort -V | tail -1)" = "2.36" ] || { echo "glibc too new" >&2; exit 1; }

pkg=$work/pkg; mkdir -p "$pkg/bin" "$work/spk"
install -m 755 "$bin" "$pkg/bin/cairn-server"
cat > "$pkg/bin/cairn-admin" <<'W'
#!/bin/sh
# Admin wrapper. Run as the package user: sudo -u cairn-server /var/packages/cairn-server/target/bin/cairn-admin host list
D=/volume1/cairn/server
[ -f /var/packages/cairn-server/var/env ] && . /var/packages/cairn-server/var/env
export CAIRN_DATA=$D
exec "$(dirname "$0")/cairn-server" "$@"
W
chmod 755 "$pkg/bin/cairn-admin"
install -D -m 644 "$here/spk/port_conf/cairn-server.sc" "$pkg/port_conf/cairn-server.sc"
tar -C "$pkg" -czf "$work/spk/package.tgz" .
cp -r "$here/spk/scripts" "$here/spk/conf" "$work/spk/"
# Icons (rendered from assets/cairn-logo.svg)
assets="$here/../../assets"
cp "$assets/cairn-64.png" "$work/spk/PACKAGE_ICON.PNG"
cp "$assets/cairn-256.png" "$work/spk/PACKAGE_ICON_256.PNG"
cat > "$work/spk/INFO" <<I
package="cairn-server"
version="$ver-$rel"
arch="$arch"
os_min_ver="7.0-40000"
maintainer="cairn"
displayname="cairn server"
description="cairn network filesystem server (mTLS, port 7420). Data lives in the 'cairn' shared folder (/volume1/cairn/server) and is never removed on uninstall."
startable="yes"
thirdparty="yes"
I
mkdir -p "$out"
spk="$out/cairn-server-$ver-$arch.spk"
tar -C "$work/spk" -cf "$spk" INFO package.tgz scripts conf PACKAGE_ICON.PNG PACKAGE_ICON_256.PNG
echo "$spk"
