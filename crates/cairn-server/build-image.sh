#!/bin/sh
# Build the cairn-server image from the workspace (no .dockerignore needed:
# the context is a filtered tarball). Usage: build-image.sh [tag]   (default cairn-server:dev)
# Also exports the binary: build-image.sh [tag] --extract <dir>
set -eu
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
tag=${1:-cairn-server:dev}
ctx=$(mktemp -d)
trap 'rm -rf "$ctx"' EXIT
tar -C "$root" --exclude='target' --exclude='experiments' --exclude='.git' -cf - Cargo.toml Cargo.lock crates vendor | tar -C "$ctx" -xf -
docker build -f "$here/Dockerfile" -t "$tag" "$ctx"
if [ "${2:-}" = "--extract" ]; then
  id=$(docker create "$tag")
  docker cp "$id:/usr/local/bin/cairn-server" "${3:?dir}/cairn-server"
  docker rm "$id" >/dev/null
fi
