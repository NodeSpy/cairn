# cairn-server image. Build context = a tarball of the workspace (see build-image.sh).
# The binary is built inside a Debian container, so it links against Debian's
# glibc (2.36) and does not depend on the devbox's glibc.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY vendor ./vendor
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p cairn-server \
    && install -D target/release/cairn-server /out/cairn-server

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin cairn \
    && mkdir -p /data && chown cairn /data
COPY --from=build /out/cairn-server /usr/local/bin/cairn-server
VOLUME /data
EXPOSE 7420
ENV CAIRN_DATA=/data RUST_LOG=info
# Run as root instead (--user 0) if the NAS share's ownership requires it.
USER cairn
ENTRYPOINT ["cairn-server"]
CMD ["serve", "--listen", "0.0.0.0:7420"]
