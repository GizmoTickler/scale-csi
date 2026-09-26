# nvmeublk: userspace NVMe/TCP multipath served through ublk (per-node daemon).
# Built on glibc (build scripts, including libublk's bindgen, must dlopen
# libclang) and cross-compiled to static musl; the runtime image holds only
# the binary.
FROM docker.io/library/rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools clang libclang-dev && rm -rf /var/lib/apt/lists/* \
 && rustup target add x86_64-unknown-linux-musl
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Vendored libublk (Cargo.toml [patch.crates-io]): chunked tag partitions.
COPY third_party ./third_party
COPY src ./src
RUN cargo build --release --locked --target x86_64-unknown-linux-musl \
 && strip target/x86_64-unknown-linux-musl/release/nvmeublk

FROM scratch
COPY --from=build /src/target/x86_64-unknown-linux-musl/release/nvmeublk /nvmeublk
# Runs as root (ublk-control and io_uring need it) in a privileged pod with
# /dev and /run/nvmeublk from the host; see README "Per-node daemon".
ENTRYPOINT ["/nvmeublk"]
CMD ["daemon"]
