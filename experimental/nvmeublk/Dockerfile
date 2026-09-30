# nvmeublk: userspace NVMe/TCP multipath served through ublk (per-node daemon).
# Keep the measured glibc allocator/runtime in the shipped data path.
# Static musl substantially reduces small-I/O throughput on the bench VM.
# Build and runtime use the same Debian generation for ABI compatibility.
FROM docker.io/library/rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
# Vendored libublk (Cargo.toml [patch.crates-io]): chunked tag partitions.
COPY third_party ./third_party
COPY src ./src
RUN cargo build --release --locked \
 && strip target/release/nvmeublk

FROM docker.io/library/debian:bookworm-slim
COPY --from=build /src/target/release/nvmeublk /nvmeublk
# Runs as root (ublk-control and io_uring need it) in a privileged pod with
# /dev and /run/nvmeublk from the host; see README "Per-node daemon".
ENTRYPOINT ["/nvmeublk"]
CMD ["daemon"]
