# Build stage
# renovate: datasource=docker depName=golang
# The exact multi-architecture manifest digest for this tag is pinned here;
# Renovate should update the tag and digest together.
FROM golang:1.27.1-alpine3.24@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS builder

RUN apk add --no-cache git

WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum* ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build \
    -ldflags="-w -s -X main.Version=${VERSION} -X main.GitCommit=${COMMIT}" \
    -o scale-csi ./cmd/scale-csi

# Rust node agent (scale-csi-node). Built on the build platform and linked
# with rust-lld into a static musl binary for the target architecture, so the
# arm64 image is not compiled under emulation (the agent has no C dependencies).
# renovate: datasource=docker depName=rust
# The exact multi-architecture manifest digest for this tag is pinned here;
# Renovate should update the tag and digest together, and the tag must match
# rust/scale-csi-node/rust-toolchain.toml.
FROM --platform=$BUILDPLATFORM rust:1.98.1-alpine3.24@sha256:7cc1c22d77d9432f7fe012a70e6d3e555af54c2a6832700ed7d553f1769ae89f AS rust-builder

ARG TARGETARCH
# The image's own toolchain, without the components rust-toolchain.toml lists
# for development (clippy, rustfmt).
ENV RUSTUP_TOOLCHAIN=1.98.1
WORKDIR /build
COPY rust/scale-csi-node ./
RUN case "${TARGETARCH}" in \
      amd64) target=x86_64-unknown-linux-musl ;; \
      arm64) target=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
 && rustup target add "${target}" \
 && linker_var="CARGO_TARGET_$(echo "${target}" | tr 'a-z-' 'A-Z_')_LINKER" \
 && env "${linker_var}=rust-lld" cargo build --release --locked --target "${target}" \
 && cp "target/${target}/release/scale-csi-node" /scale-csi-node

######################
# Runtime image
######################
# renovate: datasource=docker depName=alpine
# The exact multi-architecture manifest digest for this tag is pinned here;
# Renovate should update the tag and digest together.
FROM alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

LABEL org.opencontainers.image.source="https://github.com/GizmoTickler/scale-csi"
LABEL org.opencontainers.image.url="https://github.com/GizmoTickler/scale-csi"
LABEL org.opencontainers.image.licenses="MIT"
LABEL org.opencontainers.image.title="Scale CSI Driver"
LABEL org.opencontainers.image.description="Kubernetes CSI driver for TrueNAS SCALE Systems"

# Install runtime dependencies
# - nfs-utils: NFS client for mounting NFS shares
# - e2fsprogs, xfsprogs, btrfs-progs: filesystem tools for formatting
# - util-linux: findmnt, blkid utilities
# - ca-certificates: for HTTPS connections to TrueNAS API
# Note: open-iscsi and nvme-cli are NOT installed in container
# because we use wrapper scripts to run commands on the host
#
# APK_REFRESH exists only to be part of this layer's cache key. Docker keys a
# RUN layer on its instruction text and parent, not on the state of the Alpine
# repository, so with cache-from=type=gha the `apk upgrade` below was served
# from cache on every build and never re-executed until the base digest
# changed. The v1.11.0 release image shipped util-linux 2.42.1-r0 while
# 2.42.3-r1 had been in v3.24/main for weeks: 207 HIGH findings, all in a
# package the Dockerfile "upgraded". CI passes the run id so the layer is
# rebuilt every time; a stale value keeps the old behavior deliberately.
ARG APK_REFRESH=unset
RUN echo "apk refresh key: ${APK_REFRESH}" && apk upgrade --no-cache && apk add --no-cache \
    ca-certificates \
    bash \
    nfs-utils \
    e2fsprogs \
    e2fsprogs-extra \
    xfsprogs \
    xfsprogs-extra \
    btrfs-progs \
    util-linux

# Copy binaries from the builders. The node DaemonSet runs scale-csi-node
# instead of scale-csi when the chart's node.implementation is rust.
COPY --from=builder /build/scale-csi /usr/local/bin/scale-csi
COPY --from=rust-builder /scale-csi-node /usr/local/bin/scale-csi-node

# Copy host command wrappers - these override system commands
# and execute on the host via chroot /host
# /usr/local/bin is first in PATH so these take precedence
COPY docker/iscsiadm /usr/local/bin/iscsiadm
COPY docker/nvme /usr/local/bin/nvme
COPY docker/mount /usr/local/bin/mount
COPY docker/umount /usr/local/bin/umount

# Create directories for CSI socket and config
RUN mkdir -p /csi /etc/scale-csi

WORKDIR /

ENTRYPOINT ["/usr/local/bin/scale-csi"]
