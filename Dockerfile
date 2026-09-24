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

######################
# Runtime image
######################
# renovate: datasource=docker depName=alpine
# The exact multi-architecture manifest digest for this tag is pinned here;
# Renovate should update the tag and digest together.
FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6

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

# Copy binary from builder
COPY --from=builder /build/scale-csi /usr/local/bin/scale-csi

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
