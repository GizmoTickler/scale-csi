# Build stage
FROM golang:1.25.5-alpine3.22 AS builder

RUN apk add --no-cache git

WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum* ./
RUN go mod download || true

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
FROM alpine:3.22

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
RUN apk add --no-cache \
    ca-certificates \
    bash \
    nfs-utils \
    e2fsprogs \
    xfsprogs \
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
