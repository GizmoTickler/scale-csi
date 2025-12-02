# Contributing to Scale CSI

Thank you for your interest in contributing to the Scale CSI Driver! Your help is essential for making it better.

Please read our [Code of Conduct](CODE_OF_CONDUCT.md) before contributing.

## Development Environment

To contribute to this project, you will need:

- **Go**: Version 1.24 or later.
- **Docker**: For building container images.
- **TrueNAS Scale**: A running instance for integration testing (optional but recommended).

## Building the Driver

You can build the driver binary locally or as a Docker image.

### 1. Build Binary

To build the `scale-csi` binary locally:

```bash
CGO_ENABLED=0 go build -o scale-csi ./cmd/scale-csi
```

### 2. Build Docker Image

To build the Docker image:

```bash
docker build -t scale-csi .
```

## Testing

We use standard Go testing tools. Please ensure all tests pass before submitting a PR.

### 1. Run Unit Tests

Run all tests with race detection enabled:

```bash
go test -v -race ./...
```

### 2. Run Specific Test

To run a specific test case (e.g., `TestControllerCreateVolume`):

```bash
go test -v -race ./pkg/driver -run TestControllerCreateVolume
```

### 3. Linting

Ensure your code is clean and follows Go standards:

```bash
go vet ./...
golangci-lint run
```

### 4. Helm Chart Linting

If you modify the Helm chart, validate it:

```bash
helm lint charts/scale-csi
```

## Submitting Changes

1.  **Fork the Repository**: Create a fork of the repository on GitHub.
2.  **Clone your Fork**:
    ```bash
    git clone https://github.com/YOUR_USERNAME/scale-csi.git
    cd scale-csi
    ```
3.  **Create a Branch**:
    ```bash
    git checkout -b feature/my-new-feature
    ```
4.  **Make Changes**: Implement your feature or fix.
5.  **Test**: Run `go test ./...` to ensure everything works.
6.  **Commit**:
    ```bash
    git add .
    git commit -m "feat: add support for new protocol"
    ```
7.  **Push**:
    ```bash
    git push origin feature/my-new-feature
    ```
8.  **Open a Pull Request**: Go to the original repository and open a PR from your branch.

## Code Structure

- **`pkg/driver`**: Core CSI logic (Controller and Node services).
- **`pkg/truenas`**: TrueNAS WebSocket JSON-RPC 2.0 API client.
- **`pkg/util`**: System utilities (mount operations, iscsiadm, nvme-cli wrappers).
- **`charts/scale-csi`**: Helm chart for deployment.

## Testing Approach

Unit tests use mock clients defined in `pkg/truenas/mock_client.go`. The `MockClient` implements `ClientInterface`, allowing controller logic to be tested without a real TrueNAS instance.

Thank you for contributing!
