//! The node's Prometheus series, with the Go node's names, labels and buckets
//! (`pkg/driver/metrics.go`): the chart's dashboard and alerts read them.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use prometheus::{Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

const NAMESPACE: &str = "scale_csi";

pub struct Metrics {
    registry: Registry,
    operations_total: IntCounterVec,
    operations_duration: HistogramVec,
    node_connect_total: IntCounterVec,
    nvme_path_connect_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let operations_total = IntCounterVec::new(
            Opts::new("operations_total", "Total number of CSI operations").namespace(NAMESPACE),
            &["operation", "status", "code"],
        )
        .expect("valid metric");
        let operations_duration = HistogramVec::new(
            HistogramOpts::new("operations_duration_seconds", "Duration of CSI operations in seconds")
                .namespace(NAMESPACE)
                .buckets(vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["operation"],
        )
        .expect("valid metric");
        // A node has no TrueNAS client; the Go node reports both as 0.
        let connection_status = IntGauge::with_opts(
            Opts::new(
                "truenas_connection_status",
                "TrueNAS connection status (1 = connected, 0 = disconnected)",
            )
            .namespace(NAMESPACE),
        )
        .expect("valid metric");
        let connections_active = IntGauge::with_opts(
            Opts::new("truenas_connections_active", "Number of active TrueNAS connections").namespace(NAMESPACE),
        )
        .expect("valid metric");
        let node_connect_total = IntCounterVec::new(
            Opts::new(
                "node_connect_total",
                "Total number of node transport connection attempts",
            )
            .namespace(NAMESPACE),
            &["transport", "result"],
        )
        .expect("valid metric");
        let nvme_path_connect_total = IntCounterVec::new(
            Opts::new(
                "nvme_path_connect_total",
                "Total number of NVMe-oF path convergence results by transport address",
            )
            .namespace(NAMESPACE),
            &["address", "result"],
        )
        .expect("valid metric");
        for collector in [
            Box::new(operations_total.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(operations_duration.clone()),
            Box::new(node_connect_total.clone()),
            Box::new(nvme_path_connect_total.clone()),
            Box::new(connection_status),
            Box::new(connections_active),
        ] {
            registry.register(collector).expect("unique metric");
        }
        Metrics {
            registry,
            operations_total,
            operations_duration,
            node_connect_total,
            nvme_path_connect_total,
        }
    }

    /// One transport attach: `transport` is "nvmeof" for the kernel initiator,
    /// "nvmeof-ublk" for nvmeublkd; `result` is "success" or "error".
    pub fn record_node_connect(&self, transport: &str, result: &str) {
        self.node_connect_total.with_label_values(&[transport, result]).inc();
    }

    pub fn node_connects(&self, transport: &str, result: &str) -> u64 {
        self.node_connect_total.with_label_values(&[transport, result]).get()
    }

    pub fn record_nvme_path_connect(&self, address: &str, result: &str) {
        self.nvme_path_connect_total.with_label_values(&[address, result]).inc();
    }

    pub fn nvme_path_connects(&self, address: &str, result: &str) -> u64 {
        self.nvme_path_connect_total.with_label_values(&[address, result]).get()
    }

    /// One finished RPC. `status` is "benign" for Aborted, NotFound and
    /// AlreadyExists (retries and idempotent outcomes), as in Go.
    pub fn record_operation(&self, operation: &str, seconds: f64, code: tonic::Code) {
        let status = match code {
            tonic::Code::Ok => "success",
            tonic::Code::Aborted | tonic::Code::NotFound | tonic::Code::AlreadyExists => "benign",
            _ => "error",
        };
        self.operations_total
            .with_label_values(&[operation, status, go_code_name(code)])
            .inc();
        self.operations_duration
            .with_label_values(&[operation])
            .observe(seconds);
    }

    pub fn render(&self) -> String {
        let mut out = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut out)
            .expect("text encoding");
        String::from_utf8(out).expect("prometheus text is UTF-8")
    }

    #[doc(hidden)]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// grpc-go's `codes.Code.String()`, the `code` label's values.
pub fn go_code_name(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        Ok => "OK",
        Cancelled => "Canceled",
        Unknown => "Unknown",
        InvalidArgument => "InvalidArgument",
        DeadlineExceeded => "DeadlineExceeded",
        NotFound => "NotFound",
        AlreadyExists => "AlreadyExists",
        PermissionDenied => "PermissionDenied",
        ResourceExhausted => "ResourceExhausted",
        FailedPrecondition => "FailedPrecondition",
        Aborted => "Aborted",
        OutOfRange => "OutOfRange",
        Unimplemented => "Unimplemented",
        Internal => "Internal",
        Unavailable => "Unavailable",
        DataLoss => "DataLoss",
        Unauthenticated => "Unauthenticated",
    }
}

/// Records every RPC the server answers: the operation is the full gRPC method
/// (`/csi.v1.Node/NodeStageVolume`). A handler's error travels as a
/// trailers-only response with `grpc-status` in the headers; a success carries
/// it in the trailers, so no header means OK.
#[derive(Clone)]
pub struct OperationsLayer(pub std::sync::Arc<Metrics>);

impl<S> tower::Layer<S> for OperationsLayer {
    type Service = Operations<S>;
    fn layer(&self, inner: S) -> Self::Service {
        Operations {
            inner,
            metrics: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Operations<S> {
    inner: S,
    metrics: std::sync::Arc<Metrics>,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, B, R> tower::Service<http::Request<B>> for Operations<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<R>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let operation = request.uri().path().to_string();
        let metrics = self.metrics.clone();
        // The clone takes the readied service's place (tower's clone-and-swap).
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let started = Instant::now();
        Box::pin(async move {
            let result = inner.call(request).await;
            let code = match &result {
                Ok(response) => response
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<i32>().ok())
                    .map_or(tonic::Code::Ok, tonic::Code::from_i32),
                Err(_) => tonic::Code::Internal,
            };
            metrics.record_operation(&operation, started.elapsed().as_secs_f64(), code);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_and_go_code_names() {
        let m = Metrics::new();
        m.record_operation("/csi.v1.Node/NodeStageVolume", 0.3, tonic::Code::Ok);
        m.record_operation("/csi.v1.Node/NodeStageVolume", 0.1, tonic::Code::Aborted);
        m.record_operation("/csi.v1.Node/NodeStageVolume", 70.0, tonic::Code::Cancelled);
        let text = m.render();
        for line in [
            r#"scale_csi_operations_total{code="OK",operation="/csi.v1.Node/NodeStageVolume",status="success"} 1"#,
            r#"scale_csi_operations_total{code="Aborted",operation="/csi.v1.Node/NodeStageVolume",status="benign"} 1"#,
            r#"scale_csi_operations_total{code="Canceled",operation="/csi.v1.Node/NodeStageVolume",status="error"} 1"#,
            r#"scale_csi_operations_duration_seconds_bucket{operation="/csi.v1.Node/NodeStageVolume",le="0.25"} 1"#,
            r#"scale_csi_operations_duration_seconds_bucket{operation="/csi.v1.Node/NodeStageVolume",le="60"} 2"#,
            "scale_csi_truenas_connection_status 0",
            "scale_csi_truenas_connections_active 0",
        ] {
            assert!(text.contains(line), "missing {line}\n{text}");
        }
    }
}
