//! The little of the Kubernetes API the node writes: Events, in-cluster, with
//! the pod's service account (token and CA under
//! /var/run/secrets/kubernetes.io/serviceaccount, the API at
//! KUBERNETES_SERVICE_HOST:KUBERNETES_SERVICE_PORT), as client-go's
//! rest.InClusterConfig finds it.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::client::conn::http1::SendRequest;
use hyper_util::rt::TokioIo;
use log::debug;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

pub const SERVICE_ACCOUNT_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

/// How long one API request may take, connect included.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Why an API write failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// 404: the object (an Event a patch names) is gone.
    NotFound,
    /// Any other non-2xx answer.
    Status(u16),
    /// No answer: connect, TLS, timeout, or a broken connection.
    Transport(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiError::NotFound => write!(f, "not found"),
            ApiError::Status(code) => write!(f, "HTTP {code}"),
            ApiError::Transport(e) => write!(f, "{e}"),
        }
    }
}

/// The Event writes the recorder makes; the tests supply a fake.
pub trait EventApi: Send + Sync + 'static {
    /// POST an Event (JSON) to its namespace.
    fn create(&self, namespace: &str, event: Vec<u8>) -> impl Future<Output = Result<(), ApiError>> + Send;
    /// Strategic-merge PATCH an existing Event.
    fn patch(&self, namespace: &str, name: &str, patch: Vec<u8>) -> impl Future<Output = Result<(), ApiError>> + Send;
}

/// Where the in-cluster API is and how to authenticate to it.
pub struct InCluster {
    host: String,
    port: u16,
    token_file: PathBuf,
    tls: TlsConnector,
    server_name: ServerName<'static>,
}

impl InCluster {
    /// The in-cluster API from the service account directory and the
    /// KUBERNETES_SERVICE_HOST/PORT values. `Ok(None)`: not in a cluster (no
    /// host or port, or no token), like rest.ErrNotInCluster. An error: in a
    /// cluster, but the CA cannot be used.
    pub fn from_env(sa_dir: &Path, host: Option<String>, port: Option<String>) -> Result<Option<Self>> {
        let (Some(host), Some(port)) = (host.filter(|h| !h.is_empty()), port.filter(|p| !p.is_empty())) else {
            return Ok(None);
        };
        let token_file = sa_dir.join("token");
        if !token_file.is_file() {
            return Ok(None);
        }
        let port: u16 = port
            .parse()
            .with_context(|| format!("KUBERNETES_SERVICE_PORT {port:?}"))?;
        let ca_file = sa_dir.join("ca.crt");
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_file_iter(&ca_file).with_context(|| format!("read {}", ca_file.display()))? {
            let cert = cert.with_context(|| format!("parse {}", ca_file.display()))?;
            roots.add(cert).with_context(|| format!("use {}", ca_file.display()))?;
        }
        if roots.is_empty() {
            bail!("{} holds no certificate", ca_file.display());
        }
        let config = ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context("TLS protocol versions")?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let server_name = ServerName::try_from(host.clone()).with_context(|| format!("API server name {host:?}"))?;
        Ok(Some(InCluster {
            host,
            port,
            token_file,
            tls: TlsConnector::from(Arc::new(config)),
            server_name,
        }))
    }

    /// host:port as a URL authority (an IPv6 address in brackets).
    fn authority(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }
}

/// An HTTP/1.1 client to the API that keeps one connection open.
pub struct Client {
    cluster: InCluster,
    conn: tokio::sync::Mutex<Option<SendRequest<Full<Bytes>>>>,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl Client {
    pub fn new(cluster: InCluster) -> Self {
        Client {
            cluster,
            conn: tokio::sync::Mutex::new(None),
        }
    }

    fn connect(&self) -> BoxFuture<'_, Result<SendRequest<Full<Bytes>>, ApiError>> {
        Box::pin(async move {
            let tcp = TcpStream::connect((self.cluster.host.as_str(), self.cluster.port))
                .await
                .map_err(|e| ApiError::Transport(format!("connect {}: {e}", self.cluster.authority())))?;
            let _ = tcp.set_nodelay(true);
            let tls = self
                .cluster
                .tls
                .connect(self.cluster.server_name.clone(), tcp)
                .await
                .map_err(|e| ApiError::Transport(format!("TLS to {}: {e}", self.cluster.authority())))?;
            let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls))
                .await
                .map_err(|e| ApiError::Transport(format!("HTTP handshake: {e}")))?;
            tokio::spawn(async move {
                if let Err(e) = conn.await {
                    debug!("Kubernetes API connection closed: {e}");
                }
            });
            Ok(sender)
        })
    }

    async fn request(
        &self,
        method: http::Method,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> Result<(), ApiError> {
        // The token is re-read on each request: a projected token rotates.
        let token = tokio::fs::read_to_string(&self.cluster.token_file)
            .await
            .map_err(|e| ApiError::Transport(format!("read token: {e}")))?;
        let body = Bytes::from(body);
        let build = || {
            http::Request::builder()
                .method(method.clone())
                .uri(path)
                .header(http::header::HOST, self.cluster.authority())
                .header(http::header::AUTHORIZATION, format!("Bearer {}", token.trim()))
                .header(http::header::CONTENT_TYPE, content_type)
                .header(http::header::ACCEPT, "application/json")
                .header(
                    http::header::USER_AGENT,
                    concat!("scale-csi-node/", env!("CARGO_PKG_VERSION")),
                )
                .body(Full::new(body.clone()))
                .map_err(|e| ApiError::Transport(format!("build request: {e}")))
        };
        let mut conn = self.conn.lock().await;
        // A kept connection the server closed fails the first send: one more
        // try on a new connection.
        let reused = conn.as_ref().is_some_and(|c| !c.is_closed());
        let mut attempts = if reused { 2 } else { 1 };
        loop {
            attempts -= 1;
            let mut sender = match conn.take().filter(|c| !c.is_closed()) {
                Some(sender) => sender,
                None => self.connect().await?,
            };
            let sent = async {
                sender
                    .ready()
                    .await
                    .map_err(|e| ApiError::Transport(format!("connection: {e}")))?;
                sender
                    .send_request(build()?)
                    .await
                    .map_err(|e| ApiError::Transport(format!("send: {e}")))
            }
            .await;
            let response = match sent {
                Ok(response) => response,
                Err(e) if attempts > 0 => {
                    debug!("Kubernetes API request on a kept connection failed ({e}); reconnecting");
                    continue;
                }
                Err(e) => return Err(e),
            };
            let status = response.status();
            // Read the body to the end so the connection can be reused.
            let read = response.into_body().collect().await;
            if read.is_ok() {
                *conn = Some(sender);
            }
            return match status.as_u16() {
                200..=299 => Ok(()),
                404 => Err(ApiError::NotFound),
                code => Err(ApiError::Status(code)),
            };
        }
    }
}

impl EventApi for Client {
    async fn create(&self, namespace: &str, event: Vec<u8>) -> Result<(), ApiError> {
        let path = format!("/api/v1/namespaces/{namespace}/events");
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.request(http::Method::POST, &path, "application/json", event),
        )
        .await
        .unwrap_or_else(|_| Err(ApiError::Transport("timed out".into())))
    }

    async fn patch(&self, namespace: &str, name: &str, patch: Vec<u8>) -> Result<(), ApiError> {
        let path = format!("/api/v1/namespaces/{namespace}/events/{name}");
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.request(
                http::Method::PATCH,
                &path,
                "application/strategic-merge-patch+json",
                patch,
            ),
        )
        .await
        .unwrap_or_else(|_| Err(ApiError::Transport("timed out".into())))
    }
}
