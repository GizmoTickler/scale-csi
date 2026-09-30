//! nvmeublkd's control protocol, as the Go node speaks it (`pkg/util/nvmeublk.go`):
//! one JSON request per connection on a root-only unix socket, answered by one
//! JSON line. Every call is bounded by the caller's deadline, dial included, so
//! a wedged daemon cannot hang a CSI RPC. "Nothing is listening" (the socket is
//! absent or refuses) is kept apart from "the daemon said no": kubelet gets
//! Unavailable for the first and retries.

use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// A list of every device on a node is a few KiB; anything near this is not a
/// daemon speaking the protocol.
const MAX_RESPONSE: usize = 1 << 20;

#[derive(Debug)]
pub enum Error {
    /// The socket is absent or refuses connections: the daemon is not running.
    Unavailable(String),
    /// The caller's deadline ended the exchange.
    Deadline(String),
    /// The daemon answered and refused.
    Refused(String),
    /// Anything else: a bad request, a malformed answer, an I/O error.
    Other(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Unavailable(s) => write!(f, "nvmeublkd control socket is unavailable: {s}"),
            Error::Deadline(s) => write!(f, "{s}: deadline exceeded"),
            Error::Refused(s) => write!(f, "nvmeublkd: {s}"),
            Error::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    /// The gRPC code a node RPC returns for it (Go: nvmeUblkStatusCode).
    pub fn code(&self) -> tonic::Code {
        match self {
            Error::Unavailable(_) => tonic::Code::Unavailable,
            Error::Deadline(_) => tonic::Code::DeadlineExceeded,
            _ => tonic::Code::Internal,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct AttachRequest {
    /// The caller's unique name for the device; the daemon is idempotent per volume.
    pub volume: String,
    pub subnqn: String,
    /// Target portals, each "host:port".
    pub addrs: Vec<String>,
    /// Both or neither: with neither the daemon connects as its own node.
    pub hostnqn: String,
    pub hostid: String,
    /// 0 lets the daemon size the layout.
    pub queues: u32,
    pub depth: u32,
    pub zero_copy: bool,
    pub napi_us: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DevicePath {
    pub addr: String,
    pub up: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Device {
    pub volume: String,
    pub subnqn: String,
    pub dev_id: i64,
    pub path: String,
    #[serde(default)]
    pub paths: Vec<DevicePath>,
    /// On an attach: the volume was already attached.
    #[serde(default)]
    pub existing: bool,
}

#[derive(Serialize)]
struct AttachWire<'a> {
    op: &'static str,
    volume: &'a str,
    subnqn: &'a str,
    addrs: &'a [String],
    #[serde(skip_serializing_if = "str::is_empty")]
    hostnqn: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    hostid: &'a str,
    #[serde(skip_serializing_if = "is_zero")]
    queues: u32,
    #[serde(skip_serializing_if = "is_zero")]
    depth: u32,
    zero_copy: bool,
    napi_us: u32,
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

#[derive(Deserialize, Default)]
struct Response {
    #[serde(default)]
    ok: bool,
    #[serde(default)]
    error: String,
    dev_id: Option<i64>,
    #[serde(default)]
    path: String,
    #[serde(default)]
    existing: bool,
    #[serde(default)]
    absent: bool,
    #[serde(default)]
    devices: Vec<Device>,
}

pub const DEFAULT_SOCKET: &str = "/run/nvmeublk/nvmeublkd.sock";

#[derive(Clone, Debug)]
pub struct Client {
    socket: PathBuf,
}

impl Client {
    pub fn new(socket: &Path) -> Self {
        let socket = if socket.as_os_str().is_empty() {
            PathBuf::from(DEFAULT_SOCKET)
        } else {
            socket.to_path_buf()
        };
        Client { socket }
    }

    pub fn socket(&self) -> &Path {
        &self.socket
    }

    pub async fn attach(&self, req: &AttachRequest, deadline: Instant) -> Result<Device, Error> {
        if req.volume.is_empty() || req.subnqn.is_empty() || req.addrs.is_empty() {
            return Err(Error::Other(
                "nvmeublkd attach needs a volume, a subsystem NQN and at least one address".into(),
            ));
        }
        // The daemon falls back to its own identity unless both are given; under
        // strict fencing that host is foreign and the connect is refused.
        if req.hostnqn.is_empty() != req.hostid.is_empty() {
            return Err(Error::Other(
                "nvmeublkd attach needs both a host NQN and a host ID, or neither".into(),
            ));
        }
        let wire = AttachWire {
            op: "attach",
            volume: &req.volume,
            subnqn: &req.subnqn,
            addrs: &req.addrs,
            hostnqn: &req.hostnqn,
            hostid: &req.hostid,
            queues: req.queues,
            depth: req.depth,
            zero_copy: req.zero_copy,
            napi_us: req.napi_us,
        };
        let what = format!("attach {}", req.volume);
        let resp = self
            .call(&serde_json::to_vec(&wire).expect("encodable"), deadline, &what)
            .await?;
        match resp.dev_id {
            Some(dev_id) if !resp.path.is_empty() => Ok(Device {
                volume: req.volume.clone(),
                subnqn: req.subnqn.clone(),
                dev_id,
                path: resp.path,
                paths: Vec::new(),
                existing: resp.existing,
            }),
            _ => Err(Error::Other(format!("{what}: nvmeublkd response carries no device"))),
        }
    }

    /// Idempotent: `Ok(true)` when the daemon had no such volume.
    pub async fn detach(&self, volume: &str, deadline: Instant) -> Result<bool, Error> {
        if volume.is_empty() {
            return Err(Error::Other("nvmeublkd detach needs a volume".into()));
        }
        let request = serde_json::json!({"op": "detach", "volume": volume});
        let resp = self
            .call(request.to_string().as_bytes(), deadline, &format!("detach {volume}"))
            .await?;
        Ok(resp.absent)
    }

    pub async fn list(&self, deadline: Instant) -> Result<Vec<Device>, Error> {
        Ok(self.call(br#"{"op":"list"}"#, deadline, "list").await?.devices)
    }

    async fn call(&self, request: &[u8], deadline: Instant, what: &str) -> Result<Response, Error> {
        let deadline = tokio::time::Instant::from_std(deadline);
        let exchange = async {
            let mut conn = UnixStream::connect(&self.socket).await.map_err(|e| {
                use std::io::ErrorKind::{ConnectionRefused, NotFound};
                if matches!(e.kind(), NotFound | ConnectionRefused) {
                    Error::Unavailable(format!("{}: {e}", self.socket.display()))
                } else {
                    Error::Other(format!("{what}: connect {}: {e}", self.socket.display()))
                }
            })?;
            let mut line = request.to_vec();
            line.push(b'\n');
            conn.write_all(&line)
                .await
                .map_err(|e| Error::Other(format!("{what}: send request: {e}")))?;
            let mut reader = BufReader::new(conn);
            let mut response = Vec::new();
            loop {
                let buf = reader
                    .fill_buf()
                    .await
                    .map_err(|e| Error::Other(format!("{what}: read response: {e}")))?;
                if buf.is_empty() {
                    // EOF: a final line without its newline still counts.
                    break;
                }
                if let Some(end) = buf.iter().position(|b| *b == b'\n') {
                    response.extend_from_slice(&buf[..end]);
                    break;
                }
                let n = buf.len();
                response.extend_from_slice(buf);
                reader.consume(n);
                if response.len() > MAX_RESPONSE {
                    return Err(Error::Other(format!("{what}: response exceeds {MAX_RESPONSE} bytes")));
                }
            }
            if response.len() > MAX_RESPONSE {
                return Err(Error::Other(format!("{what}: response exceeds {MAX_RESPONSE} bytes")));
            }
            if response.is_empty() {
                return Err(Error::Other(format!(
                    "{what}: read response: connection closed without an answer"
                )));
            }
            let resp: Response =
                serde_json::from_slice(&response).map_err(|e| Error::Other(format!("{what}: decode response: {e}")))?;
            if !resp.ok {
                if resp.error.is_empty() {
                    return Err(Error::Refused(format!("{what}: refused the request without a reason")));
                }
                return Err(Error::Refused(format!("{what}: {}", resp.error)));
            }
            Ok(resp)
        };
        match tokio::time::timeout_at(deadline, exchange).await {
            Ok(result) => result,
            Err(_) => Err(Error::Deadline(what.to_string())),
        }
    }
}

/// `/dev/ublkbN` (a whole disk; `ublkbNpM` is not a staged device).
pub fn is_ublk_device(path: &str) -> bool {
    let base = Path::new(path).file_name().and_then(|n| n.to_str()).unwrap_or_default();
    base.strip_prefix("ublkb")
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::UnixListener;

    /// A one-shot daemon: records the request line and answers `reply` (None: hang).
    async fn daemon(reply: Option<&'static str>) -> (tempfile::TempDir, PathBuf, tokio::task::JoinHandle<String>) {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let task = tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = conn.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();
            match reply {
                Some(r) => conn.write_all(r.as_bytes()).await.unwrap(),
                None => tokio::time::sleep(Duration::from_secs(30)).await,
            }
            request
        });
        (dir, socket, task)
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn attach_request() -> AttachRequest {
        AttachRequest {
            volume: "pvc-1".into(),
            subnqn: "nqn.sub".into(),
            addrs: vec!["192.0.2.1:4420".into()],
            hostnqn: "nqn.host".into(),
            hostid: "0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718".into(),
            zero_copy: true,
            napi_us: 200,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn attach_wire_format_and_answer() {
        let (_d, socket, task) = daemon(Some(
            "{\"ok\":true,\"dev_id\":3,\"path\":\"/dev/ublkb3\",\"existing\":true}\n",
        ))
        .await;
        let dev = Client::new(&socket).attach(&attach_request(), soon()).await.unwrap();
        assert_eq!((dev.dev_id, dev.path.as_str(), dev.existing), (3, "/dev/ublkb3", true));
        let sent: serde_json::Value = serde_json::from_str(task.await.unwrap().trim_end()).unwrap();
        assert_eq!(
            sent,
            serde_json::json!({"op": "attach", "volume": "pvc-1", "subnqn": "nqn.sub", "addrs": ["192.0.2.1:4420"],
                "hostnqn": "nqn.host", "hostid": "0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718", "zero_copy": true, "napi_us": 200}),
            "queues and depth 0 are omitted so the daemon sizes the layout"
        );
    }

    #[tokio::test]
    async fn half_an_identity_is_refused_before_any_connect() {
        let client = Client::new(Path::new("/nonexistent/x.sock"));
        let mut req = attach_request();
        req.hostid.clear();
        assert!(matches!(client.attach(&req, soon()).await, Err(Error::Other(_))));
        req.hostnqn.clear();
        // Both empty is allowed; this one then fails only for lack of a daemon.
        assert!(matches!(client.attach(&req, soon()).await, Err(Error::Unavailable(_))));
    }

    #[tokio::test]
    async fn errors_are_classified() {
        let err = Client::new(Path::new("/nonexistent/x.sock"))
            .list(soon())
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);

        let (_d, socket, _t) = daemon(Some(
            "{\"ok\":false,\"error\":\"volume pvc-1 is being recovered, retry\"}\n",
        ))
        .await;
        let err = Client::new(&socket).detach("pvc-1", soon()).await.unwrap_err();
        assert!(matches!(err, Error::Refused(ref s) if s.contains("retry")), "{err}");
        assert_eq!(err.code(), tonic::Code::Internal);

        let (_d, socket, _t) = daemon(Some("{\"ok\":false}")).await;
        assert!(
            matches!(Client::new(&socket).list(soon()).await, Err(Error::Refused(_))),
            "an empty reason is still a refusal"
        );

        let (_d, socket, _t) = daemon(None).await;
        let started = Instant::now();
        let err = Client::new(&socket)
            .list(Instant::now() + Duration::from_millis(200))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
        assert!(started.elapsed() < Duration::from_secs(2));

        let (_d, socket, _t) = daemon(Some("{\"ok\":true}\n")).await;
        assert!(
            matches!(
                Client::new(&socket).attach(&attach_request(), soon()).await,
                Err(Error::Other(_))
            ),
            "no device"
        );
    }

    #[tokio::test]
    async fn detach_absent_and_list() {
        let (_d, socket, _t) = daemon(Some("{\"ok\":true,\"absent\":true}")).await;
        assert!(
            Client::new(&socket).detach("pvc-1", soon()).await.unwrap(),
            "a final line without newline counts"
        );
        let (_d, socket, _t) = daemon(Some(
            "{\"ok\":true,\"devices\":[{\"volume\":\"pvc-1\",\"subnqn\":\"nqn.sub\",\"dev_id\":0,\"path\":\"/dev/ublkb0\",\"paths\":[{\"addr\":\"192.0.2.1:4420\",\"up\":false}],\"recovering\":true}]}\n",
        ))
        .await;
        let devices = Client::new(&socket).list(soon()).await.unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(
            devices[0].paths,
            vec![DevicePath {
                addr: "192.0.2.1:4420".into(),
                up: false
            }]
        );
    }

    #[test]
    fn ublk_device_names() {
        assert!(is_ublk_device("/dev/ublkb0") && is_ublk_device("ublkb12"));
        assert!(!is_ublk_device("/dev/ublkb1p1") && !is_ublk_device("/dev/ublkb") && !is_ublk_device("/dev/nvme0n1"));
        assert!(!is_ublk_device(""));
    }
}
