//! Kubernetes Events: the objects client-go's recorder would write for the Go
//! node, the queue, aggregation, rate limiting, the fallback to the log, and
//! the API client over TLS.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::events::{self, Events, ObjectRef};
use crate::kube_api::{ApiError, Client, EventApi, InCluster};
use crate::kube_events::{self, KubeEvents, Op, Pending, SinkKind, Source};
use crate::metrics::Metrics;
use crate::nvme_kernel::REASON_MULTIPATH_UNAGGREGATED;
use crate::publish::REASON_MOUNT_FAILED;

const GO_EVENTS: &str = include_str!("../../../pkg/driver/events.go");

fn source() -> Source {
    Source {
        component: "org.scale.csi.nvmeof".into(),
        host: "k8s-0".into(),
    }
}

fn pv(name: &str) -> ObjectRef {
    ObjectRef::Pv { name: name.into() }
}

fn at(secs: u64, nanos: u32) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs, nanos)
}

fn pending(object: &ObjectRef, reason: &str, message: &str, wall: SystemTime, seen: Instant) -> Pending {
    Pending {
        object: object.clone(),
        event_type: kube_events::EVENT_TYPE_WARNING,
        reason: reason.into(),
        message: message.into(),
        at: wall,
        seen,
    }
}

/// The string value of a Go constant `name = "value"` in events.go.
fn go_const(name: &str) -> String {
    let line = GO_EVENTS
        .lines()
        .map(str::trim)
        .find(|l| l.split_whitespace().next() == Some(name) && l.contains('='))
        .unwrap_or_else(|| panic!("{name} not in pkg/driver/events.go"));
    line.split('"').nth(1).expect("quoted value").to_string()
}

/// The `field: "value"` of the ObjectReference a Go `func <name>(` returns.
fn go_ref_field(func: &str, field: &str) -> String {
    let start = GO_EVENTS
        .find(&format!("func {func}("))
        .unwrap_or_else(|| panic!("func {func} not in events.go"));
    let body = &GO_EVENTS[start..start + GO_EVENTS[start..].find("\n}\n").expect("end of func")];
    let line = body
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with(&format!("{field}:")))
        .unwrap_or_else(|| panic!("{func} sets no {field}"));
    line.split('"').nth(1).expect("quoted value").to_string()
}

#[test]
fn reasons_are_the_go_nodes() {
    for (go, rust) in [
        ("EventReasonMountFailed", REASON_MOUNT_FAILED),
        ("EventReasonNVMeConnectFailed", events::REASON_NVME_CONNECT_FAILED),
        ("EventReasonNVMePathDegraded", events::REASON_NVME_PATH_DEGRADED),
        ("EventReasonNVMeMultipathUnaggregated", REASON_MULTIPATH_UNAGGREGATED),
    ] {
        assert_eq!(rust, go_const(go), "{go}");
    }
    // The Go node records them all as Warning (recordWarningEvent).
    assert_eq!(kube_events::EVENT_TYPE_WARNING, "Warning");
}

#[test]
fn involved_objects_are_the_go_nodes() {
    for (func, object) in [
        (
            "PodRef",
            ObjectRef::Pod {
                namespace: "apps".into(),
                name: "web-0".into(),
            },
        ),
        (
            "PVCRef",
            ObjectRef::Pvc {
                namespace: "apps".into(),
                name: "data".into(),
            },
        ),
        ("PVRef", pv("pvc-1")),
        ("NodeRef", ObjectRef::Node { name: "k8s-0".into() }),
    ] {
        assert_eq!(object.kind(), go_ref_field(func, "Kind"), "{func}");
        assert_eq!(object.api_version(), go_ref_field(func, "APIVersion"), "{func}");
        let namespaced = matches!(object, ObjectRef::Pod { .. } | ObjectRef::Pvc { .. });
        let body = &GO_EVENTS[GO_EVENTS.find(&format!("func {func}(")).unwrap()..];
        let body = &body[..body.find("\n}\n").unwrap()];
        assert_eq!(body.contains("Namespace:"), namespaced, "{func} namespace");
    }
}

#[test]
fn the_event_object_client_go_creates() {
    let pod = ObjectRef::Pod {
        namespace: "apps".into(),
        name: "web-0".into(),
    };
    let wall = at(1_790_000_000, 123);
    let event = pending(&pod, REASON_MOUNT_FAILED, "mount failed: busy", wall, Instant::now());
    let name = kube_events::event_name(&pod, wall);
    assert_eq!(
        name, "web-0.18d75b8423f3007b",
        "client-go GenerateEventName: name.<hex unix nanos>"
    );
    assert_eq!(
        kube_events::event_object(&source(), &event, &name, 1, wall),
        json!({
            "apiVersion": "v1",
            "kind": "Event",
            "metadata": {"name": "web-0.18d75b8423f3007b", "namespace": "apps"},
            "involvedObject": {"kind": "Pod", "namespace": "apps", "name": "web-0", "apiVersion": "v1"},
            "reason": "MountFailed",
            "message": "mount failed: busy",
            "source": {"component": "org.scale.csi.nvmeof", "host": "k8s-0"},
            "firstTimestamp": "2026-09-21T14:13:20Z",
            "lastTimestamp": "2026-09-21T14:13:20Z",
            "count": 1,
            "type": "Warning",
            "reportingComponent": "org.scale.csi.nvmeof",
            "reportingInstance": "k8s-0",
        })
    );
    // A cluster-scoped object's events go to "default" (client-go makeEvent),
    // and its reference carries no namespace.
    for object in [pv("pvc-1"), ObjectRef::Node { name: "k8s-0".into() }] {
        let event = pending(&object, events::REASON_NVME_PATH_DEGRADED, "m", wall, Instant::now());
        let body = kube_events::event_object(&source(), &event, "n", 1, wall);
        assert_eq!(body["metadata"]["namespace"], "default", "{object}");
        assert!(body["involvedObject"].get("namespace").is_none(), "{object}");
        assert_eq!(body["involvedObject"]["kind"], object.kind());
    }
    let pvc = ObjectRef::Pvc {
        namespace: "db".into(),
        name: "data".into(),
    };
    let event = pending(&pvc, events::REASON_NVME_CONNECT_FAILED, "m", wall, Instant::now());
    let body = kube_events::event_object(&source(), &event, "n", 1, wall);
    assert_eq!(body["metadata"]["namespace"], "db");
    assert_eq!(
        body["involvedObject"],
        json!({"kind": "PersistentVolumeClaim", "namespace": "db", "name": "data", "apiVersion": "v1"})
    );
}

#[test]
fn an_event_name_too_long_for_a_name_is_a_hash() {
    let wall = at(1_790_000_000, 123);
    let fits = ObjectRef::Pod {
        namespace: "apps".into(),
        name: "p".repeat(253 - 17),
    };
    assert_eq!(
        kube_events::event_name(&fits, wall),
        format!("{}.18d75b8423f3007b", fits.name())
    );
    let long = ObjectRef::Pod {
        namespace: "apps".into(),
        name: "p".repeat(253 - 16),
    };
    let name = kube_events::event_name(&long, wall);
    assert_eq!(name.len(), 64, "{name}");
    assert!(name.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    assert_ne!(name, kube_events::event_name(&long, at(1_790_000_000, 124)));
}

#[test]
fn timestamps_are_rfc3339_utc_seconds() {
    assert_eq!(kube_events::rfc3339(UNIX_EPOCH), "1970-01-01T00:00:00Z");
    assert_eq!(kube_events::rfc3339(at(951_782_400, 0)), "2000-02-29T00:00:00Z");
    assert_eq!(
        kube_events::rfc3339(at(1_790_000_000, 999_999_999)),
        "2026-09-21T14:13:20Z"
    );
    assert_eq!(kube_events::rfc3339(at(4_102_444_799, 0)), "2099-12-31T23:59:59Z");
}

#[derive(Debug, Clone, PartialEq)]
enum Call {
    Create {
        namespace: String,
        body: Value,
    },
    Patch {
        namespace: String,
        name: String,
        body: Value,
    },
}

/// Records each write and answers from a script (Ok when it runs out).
#[derive(Default)]
struct FakeApi {
    calls: Mutex<Vec<Call>>,
    answers: Mutex<VecDeque<Result<(), ApiError>>>,
}

impl FakeApi {
    fn answer(&self, answer: Result<(), ApiError>) {
        self.answers.lock().unwrap().push_back(answer);
    }

    fn take(&self) -> Vec<Call> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }

    fn next(&self) -> Result<(), ApiError> {
        self.answers.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }
}

impl EventApi for Arc<FakeApi> {
    async fn create(&self, namespace: &str, event: Vec<u8>) -> Result<(), ApiError> {
        self.calls.lock().unwrap().push(Call::Create {
            namespace: namespace.into(),
            body: serde_json::from_slice(&event).unwrap(),
        });
        self.next()
    }

    async fn patch(&self, namespace: &str, name: &str, patch: Vec<u8>) -> Result<(), ApiError> {
        self.calls.lock().unwrap().push(Call::Patch {
            namespace: namespace.into(),
            name: name.into(),
            body: serde_json::from_slice(&patch).unwrap(),
        });
        self.next()
    }
}

fn created(call: &Call) -> &Value {
    match call {
        Call::Create { body, .. } => body,
        other => panic!("not a create: {other:?}"),
    }
}

#[tokio::test]
async fn an_identical_event_counts_on_the_first() {
    let api = Arc::new(FakeApi::default());
    let mut correlator = kube_events::Correlator::new(source());
    let object = pv("pvc-1");
    let t0 = Instant::now();
    let w0 = at(1_790_000_000, 0);
    let event = |secs: u64, message: &str| {
        pending(
            &object,
            events::REASON_NVME_PATH_DEGRADED,
            message,
            w0 + Duration::from_secs(secs),
            t0 + Duration::from_secs(secs),
        )
    };

    kube_events::write(&api, &mut correlator, &event(0, "path down"))
        .await
        .unwrap();
    let calls = api.take();
    assert_eq!(calls.len(), 1);
    let name = created(&calls[0])["metadata"]["name"].as_str().unwrap().to_string();
    assert_eq!(created(&calls[0])["count"], 1);

    // The same event a minute later: the Event's count, not a new Event.
    kube_events::write(&api, &mut correlator, &event(60, "path down"))
        .await
        .unwrap();
    kube_events::write(&api, &mut correlator, &event(120, "path down"))
        .await
        .unwrap();
    assert_eq!(
        api.take(),
        vec![
            Call::Patch {
                namespace: "default".into(),
                name: name.clone(),
                body: json!({"count": 2, "lastTimestamp": "2026-09-21T14:14:20Z", "message": "path down"}),
            },
            Call::Patch {
                namespace: "default".into(),
                name: name.clone(),
                body: json!({"count": 3, "lastTimestamp": "2026-09-21T14:15:20Z", "message": "path down"}),
            },
        ]
    );

    // A different message is a different event.
    kube_events::write(&api, &mut correlator, &event(130, "path up"))
        .await
        .unwrap();
    let calls = api.take();
    assert_eq!(calls.len(), 1);
    assert_eq!(created(&calls[0])["message"], "path up");
    assert_ne!(created(&calls[0])["metadata"]["name"], name.as_str());

    // Past the window since the last repeat: a new Event.
    let late = 120 + kube_events::AGGREGATE_WINDOW.as_secs() + 1;
    kube_events::write(&api, &mut correlator, &event(late, "path down"))
        .await
        .unwrap();
    let calls = api.take();
    assert_eq!(calls.len(), 1, "{calls:?}");
    assert_eq!(created(&calls[0])["count"], 1);
    assert_ne!(created(&calls[0])["metadata"]["name"], name.as_str());
}

#[tokio::test]
async fn a_repeat_of_a_gone_event_creates_it_again() {
    let api = Arc::new(FakeApi::default());
    let mut correlator = kube_events::Correlator::new(source());
    let object = pv("pvc-1");
    let (t0, w0) = (Instant::now(), at(1_790_000_000, 0));
    let first = pending(&object, REASON_MOUNT_FAILED, "m", w0, t0);
    let again = pending(
        &object,
        REASON_MOUNT_FAILED,
        "m",
        w0 + Duration::from_secs(5),
        t0 + Duration::from_secs(5),
    );
    kube_events::write(&api, &mut correlator, &first).await.unwrap();
    let name = created(&api.take()[0])["metadata"]["name"].clone();
    api.answer(Err(ApiError::NotFound));
    kube_events::write(&api, &mut correlator, &again).await.unwrap();
    let calls = api.take();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert!(matches!(calls[0], Call::Patch { .. }));
    let body = created(&calls[1]);
    assert_eq!(body["metadata"]["name"], name);
    assert_eq!(body["count"], 2);
    assert_eq!(body["firstTimestamp"], "2026-09-21T14:13:20Z");
    assert_eq!(body["lastTimestamp"], "2026-09-21T14:13:25Z");
}

#[tokio::test]
async fn a_refused_write_is_dropped_and_not_counted_on() {
    let api = Arc::new(FakeApi::default());
    let mut correlator = kube_events::Correlator::new(source());
    let object = pv("pvc-1");
    let t0 = Instant::now();
    let event = pending(&object, REASON_MOUNT_FAILED, "m", at(1_790_000_000, 0), t0);
    api.answer(Err(ApiError::Status(403)));
    assert_eq!(
        kube_events::write(&api, &mut correlator, &event).await,
        Err(kube_events::DROP_API_ERROR)
    );
    // Nothing was written, so the next one creates.
    kube_events::write(&api, &mut correlator, &event).await.unwrap();
    let calls = api.take();
    assert_eq!(calls.len(), 2);
    assert!(calls.iter().all(|c| matches!(c, Call::Create { .. })), "{calls:?}");
}

#[tokio::test]
async fn an_object_gets_a_burst_then_one_event_per_refill() {
    let api = Arc::new(FakeApi::default());
    let mut correlator = kube_events::Correlator::new(source());
    let noisy = pv("pvc-noisy");
    let t0 = Instant::now();
    let w0 = at(1_790_000_000, 0);
    let burst = kube_events::SPAM_BURST as usize;
    for i in 0..burst {
        let event = pending(&noisy, REASON_MOUNT_FAILED, &format!("m{i}"), w0, t0);
        kube_events::write(&api, &mut correlator, &event).await.unwrap();
    }
    let over = pending(&noisy, REASON_MOUNT_FAILED, "over", w0, t0);
    assert_eq!(
        kube_events::write(&api, &mut correlator, &over).await,
        Err(kube_events::DROP_RATE_LIMITED)
    );
    // Repeats of an identical event count against the object too.
    let repeat = pending(&noisy, REASON_MOUNT_FAILED, "m0", w0, t0 + Duration::from_secs(1));
    assert_eq!(
        kube_events::write(&api, &mut correlator, &repeat).await,
        Err(kube_events::DROP_RATE_LIMITED)
    );
    assert_eq!(api.take().len(), burst);

    // Another object has its own burst.
    let quiet = pending(&pv("pvc-quiet"), REASON_MOUNT_FAILED, "m", w0, t0);
    kube_events::write(&api, &mut correlator, &quiet).await.unwrap();

    // One refill period later, one more.
    let later = t0 + kube_events::SPAM_REFILL;
    let refilled = pending(&noisy, REASON_MOUNT_FAILED, "later", w0, later);
    kube_events::write(&api, &mut correlator, &refilled).await.unwrap();
    let again = pending(&noisy, REASON_MOUNT_FAILED, "later2", w0, later);
    assert_eq!(
        kube_events::write(&api, &mut correlator, &again).await,
        Err(kube_events::DROP_RATE_LIMITED)
    );
    assert_eq!(api.take().len(), 2);
}

#[tokio::test]
async fn a_full_queue_drops_and_counts_without_blocking() {
    let metrics = Arc::new(Metrics::new());
    let (sink, mut queue) = KubeEvents::queue(2, metrics.clone());
    let object = pv("pvc-1");
    let started = Instant::now();
    for i in 0..5 {
        sink.warning(&object, REASON_MOUNT_FAILED, &format!("m{i}"));
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "warning must not wait for the API"
    );
    assert_eq!(metrics.events_dropped(kube_events::DROP_QUEUE_FULL), 3);
    assert!(
        metrics
            .render()
            .contains(r#"scale_csi_events_dropped_total{reason="queue_full"} 3"#)
    );
    let mut queued = Vec::new();
    while let Ok(event) = queue.try_recv() {
        queued.push(event.message);
    }
    assert_eq!(queued, ["m0", "m1"]);

    // The writer gone, events are dropped as closed.
    drop(queue);
    sink.warning(&object, REASON_MOUNT_FAILED, "late");
    assert_eq!(metrics.events_dropped(kube_events::DROP_CLOSED), 1);
}

#[tokio::test]
async fn the_writer_counts_what_the_api_refuses() {
    let metrics = Arc::new(Metrics::new());
    let api = Arc::new(FakeApi::default());
    api.answer(Err(ApiError::Transport("connection refused".into())));
    let (sink, queue) = KubeEvents::queue(8, metrics.clone());
    let object = pv("pvc-1");
    sink.warning(&object, REASON_MOUNT_FAILED, "first");
    sink.warning(&object, REASON_MOUNT_FAILED, "second");
    drop(sink);
    kube_events::run(queue, api.clone(), source(), metrics.clone()).await;
    assert_eq!(metrics.events_dropped(kube_events::DROP_API_ERROR), 1);
    let calls = api.take();
    assert_eq!(calls.len(), 2);
    assert_eq!(created(&calls[1])["message"], "second");
}

fn sa_dir(token: bool, ca: Option<&str>) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    if token {
        std::fs::write(dir.path().join("token"), "tok-1\n").unwrap();
    }
    if let Some(ca) = ca {
        std::fs::write(dir.path().join("ca.crt"), ca).unwrap();
    }
    dir
}

fn chosen(dir: &Path, host: Option<&str>, port: Option<&str>) -> SinkKind {
    kube_events::sink(
        dir,
        host.map(Into::into),
        port.map(Into::into),
        source(),
        Arc::new(Metrics::new()),
    )
    .1
}

#[tokio::test]
async fn outside_a_cluster_events_go_to_the_log() {
    let tls = TestTls::new();
    let ok = sa_dir(true, Some(&tls.ca_pem));
    assert_eq!(chosen(ok.path(), Some("127.0.0.1"), Some("443")), SinkKind::Kubernetes);
    // No API address, no token: not in a cluster.
    assert_eq!(chosen(ok.path(), None, Some("443")), SinkKind::Log);
    assert_eq!(chosen(ok.path(), Some("127.0.0.1"), None), SinkKind::Log);
    assert_eq!(chosen(ok.path(), Some(""), Some("443")), SinkKind::Log);
    let no_token = sa_dir(false, Some(&tls.ca_pem));
    assert_eq!(chosen(no_token.path(), Some("127.0.0.1"), Some("443")), SinkKind::Log);
    // In a cluster, but no usable CA or port: the log, not a failed start.
    let no_ca = sa_dir(true, None);
    assert_eq!(chosen(no_ca.path(), Some("127.0.0.1"), Some("443")), SinkKind::Log);
    let bad_ca = sa_dir(true, Some("not a certificate"));
    assert_eq!(chosen(bad_ca.path(), Some("127.0.0.1"), Some("443")), SinkKind::Log);
    assert_eq!(chosen(ok.path(), Some("127.0.0.1"), Some("https")), SinkKind::Log);
}

/// A CA and a server certificate for 127.0.0.1 it signed.
struct TestTls {
    ca_pem: String,
    server: Arc<tokio_rustls::rustls::ServerConfig>,
}

impl TestTls {
    fn new() -> Self {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
        use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_key = KeyPair::generate().unwrap();
        let ca_cert = ca.self_signed(&ca_key).unwrap();
        let issuer = Issuer::new(ca, ca_key);
        let leaf_key = KeyPair::generate().unwrap();
        let leaf = CertificateParams::new(vec!["127.0.0.1".to_string()])
            .unwrap()
            .signed_by(&leaf_key, &issuer)
            .unwrap();
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let server = tokio_rustls::rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![leaf.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
            )
            .unwrap();
        TestTls {
            ca_pem: ca_cert.pem(),
            server: Arc::new(server),
        }
    }
}

#[derive(Debug)]
struct Seen {
    connection: usize,
    head: String,
    body: String,
}

/// An HTTPS server that answers 404 to paths ending in /gone, else 201, and
/// keeps each request it reads.
async fn serve(tls: &TestTls) -> (u16, Arc<Mutex<Vec<Seen>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let acceptor = tokio_rustls::TlsAcceptor::from(tls.server.clone());
    let kept = seen.clone();
    tokio::spawn(async move {
        let mut connection = 0;
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            connection += 1;
            let (acceptor, kept) = (acceptor.clone(), kept.clone());
            tokio::spawn(async move {
                let mut stream = acceptor.accept(tcp).await.unwrap();
                let mut buf = Vec::new();
                loop {
                    let end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        let mut chunk = [0u8; 4096];
                        let n = stream.read(&mut chunk).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    };
                    let head = String::from_utf8(buf[..end].to_vec()).unwrap();
                    let length: usize = head
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(str::to_string)
                        })
                        .map_or(0, |v| v.trim().parse().unwrap());
                    while buf.len() < end + length {
                        let mut chunk = [0u8; 4096];
                        let n = stream.read(&mut chunk).await.unwrap();
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    let body = String::from_utf8(buf[end..end + length].to_vec()).unwrap();
                    buf.drain(..end + length);
                    let gone = head.lines().next().unwrap().contains("/gone ");
                    kept.lock().unwrap().push(Seen { connection, head, body });
                    let answer: &[u8] = if gone {
                        b"HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
                    } else {
                        b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
                    };
                    stream.write_all(answer).await.unwrap();
                }
            });
        }
    });
    (port, seen)
}

#[tokio::test]
async fn the_client_writes_events_over_tls_with_the_token() {
    let tls = TestTls::new();
    let (port, seen) = serve(&tls).await;
    let dir = sa_dir(true, Some(&tls.ca_pem));
    let cluster = InCluster::from_env(dir.path(), Some("127.0.0.1".into()), Some(port.to_string()))
        .unwrap()
        .unwrap();
    let client = Client::new(cluster);

    client.create("apps", br#"{"kind":"Event"}"#.to_vec()).await.unwrap();
    // A rotated token is used on the next request.
    std::fs::write(dir.path().join("token"), "tok-2\n").unwrap();
    client
        .patch("default", "pvc-1.abc", br#"{"count":2}"#.to_vec())
        .await
        .unwrap();
    assert_eq!(
        client.patch("default", "gone", b"{}".to_vec()).await,
        Err(ApiError::NotFound)
    );

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3, "{seen:?}");
    let lower = |s: &Seen| s.head.to_ascii_lowercase();
    assert!(
        seen[0]
            .head
            .starts_with("POST /api/v1/namespaces/apps/events HTTP/1.1\r\n"),
        "{}",
        seen[0].head
    );
    assert!(
        lower(&seen[0]).contains("authorization: bearer tok-1\r\n"),
        "{}",
        seen[0].head
    );
    assert!(lower(&seen[0]).contains("content-type: application/json\r\n"));
    assert!(lower(&seen[0]).contains(&format!("host: 127.0.0.1:{port}\r\n")));
    assert_eq!(seen[0].body, r#"{"kind":"Event"}"#);
    assert!(
        seen[1]
            .head
            .starts_with("PATCH /api/v1/namespaces/default/events/pvc-1.abc HTTP/1.1\r\n")
    );
    assert!(lower(&seen[1]).contains("authorization: bearer tok-2\r\n"));
    assert!(lower(&seen[1]).contains("content-type: application/strategic-merge-patch+json\r\n"));
    assert_eq!(seen[1].body, r#"{"count":2}"#);
    // One connection for all three.
    assert!(seen.iter().all(|s| s.connection == 1), "{seen:?}");
}

#[tokio::test]
async fn the_client_refuses_a_server_the_ca_did_not_sign() {
    let (tls, other) = (TestTls::new(), TestTls::new());
    let (port, seen) = serve(&tls).await;
    let dir = sa_dir(true, Some(&other.ca_pem));
    let client = Client::new(
        InCluster::from_env(dir.path(), Some("127.0.0.1".into()), Some(port.to_string()))
            .unwrap()
            .unwrap(),
    );
    let err = client.create("apps", b"{}".to_vec()).await.unwrap_err();
    assert!(
        matches!(err, ApiError::Transport(ref e) if e.contains("TLS")),
        "{err:?}"
    );
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn every_op_names_its_event() {
    // Patch and its fallback create refer to the same Event.
    let mut correlator = kube_events::Correlator::new(source());
    let object = pv("pvc-1");
    let t0 = Instant::now();
    let first = pending(&object, REASON_MOUNT_FAILED, "m", at(1_790_000_000, 0), t0);
    let w = correlator.plan(&first).unwrap();
    correlator.record(&first, &w);
    let again = pending(&object, REASON_MOUNT_FAILED, "m", at(1_790_000_001, 0), t0);
    let w2 = correlator.plan(&again).unwrap();
    assert_eq!(w2.name, w.name);
    match w2.op {
        Op::Patch { recreate, .. } => assert_eq!(recreate["metadata"]["name"], w.name.as_str()),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_correlator_remembers_a_bounded_number_of_events() {
    let mut correlator = kube_events::Correlator::new(source());
    let t0 = Instant::now();
    let w0 = at(1_790_000_000, 0);
    // A millisecond apart: all well inside the aggregation window.
    let event = |i: usize, millis: u64| {
        pending(
            &pv(&format!("pvc-{i}")),
            REASON_MOUNT_FAILED,
            "m",
            w0,
            t0 + Duration::from_millis(millis),
        )
    };
    for i in 0..=kube_events::CACHE_ENTRIES {
        let e = event(i, i as u64);
        let w = correlator.plan(&e).unwrap();
        correlator.record(&e, &w);
    }
    // The newest is still remembered; the oldest made room for it.
    let later = kube_events::CACHE_ENTRIES as u64 + 1;
    let newest = event(kube_events::CACHE_ENTRIES, later);
    assert!(matches!(correlator.plan(&newest).unwrap().op, Op::Patch { .. }));
    let oldest = event(0, later);
    assert!(matches!(correlator.plan(&oldest).unwrap().op, Op::Create(_)));
}
