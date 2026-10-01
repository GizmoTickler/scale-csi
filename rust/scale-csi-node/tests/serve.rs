//! The binary end to end: started the way the chart starts the Go node, it
//! serves Identity and Node on the socket, answers the health probes, refuses
//! protocols it does not serve, and stops on SIGTERM.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use hyper_util::rt::TokioIo;
use scale_csi_node::csi;
use scale_csi_node::csi::identity_client::IdentityClient;
use scale_csi_node::csi::node_client::NodeClient;
use scale_csi_node::node_id;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint};

struct Agent {
    child: Child,
    socket: PathBuf,
    _dir: tempfile::TempDir,
}

impl Drop for Agent {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(config: &str, health_port: u16) -> Agent {
    start_with(config, health_port, 2)
}

fn start_with(config: &str, health_port: u16, verbosity: u8) -> Agent {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("config.yaml");
    std::fs::write(&config_path, config).unwrap();
    let socket = dir.path().join("csi.sock");
    let child = Command::new(env!("CARGO_BIN_EXE_scale-csi-node"))
        .args([
            format!("-endpoint=unix://{}", socket.display()),
            "-node-id=test-node".into(),
            "-driver-name=csi.scale.io".into(),
            format!("-config={}", config_path.display()),
            "-mode=node".into(),
            format!("-health-port={health_port}"),
            format!("-v={verbosity}"),
        ])
        .env("NODE_IP", "192.0.2.10")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    Agent {
        child,
        socket,
        _dir: dir,
    }
}

async fn channel(socket: &Path) -> Channel {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "the agent never created its socket");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let socket = socket.to_path_buf();
    Endpoint::try_from("http://[::]:50051")
        .unwrap()
        .connect_with_connector(tower::service_fn(move |_| {
            let socket = socket.clone();
            async move { UnixStream::connect(socket).await.map(TokioIo::new) }
        }))
        .await
        .unwrap()
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn serves_identity_and_node_info() {
    let port = free_port();
    let agent = start(
        "driver: csi.scale.io\nnvmeof:\n  enabled: true\n  dataPath: ublk\n  ublk: {maxVolumesPerNode: 64}\n",
        port,
    );
    let ch = channel(&agent.socket).await;

    let info = IdentityClient::new(ch.clone())
        .get_plugin_info(csi::GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, "csi.scale.io");
    let probe = IdentityClient::new(ch.clone())
        .probe(csi::ProbeRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(probe.ready, Some(true));

    let mut node = NodeClient::new(ch);
    let got = node
        .node_get_info(csi::NodeGetInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    let identity = node_id::parse(&got.node_id).unwrap();
    assert_eq!(identity.name, "test-node");
    assert!(identity.ips.is_empty(), "NFS is off: its IPs are pruned from the id");
    assert_eq!(
        got.max_volumes_per_node, 64,
        "ublk default data path with zero copy advertises the daemon budget"
    );

    let caps = node
        .node_get_capabilities(csi::NodeGetCapabilitiesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(caps.capabilities.len(), 4);

    let stage = node
        .node_stage_volume(csi::NodeStageVolumeRequest {
            volume_id: "v".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(stage.code(), tonic::Code::InvalidArgument, "{stage:?}");
    assert_eq!(stage.message(), "staging target path is required");
    let stats = node
        .node_get_volume_stats(csi::NodeGetVolumeStatsRequest {
            volume_id: "v".into(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(stats.code(), tonic::Code::InvalidArgument, "{stats:?}");
    let health = node
        .node_get_volume_health(csi::NodeGetVolumeHealthRequest {
            volume_id: "v".into(),
            ..Default::default()
        })
        .await;
    assert_eq!(
        health.unwrap_err().code(),
        tonic::Code::Unimplemented,
        "the alpha health RPCs are not served, as in the Go node"
    );

    let ready = reqwest_get(port, "/readyz").await;
    assert!(ready.starts_with("HTTP/1.1 200"), "{ready}");
    let metrics = reqwest_get(port, "/metrics").await;
    for line in [
        r#"scale_csi_operations_total{code="OK",operation="/csi.v1.Node/NodeGetInfo",status="success"} 1"#,
        r#"scale_csi_operations_total{code="InvalidArgument",operation="/csi.v1.Node/NodeStageVolume",status="error"} 1"#,
        r#"scale_csi_operations_total{code="InvalidArgument",operation="/csi.v1.Node/NodeGetVolumeStats",status="error"} 1"#,
        r#"scale_csi_operations_total{code="Unimplemented",operation="/csi.v1.Node/NodeGetVolumeHealth",status="error"} 1"#,
        r#"scale_csi_operations_duration_seconds_count{operation="/csi.v1.Identity/Probe"} 1"#,
    ] {
        assert!(metrics.contains(line), "missing {line}\n{metrics}");
    }
}

#[tokio::test]
async fn refuses_protocols_it_does_not_serve() {
    for config in [
        "nfs: {}\nnvmeof: {}\n",
        "iscsi:\n  targetPortal: 192.0.2.1:3260\n",
        // The kernel initiator is the NVMe-oF default data path.
        "nvmeof:\n  ublk:\n    enabled: true\n",
    ] {
        let mut agent = start(config, 0);
        let status = agent.child.wait().unwrap();
        assert!(!status.success(), "{config:?} was accepted");
        let mut err = String::new();
        std::io::Read::read_to_string(agent.child.stderr.as_mut().unwrap(), &mut err).unwrap();
        assert!(err.contains("does not serve yet"), "{err}");
    }
}

#[tokio::test]
async fn stops_on_sigterm() {
    let mut agent = start("nvmeof:\n  dataPath: ublk\n", 0);
    let _ = channel(&agent.socket).await;
    // SAFETY: signalling our own child.
    unsafe { libc::kill(agent.child.id() as i32, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = agent.child.try_wait().unwrap() {
            assert!(status.success(), "{status:?}");
            break;
        }
        assert!(Instant::now() < deadline, "no exit after SIGTERM");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn reqwest_get(port: u16, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

/// Even at trace verbosity, where request bodies are logged, a request's
/// secrets never reach the log.
#[tokio::test]
async fn request_secrets_never_reach_the_log() {
    let mut agent = start_with("nvmeof:\n  dataPath: ublk\n", 0, 5);
    let mut node = NodeClient::new(channel(&agent.socket).await);
    let mut secrets = std::collections::HashMap::new();
    secrets.insert(
        "node.session.auth.password".to_string(),
        "hunter2-not-logged".to_string(),
    );
    let err = node
        .node_stage_volume(csi::NodeStageVolumeRequest {
            volume_id: "pvc-secret".into(),
            staging_target_path: "/nonexistent/stage".into(),
            secrets,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    drop(node);
    // Drain the log while the agent stops, so a full pipe cannot block it.
    let mut stderr = agent.child.stderr.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut log = String::new();
        let _ = std::io::Read::read_to_string(&mut stderr, &mut log);
        log
    });
    // SAFETY: signalling our own child.
    unsafe { libc::kill(agent.child.id() as i32, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(10);
    while agent.child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = agent.child.kill();
            let _ = agent.child.wait();
            panic!("no exit after SIGTERM: {}", reader.join().unwrap());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let log = reader.join().unwrap();
    assert!(
        log.contains("[req-1] /csi.v1.Node/NodeStageVolume volumeID=pvc-secret"),
        "{log}"
    );
    assert!(
        log.contains("request: NodeStageVolumeRequest"),
        "the body is logged at trace: {log}"
    );
    assert!(log.contains("failed after"), "{log}");
    assert!(!log.contains("hunter2"), "a secret reached the log: {log}");
}
