use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use log::{info, warn};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::UnixListenerStream;

use scale_csi_node::csi::{identity_server::IdentityServer, node_server::NodeServer};
use scale_csi_node::metrics::{Metrics, OperationsLayer};
use scale_csi_node::node_id::{self, Protocols};
use scale_csi_node::service::{IdentityService, NodeService, State};
use scale_csi_node::session_registry::SessionRegistry;
use scale_csi_node::{args, config, discovery, health, kube_api, kube_events};

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("scale-csi-node: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args = args::parse(std::env::args().skip(1))?;
    if args.version {
        println!("scale-csi-node {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    init_logging(args.verbosity);
    if args.mode != "node" {
        bail!("-mode={}: this binary is the node service only (-mode=node)", args.mode);
    }
    if args.config.as_os_str().is_empty() {
        bail!("-config is required");
    }
    let config = config::load(&args.config)?;
    // The flag wins only when it is not the default; an empty config value takes it.
    let driver_name = if args.driver_name != config::DEFAULT_DRIVER_NAME || config.driver.is_empty() {
        args.driver_name.clone()
    } else {
        config.driver.clone()
    };
    let node_name = if !args.node_id.is_empty() {
        args.node_id.clone()
    } else if let Some(id) = std::env::var("NODE_ID").ok().filter(|s| !s.is_empty()) {
        id
    } else {
        hostname().context("-node-id is required for node mode")?
    };

    let identity_networks =
        discovery::parse_identity_networks(&config.nfs.node_identity_networks).map_err(anyhow::Error::msg)?;
    let discovered = discovery::discover(
        &node_name,
        &discovery::Sources::host(config.command_timeouts.nvme()),
        &identity_networks,
    )
    .await;
    let protocols = Protocols {
        nfs: config.nfs_enabled,
        iscsi: config.iscsi_enabled,
        nvmeof: config.nvmeof.enabled,
    };
    let identity = node_id::for_enabled_protocols(discovered, protocols);
    let node_id = node_id::encode(&identity).context("encode this node's identity")?;
    for ip in discovery::dropped_ips(&identity, &identity_networks, &node_id) {
        warn!(
            "nfs.nodeIdentityNetworks address {ip} does not fit in CSI's 256-byte node_id and is left out: the controller cannot grant it, so NFS mounts from it will be refused"
        );
    }
    info!(
        "scale-csi-node {} driver={driver_name} node={node_name} node_id={node_id}",
        env!("CARGO_PKG_VERSION")
    );

    let socket = args::socket_path(&args.endpoint)?;
    match std::fs::remove_file(&socket) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("remove stale socket {}", socket.display())),
    }
    let listener = UnixListener::bind(&socket).with_context(|| format!("listen on {}", socket.display()))?;
    // node-driver-registrar connects from another container of the pod.
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o660))?;

    let metrics = Arc::new(Metrics::new());
    // Events as the Go node records them: from the driver, on this host.
    let source = kube_events::Source {
        component: driver_name.clone(),
        host: hostname().unwrap_or_else(|_| "unknown".into()),
    };
    let (events, sink) = kube_events::sink(
        std::path::Path::new(kube_api::SERVICE_ACCOUNT_DIR),
        std::env::var("KUBERNETES_SERVICE_HOST").ok(),
        std::env::var("KUBERNETES_SERVICE_PORT").ok(),
        source,
        metrics.clone(),
    );
    info!("events: {sink:?}");
    let mut state = State::new(config, driver_name, node_name, node_id, metrics.clone());
    state.events = events;
    state.nvme_sessions = match SessionRegistry::beside(&socket) {
        Ok(registry) => Some(registry),
        Err(e) => {
            warn!("no NVMe-oF session registry ({e:#}): session GC will not collect NVMe-oF sessions");
            None
        }
    };
    if let Some(kubelet) = scale_csi_node::session_gc::kubelet_dir_of(&socket) {
        state.host.kubelet_dir = kubelet;
    }
    let state = Arc::new(state);
    let (stop_gc, gc_stop) = tokio::sync::watch::channel(false);
    let gc = tokio::spawn(scale_csi_node::session_gc::run(state.clone(), gc_stop));
    if args.health_port > 0 {
        let health = health::bind(args.health_port)
            .await
            .with_context(|| format!("bind -health-port {}", args.health_port))?;
        tokio::spawn(health::serve(health, state.clone()));
    }

    let shutdown_state = state.clone();
    let shutdown = async move {
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = term.recv() => info!("SIGTERM: stopping"),
            _ = int.recv() => info!("SIGINT: stopping"),
        }
        shutdown_state.set_ready(false);
    };
    state.set_ready(true);
    info!("serving CSI Identity and Node on {}", socket.display());
    tonic::transport::Server::builder()
        .layer(OperationsLayer(metrics))
        .add_service(IdentityServer::new(IdentityService(state.clone())))
        .add_service(NodeServer::new(NodeService(state.clone())))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .context("serve CSI")?;
    // Operations whose RPC the caller already gave up on are still running.
    state.wait_for_operations().await;
    // Session GC finishes the session it is on, then stops.
    let _ = stop_gc.send(true);
    let _ = gc.await;
    Ok(())
}

/// `-v` raises this agent's own log level only, as klog's does for the Go
/// node: libraries (hyper, h2, tonic) stay at info, or a -v=5 debugging session
/// would drown the log in transport frames.
fn init_logging(verbosity: u8) {
    let filter = match verbosity {
        0..=3 => "info",
        4 => "info,scale_csi_node=debug",
        _ => "info,scale_csi_node=trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(filter))
        .format_timestamp_micros()
        .init();
}

fn hostname() -> Result<String> {
    let name = std::fs::read_to_string("/proc/sys/kernel/hostname")?.trim().to_string();
    if name.is_empty() {
        bail!("empty hostname");
    }
    Ok(name)
}
