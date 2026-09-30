use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use log::info;
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio_stream::wrappers::UnixListenerStream;

use scale_csi_node::csi::{identity_server::IdentityServer, node_server::NodeServer};
use scale_csi_node::node_id::{self, Protocols};
use scale_csi_node::service::{IdentityService, NodeService, State};
use scale_csi_node::{args, config, discovery, health};

/// `commandTimeouts.nvme`'s default, which bounds identity discovery.
const NVME_TIMEOUT: Duration = Duration::from_secs(30);

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
    if config.nfs_enabled || config.iscsi_enabled {
        bail!(
            "this install enables NFS or iSCSI, which the Rust node agent does not serve yet; run the Go node plugin"
        );
    }
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

    let discovered = discovery::discover(&node_name, &discovery::Sources::host(NVME_TIMEOUT)).await;
    let protocols = Protocols {
        nfs: config.nfs_enabled,
        iscsi: config.iscsi_enabled,
        nvmeof: config.nvmeof.enabled,
    };
    let node_id = node_id::encode(&node_id::for_enabled_protocols(discovered, protocols))
        .context("encode this node's identity")?;
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

    let state = Arc::new(State {
        driver_name,
        version: env!("CARGO_PKG_VERSION").to_string(),
        node_id,
        config,
        ready: AtomicBool::new(false),
    });
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
        .add_service(IdentityServer::new(IdentityService(state.clone())))
        .add_service(NodeServer::new(NodeService(state.clone())))
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), shutdown)
        .await
        .context("serve CSI")?;
    Ok(())
}

fn init_logging(verbosity: u8) {
    let level = match verbosity {
        0..=3 => "info",
        4 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(level))
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
