//! The health and metrics endpoints on `-health-port`, as the Go node serves
//! them (`pkg/driver/health.go`): `/healthz` and `/livez` always 200 `OK`;
//! `/readyz` 200 `OK` once serving, else 503; `/health` a JSON summary;
//! `/metrics` Prometheus text. The chart's startup and readiness probes use
//! `/readyz`. A plain HTTP/1.1 responder: probes and scrapers send one GET.

use std::sync::Arc;
use std::time::Duration;

use log::debug;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::service::State;

const MAX_REQUEST: usize = 8192;

/// Binds before the CSI socket is served, so a port clash fails startup.
pub async fn bind(port: u16) -> std::io::Result<TcpListener> {
    TcpListener::bind(("0.0.0.0", port)).await
}

pub async fn serve(listener: TcpListener, state: Arc<State>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, &state).await {
                debug!("health request: {e}");
            }
        });
    }
}

async fn handle(mut stream: TcpStream, state: &State) -> std::io::Result<()> {
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 1024];
    let read = async {
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < MAX_REQUEST {
            let n = stream.read(&mut chunk).await?;
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        Ok::<_, std::io::Error>(())
    };
    if tokio::time::timeout(Duration::from_secs(5), read).await.is_err() {
        return Ok(());
    }
    let line = String::from_utf8_lossy(&buf);
    let path = line.split_whitespace().nth(1).unwrap_or("/");
    let path = path.split('?').next().unwrap_or(path);
    let (status, content_type, body) = respond(path, state);
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.shutdown().await
}

fn respond(path: &str, state: &State) -> (&'static str, &'static str, String) {
    match path {
        "/healthz" | "/livez" => ("200 OK", "text/plain", "OK".into()),
        "/readyz" if state.is_ready() => ("200 OK", "text/plain", "OK".into()),
        "/readyz" => ("503 Service Unavailable", "text/plain", "Not Ready".into()),
        "/health" => {
            let ready = state.is_ready();
            let body = format!(
                "{{\"ready\":{ready},\"truenas_connected\":false,\"controller_running\":false,\"node_running\":{ready}}}\n"
            );
            (
                if ready { "200 OK" } else { "503 Service Unavailable" },
                "application/json",
                body,
            )
        }
        "/metrics" => ("200 OK", "text/plain; version=0.0.4", state.metrics.render()),
        _ => ("404 Not Found", "text/plain", "404 page not found\n".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;

    async fn get(port: u16, path: &str) -> String {
        let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn probes() {
        let listener = bind(0).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let state = Arc::new(State::new(
            config::parse("nvmeof: {}", |_| None).unwrap(),
            "csi.scale.io".into(),
            "node".into(),
            "n".into(),
            Default::default(),
        ));
        tokio::spawn(serve(listener, state.clone()));
        assert!(get(port, "/livez").await.starts_with("HTTP/1.1 200 OK"));
        let not_ready = get(port, "/readyz").await;
        assert!(
            not_ready.starts_with("HTTP/1.1 503") && not_ready.ends_with("Not Ready"),
            "{not_ready}"
        );
        state.set_ready(true);
        let ready = get(port, "/readyz?verbose").await;
        assert!(
            ready.starts_with("HTTP/1.1 200 OK") && ready.ends_with("\r\n\r\nOK"),
            "{ready}"
        );
        assert!(
            get(port, "/metrics")
                .await
                .contains("scale_csi_truenas_connection_status 0")
        );
        assert!(get(port, "/nope").await.starts_with("HTTP/1.1 404"));
    }
}
