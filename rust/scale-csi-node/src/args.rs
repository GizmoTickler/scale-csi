//! Command-line flags, spelled and parsed the way Go's `flag` package does for
//! the Go binary (`cmd/scale-csi/main.go`), so the chart can start either one
//! with the same arguments: `-name=value`, `--name=value` or `-name value`.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub config: PathBuf,
    pub endpoint: String,
    pub node_id: String,
    pub driver_name: String,
    pub mode: String,
    pub health_port: u16,
    /// klog's `-v`.
    pub verbosity: u8,
    pub version: bool,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            config: PathBuf::new(),
            endpoint: "unix:///csi/csi.sock".into(),
            node_id: String::new(),
            driver_name: crate::config::DEFAULT_DRIVER_NAME.into(),
            mode: "all".into(),
            health_port: 9809,
            verbosity: 0,
            version: false,
        }
    }
}

/// klog flags the Go binary accepts that change nothing here.
const IGNORED: &[&str] = &[
    "startup-connect-timeout",
    "logtostderr",
    "alsologtostderr",
    "stderrthreshold",
    "vmodule",
    "log_dir",
    "log_file",
    "log_file_max_size",
    "one_output",
    "skip_headers",
    "skip_log_headers",
    "add_dir_header",
    "log_backtrace_at",
];
const BOOLS: &[&str] = &[
    "version",
    "logtostderr",
    "alsologtostderr",
    "one_output",
    "skip_headers",
    "skip_log_headers",
    "add_dir_header",
];

pub fn parse(argv: impl IntoIterator<Item = String>) -> Result<Args> {
    let mut args = Args::default();
    let mut it = argv.into_iter();
    while let Some(arg) = it.next() {
        if arg == "--" {
            break;
        }
        let Some(flag) = arg.strip_prefix("--").or_else(|| arg.strip_prefix('-')) else {
            bail!("unexpected argument {arg:?}");
        };
        let (name, inline) = match flag.split_once('=') {
            Some((n, v)) => (n.to_string(), Some(v.to_string())),
            None => (flag.to_string(), None),
        };
        let value = match inline {
            Some(v) => v,
            None if BOOLS.contains(&name.as_str()) => "true".into(),
            None => it.next().with_context(|| format!("flag needs an argument: -{name}"))?,
        };
        let parse_bool = |v: &str| match v {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "f" | "F" | "false" | "FALSE" | "False" => Ok(false),
            _ => bail!("invalid boolean value {v:?} for -{name}"),
        };
        match name.as_str() {
            "config" => args.config = PathBuf::from(value),
            "endpoint" => args.endpoint = value,
            "node-id" => args.node_id = value,
            "driver-name" => args.driver_name = value,
            "mode" => args.mode = value,
            "health-port" => args.health_port = value.parse().with_context(|| format!("-health-port {value:?}"))?,
            "v" => args.verbosity = value.parse().with_context(|| format!("-v {value:?}"))?,
            "version" => args.version = parse_bool(&value)?,
            n if IGNORED.contains(&n) => {
                if BOOLS.contains(&n) {
                    parse_bool(&value)?;
                }
            }
            _ => bail!("flag provided but not defined: -{name}"),
        }
    }
    Ok(args)
}

/// The socket path of a `unix://` (or `unix:`) endpoint.
pub fn socket_path(endpoint: &str) -> Result<PathBuf> {
    let path = endpoint
        .strip_prefix("unix://")
        .or_else(|| endpoint.strip_prefix("unix:"))
        .with_context(|| format!("endpoint {endpoint:?} is not a unix socket"))?;
    if path.is_empty() {
        bail!("endpoint {endpoint:?} has no path");
    }
    Ok(PathBuf::from(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(argv: &[&str]) -> Result<Args> {
        parse(argv.iter().map(|s| s.to_string()))
    }

    #[test]
    fn the_charts_arguments() {
        let a = p(&[
            "-endpoint=unix:///csi/csi.sock",
            "-node-id=k8s-0",
            "-driver-name=csi.scale.io",
            "-config=/etc/scale-csi/config.yaml",
            "-mode=node",
            "-health-port=9809",
            "-v=2",
        ])
        .unwrap();
        assert_eq!(a.endpoint, "unix:///csi/csi.sock");
        assert_eq!(a.node_id, "k8s-0");
        assert_eq!(a.config, PathBuf::from("/etc/scale-csi/config.yaml"));
        assert_eq!((a.mode.as_str(), a.health_port, a.verbosity), ("node", 9809, 2));
    }

    #[test]
    fn go_flag_spellings() {
        let a = p(&[
            "--mode",
            "node",
            "-config",
            "/c.yaml",
            "--v=4",
            "-version",
            "-logtostderr",
            "-startup-connect-timeout=5m",
        ])
        .unwrap();
        assert_eq!((a.mode.as_str(), a.verbosity, a.version), ("node", 4, true));
        assert!(p(&["-bogus=1"]).is_err());
        assert!(p(&["-config"]).is_err());
        assert!(p(&["-health-port=x"]).is_err());
        assert!(p(&["stray"]).is_err());
    }

    #[test]
    fn endpoints() {
        assert_eq!(
            socket_path("unix:///csi/csi.sock").unwrap(),
            PathBuf::from("/csi/csi.sock")
        );
        assert_eq!(
            socket_path("unix:/csi/csi.sock").unwrap(),
            PathBuf::from("/csi/csi.sock")
        );
        assert!(socket_path("tcp://0.0.0.0:10000").is_err());
        assert!(socket_path("unix://").is_err());
    }
}
