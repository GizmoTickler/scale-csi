//! The node id against the Go encoder's output: both must be byte-identical.
//! The vectors are generated and checked on the Go side by
//! TestNodeIdentityVectors (pkg/driver/node_identity_vectors_test.go).

use std::net::IpAddr;

use scale_csi_node::discovery;
use scale_csi_node::node_id::{self, NodeIdentity, Protocols};
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    encode: Vec<EncodeVector>,
    parse: Vec<ParseVector>,
    identity_ips: Vec<IdentityIpsVector>,
}

#[derive(Deserialize)]
struct IdentityIpsVector {
    case: String,
    input: IdentityIpsInput,
    #[serde(default)]
    error: bool,
    interfaces_listed: bool,
    ips: Vec<String>,
    #[serde(default)]
    node_id: String,
    dropped: Vec<String>,
}

#[derive(Deserialize)]
struct IdentityIpsInput {
    #[serde(default)]
    nvme_nqn: String,
    #[serde(default)]
    iscsi_iqn: String,
    #[serde(default)]
    node_ip: String,
    #[serde(default)]
    node_ips: String,
    #[serde(default)]
    interfaces: Vec<String>,
    #[serde(default)]
    networks: Vec<String>,
}

#[derive(Deserialize)]
struct EncodeVector {
    case: String,
    input: Input,
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    error: bool,
}

#[derive(Deserialize)]
struct Input {
    name: String,
    #[serde(default)]
    nvme_nqn: String,
    #[serde(default)]
    iscsi_iqn: String,
    #[serde(default)]
    ips: Vec<String>,
    #[serde(default)]
    iscsi_reported_sentinel: bool,
    protocols: Option<ProtocolsInput>,
}

#[derive(Deserialize)]
struct ProtocolsInput {
    nfs: bool,
    iscsi: bool,
    nvmeof: bool,
}

#[derive(Deserialize)]
struct ParseVector {
    case: String,
    node_id: String,
    identity: Option<Identity>,
    #[serde(default)]
    error: bool,
}

#[derive(Deserialize)]
struct Identity {
    name: String,
    nvme_nqn: String,
    iscsi_iqn: String,
    ips: Vec<String>,
    legacy: bool,
    iscsi_reported_sentinel: bool,
}

fn vectors() -> Vectors {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../pkg/driver/testdata/node_identity_vectors.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).expect("read the Go vectors")).expect("parse the Go vectors")
}

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap_or_else(|_| panic!("bad ip {text}"))
}

#[test]
fn encoding_matches_go() {
    let vectors = vectors();
    assert!(vectors.encode.len() >= 20);
    for v in vectors.encode {
        let mut identity = NodeIdentity {
            name: v.input.name,
            nvme_nqn: v.input.nvme_nqn,
            iscsi_iqn: v.input.iscsi_iqn,
            ips: v.input.ips.iter().map(|s| ip(s)).collect(),
            legacy: false,
            iscsi_reported_sentinel: v.input.iscsi_reported_sentinel,
        };
        if let Some(p) = v.input.protocols {
            identity = node_id::for_enabled_protocols(
                identity,
                Protocols {
                    nfs: p.nfs,
                    iscsi: p.iscsi,
                    nvmeof: p.nvmeof,
                },
            );
        }
        match node_id::encode(&identity) {
            Ok(got) => {
                assert!(!v.error, "{}: Go refused it, Rust encoded {got}", v.case);
                assert_eq!(got, v.node_id, "{}", v.case);
            }
            Err(e) => assert!(v.error, "{}: Go encoded {}, Rust refused: {e}", v.case, v.node_id),
        }
    }
}

#[test]
fn parsing_matches_go() {
    let vectors = vectors();
    assert!(vectors.parse.len() >= 15);
    for v in vectors.parse {
        match node_id::parse(&v.node_id) {
            Ok(got) => {
                assert!(!v.error, "{}: Go refused {:?}, Rust parsed {got:?}", v.case, v.node_id);
                let want = v.identity.expect("a parsed vector has an identity");
                assert_eq!(got.name, want.name, "{}", v.case);
                assert_eq!(got.nvme_nqn, want.nvme_nqn, "{}", v.case);
                assert_eq!(got.iscsi_iqn, want.iscsi_iqn, "{}", v.case);
                assert_eq!(got.legacy, want.legacy, "{}", v.case);
                assert_eq!(got.iscsi_reported_sentinel, want.iscsi_reported_sentinel, "{}", v.case);
                let got_ips: Vec<String> = got.ips.iter().map(|ip| ip.to_string()).collect();
                assert_eq!(got_ips, want.ips, "{}", v.case);
            }
            Err(e) => assert!(v.error, "{}: Go parsed {:?}, Rust refused: {e}", v.case, v.node_id),
        }
    }
}

/// nfs.nodeIdentityNetworks: the same validation, the same identity IPs, the
/// same node id and the same dropped addresses as the Go node.
#[test]
fn identity_ips_match_go() {
    let vectors = vectors();
    assert!(vectors.identity_ips.len() >= 25);
    for v in vectors.identity_ips {
        let networks = match discovery::parse_identity_networks(&v.input.networks) {
            Ok(networks) => {
                assert!(!v.error, "{}: Go refused the networks, Rust accepted them", v.case);
                networks
            }
            Err(e) => {
                assert!(v.error, "{}: Go accepted the networks, Rust refused: {e}", v.case);
                continue;
            }
        };
        let mut listed = false;
        let ips = discovery::identity_ips(
            &v.input.node_ip,
            &v.input.node_ips,
            || {
                listed = true;
                v.input.interfaces.iter().map(|s| ip(s)).collect()
            },
            &networks,
        );
        assert_eq!(listed, v.interfaces_listed, "{}: interfaces listed", v.case);
        let got: Vec<String> = ips.iter().map(|ip| ip.to_string()).collect();
        assert_eq!(got, v.ips, "{}", v.case);
        let identity = NodeIdentity {
            name: "k8s-1".into(),
            nvme_nqn: v.input.nvme_nqn,
            iscsi_iqn: v.input.iscsi_iqn,
            ips,
            ..Default::default()
        };
        let node_id = node_id::encode(&identity).expect("encodes");
        assert_eq!(node_id, v.node_id, "{}", v.case);
        let dropped: Vec<String> = discovery::dropped_ips(&identity, &networks, &node_id)
            .iter()
            .map(|ip| ip.to_string())
            .collect();
        assert_eq!(dropped, v.dropped, "{}: dropped", v.case);
    }
}
