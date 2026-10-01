//! The node id against the Go encoder's output: both must be byte-identical.
//! The vectors are generated and checked on the Go side by
//! TestNodeIdentityVectors (pkg/driver/node_identity_vectors_test.go).

use std::net::IpAddr;

use scale_csi_node::node_id::{self, NodeIdentity, Protocols};
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    encode: Vec<EncodeVector>,
    parse: Vec<ParseVector>,
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
