//! The CSI node id: `sc1.` + base64url (no padding) of a version byte and TLV
//! fields `[type u8][len u8][value]`. It must be byte-identical to the Go
//! driver's (`pkg/driver/node_identity.go`): kubelet stores it in the CSINode,
//! the controller parses it for fencing, and a node moving between the Go and
//! the Rust agent must keep it. `tests/node_id_vectors.rs` checks this module
//! against `pkg/driver/testdata/node_identity_vectors.json`, which the Go tests
//! generate and check against the Go code.

use std::cmp::Ordering;
use std::net::IpAddr;

use base64::Engine as _;
use base64::engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig};

pub const PREFIX: &str = "sc1.";
const VERSION: u8 = 1;
/// CSI's limit on node_id.
const MAX_NODE_ID_BYTES: usize = 256;
const FIELD_NAME: u8 = 1;
const FIELD_NQN: u8 = 2;
const FIELD_IQN: u8 = 3;
const FIELD_IPV4: u8 = 4;
const FIELD_IPV6: u8 = 5;
/// Set when the node reported the reserved deny-all IQN as its own; the IQN
/// itself is then blanked, and the controller refuses iSCSI publication.
const FIELD_ISCSI_SENTINEL_REPORTED: u8 = 6;

/// A reserved backend fencing value, never a node's identity.
pub const ISCSI_DENY_ALL_SENTINEL_IQN: &str = "iqn.2016-09.io.scale-csi:deny-all-fence";

/// Go's `base64.RawURLEncoding`: no padding (a `=` is an error) and, unlike
/// the base64 crate's default, non-zero trailing bits are accepted.
const RAW_URL: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone)
        .with_decode_allow_trailing_bits(true),
);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NodeIdentity {
    /// The Kubernetes node name.
    pub name: String,
    pub nvme_nqn: String,
    pub iscsi_iqn: String,
    pub ips: Vec<IpAddr>,
    /// Parsed from a plain node name, not an `sc1.` id.
    pub legacy: bool,
    pub iscsi_reported_sentinel: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub nfs: bool,
    pub iscsi: bool,
    pub nvmeof: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NameRequired,
    ReservedIqn(String),
    FieldTooLong(u8),
    TooLong,
    Empty,
    Invalid(&'static str),
    UnsupportedVersion(u8),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NameRequired => write!(f, "node name is required"),
            Error::ReservedIqn(name) => write!(
                f,
                "node {name} reported the reserved iSCSI fencing identity {ISCSI_DENY_ALL_SENTINEL_IQN:?} as its own initiator IQN"
            ),
            Error::FieldTooLong(t) => write!(f, "node identity field {t} is too long"),
            Error::TooLong => write!(
                f,
                "node name and transport identities exceed CSI's {MAX_NODE_ID_BYTES}-byte node_id limit"
            ),
            Error::Empty => write!(f, "node ID is empty"),
            Error::Invalid(what) => write!(f, "{what}"),
            Error::UnsupportedVersion(v) => write!(f, "unsupported node identity version {v}"),
        }
    }
}

impl std::error::Error for Error {}

fn encoded_len(raw: usize) -> usize {
    PREFIX.len() + (raw * 4).div_ceil(3)
}

fn append_field(raw: &mut Vec<u8>, field: u8, value: &[u8]) -> Result<(), Error> {
    if value.is_empty() {
        return Ok(());
    }
    let len = u8::try_from(value.len()).map_err(|_| Error::FieldTooLong(field))?;
    raw.push(field);
    raw.push(len);
    raw.extend_from_slice(value);
    Ok(())
}

/// Packs the name and the transport identities, then as many canonical IPs as
/// fit in 256 bytes. The name and the transport identities are exact
/// authorization identities and are never cut: if they alone do not fit, it is
/// an error.
pub fn encode(identity: &NodeIdentity) -> Result<String, Error> {
    let name = identity.name.trim();
    let nqn = identity.nvme_nqn.trim();
    let iqn = identity.iscsi_iqn.trim();
    if name.is_empty() {
        return Err(Error::NameRequired);
    }
    if iqn == ISCSI_DENY_ALL_SENTINEL_IQN {
        return Err(Error::ReservedIqn(name.to_string()));
    }
    let mut raw = vec![VERSION];
    append_field(&mut raw, FIELD_NAME, name.as_bytes())?;
    append_field(&mut raw, FIELD_NQN, nqn.as_bytes())?;
    append_field(&mut raw, FIELD_IQN, iqn.as_bytes())?;
    if identity.iscsi_reported_sentinel {
        append_field(&mut raw, FIELD_ISCSI_SENTINEL_REPORTED, &[1])?;
    }
    if encoded_len(raw.len()) > MAX_NODE_ID_BYTES {
        return Err(Error::TooLong);
    }
    for ip in canonical_ips(&identity.ips) {
        let mut candidate = raw.clone();
        match ip {
            IpAddr::V4(v4) => append_field(&mut candidate, FIELD_IPV4, &v4.octets())?,
            IpAddr::V6(v6) => append_field(&mut candidate, FIELD_IPV6, &v6.octets())?,
        }
        if encoded_len(candidate.len()) > MAX_NODE_ID_BYTES {
            break;
        }
        raw = candidate;
    }
    Ok(format!("{PREFIX}{}", RAW_URL.encode(raw)))
}

/// Tolerant in both upgrade directions: a value without the prefix is an older,
/// plain node name; unknown field types are skipped. A malformed envelope is an
/// error, never partly used. One difference from Go: a field that is not UTF-8
/// is an error here (Go keeps the bytes); the encoders only write UTF-8.
pub fn parse(node_id: &str) -> Result<NodeIdentity, Error> {
    if node_id.is_empty() {
        return Err(Error::Empty);
    }
    let Some(encoded) = node_id.strip_prefix(PREFIX) else {
        return Ok(NodeIdentity {
            name: node_id.to_string(),
            legacy: true,
            ..Default::default()
        });
    };
    // Go's decoder skips line breaks.
    let encoded: String = encoded.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    let raw = RAW_URL
        .decode(encoded)
        .map_err(|_| Error::Invalid("invalid encoded node ID"))?;
    if raw.is_empty() {
        return Err(Error::Invalid("invalid encoded node ID"));
    }
    if raw[0] != VERSION {
        return Err(Error::UnsupportedVersion(raw[0]));
    }
    let text = |value: &[u8]| {
        String::from_utf8(value.to_vec()).map_err(|_| Error::Invalid("node identity field is not UTF-8"))
    };
    let mut identity = NodeIdentity::default();
    let mut offset = 1;
    while offset < raw.len() {
        if raw.len() - offset < 2 {
            return Err(Error::Invalid("truncated node identity field header"));
        }
        let (field, len) = (raw[offset], raw[offset + 1] as usize);
        offset += 2;
        if len == 0 || raw.len() - offset < len {
            return Err(Error::Invalid("invalid node identity field length"));
        }
        let value = &raw[offset..offset + len];
        offset += len;
        match field {
            FIELD_NAME => identity.name = text(value)?,
            FIELD_NQN => identity.nvme_nqn = text(value)?,
            FIELD_IQN => identity.iscsi_iqn = text(value)?,
            FIELD_IPV4 => {
                let octets: [u8; 4] = value
                    .try_into()
                    .map_err(|_| Error::Invalid("invalid IPv4 identity length"))?;
                identity.ips.push(IpAddr::from(octets));
            }
            FIELD_IPV6 => {
                let octets: [u8; 16] = value
                    .try_into()
                    .map_err(|_| Error::Invalid("invalid IPv6 identity length"))?;
                identity.ips.push(IpAddr::from(octets));
            }
            FIELD_ISCSI_SENTINEL_REPORTED => identity.iscsi_reported_sentinel = true,
            _ => {} // reserved for future identity types
        }
    }
    if identity.name.is_empty() {
        return Err(Error::Invalid("encoded node ID has no node name"));
    }
    identity.ips = canonical_ips(&identity.ips);
    Ok(identity)
}

/// Unicast addresses only (no unspecified, loopback, multicast or link-local),
/// IPv4-mapped IPv6 as IPv4, de-duplicated, sorted by their text, which is the
/// order Go's encoder packs them in (so "10.0.0.2" < "100.64.0.1" < "9.9.9.9").
pub fn canonical_ips(ips: &[IpAddr]) -> Vec<IpAddr> {
    let mut out: Vec<(String, IpAddr)> = Vec::with_capacity(ips.len());
    for ip in ips {
        let ip = match *ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
            v4 => v4,
        };
        let unicast = match ip {
            IpAddr::V4(v4) => !(v4.is_unspecified() || v4.is_loopback() || v4.is_multicast() || v4.is_link_local()),
            IpAddr::V6(v6) => {
                !(v6.is_unspecified() || v6.is_loopback() || v6.is_multicast() || v6.is_unicast_link_local())
            }
        };
        if unicast {
            out.push((ip.to_string(), ip));
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out.dedup_by(|a, b| a.0.cmp(&b.0) == Ordering::Equal);
    out.into_iter().map(|(_, ip)| ip).collect()
}

/// Drops the identities of protocols this install does not serve before the
/// size limit is applied: IPs are NFS's, the IQN and the sentinel flag
/// iSCSI's, the NQN NVMe-oF's.
pub fn for_enabled_protocols(mut identity: NodeIdentity, protocols: Protocols) -> NodeIdentity {
    if !protocols.nfs {
        identity.ips.clear();
    }
    if !protocols.iscsi {
        identity.iscsi_iqn.clear();
        identity.iscsi_reported_sentinel = false;
    }
    if !protocols.nvmeof {
        identity.nvme_nqn.clear();
    }
    identity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_keeps_the_legacy_form() {
        let identity = NodeIdentity {
            name: "k8s-0".into(),
            nvme_nqn: "nqn.2014-08.org.nvmexpress:uuid:x".into(),
            ips: vec!["192.0.2.1".parse().unwrap()],
            ..Default::default()
        };
        let id = encode(&identity).unwrap();
        assert!(id.starts_with(PREFIX));
        assert_eq!(parse(&id).unwrap(), identity);
        let legacy = parse("k8s-0").unwrap();
        assert!(legacy.legacy);
        assert_eq!(legacy.name, "k8s-0");
    }
}
