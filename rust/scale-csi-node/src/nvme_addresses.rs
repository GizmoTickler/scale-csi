//! The NVMe-oF portals a volume is reached on, from its publish context
//! (`pkg/driver/node.go` parseNVMeoFMultipathAddresses, nvmeUblkAddresses): the
//! `addresses` hint (a JSON list of bare IPs, IPv6 optionally bracketed) when it
//! is usable, else the single `address`. A malformed hint is discarded whole,
//! never partly used; the caller reports it (NVMePathDegraded) and falls back.

use std::collections::HashMap;
use std::net::IpAddr;

/// `Ok(None)` when there is no hint (absent or an empty list).
pub fn parse_multipath_addresses(context: &HashMap<String, String>) -> Result<Option<Vec<String>>, String> {
    let Some(raw) = context.get("addresses") else {
        return Ok(None);
    };
    // JSON null decodes to no list, as in Go.
    let decoded: Option<Vec<String>> = serde_json::from_str(raw).map_err(|e| format!("decode addresses: {e}"))?;
    let Some(decoded) = decoded.filter(|d| !d.is_empty()) else {
        return Ok(None);
    };
    let mut out: Vec<String> = Vec::with_capacity(decoded.len());
    for raw in &decoded {
        let address =
            normalize_address(raw).map_err(|e| format!("addresses contains invalid transport address {raw:?}: {e}"))?;
        if !out.contains(&address) {
            out.push(address);
        }
    }
    Ok(Some(out))
}

/// A bare IP as given (not re-formatted), brackets removed from IPv6.
pub fn normalize_address(raw: &str) -> Result<String, &'static str> {
    if raw != raw.trim() {
        return Err("leading or trailing whitespace is not allowed");
    }
    if raw.is_empty() {
        return Err("empty address");
    }
    if raw.contains("://") {
        return Err("URI schemes are not allowed");
    }
    if let Some(rest) = raw.strip_prefix('[') {
        if raw.find(']') != Some(raw.len() - 1) {
            return Err("bracketed addresses must not include a port or suffix");
        }
        let inner = &rest[..rest.len() - 1];
        if inner.parse::<IpAddr>().is_err() || !inner.contains(':') {
            return Err("brackets are valid only around an IPv6 address");
        }
        return Ok(inner.to_string());
    }
    if raw.contains(['[', ']', ' ', '\t', '\r', '\n']) {
        return Err("address must be a bare IP");
    }
    if raw.parse::<IpAddr>().is_ok() {
        return Ok(raw.to_string());
    }
    Err("address must be a bare IP without a port")
}

/// Go's net.JoinHostPort: a host containing ':' is bracketed.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The daemon's portal list, and the reason a hint was discarded (for the
/// caller's event), if one was.
pub fn ublk_portals(context: &HashMap<String, String>, address: &str, port: &str) -> (Vec<String>, Option<String>) {
    let (addresses, discarded) = match parse_multipath_addresses(context) {
        Ok(Some(list)) => (list, None),
        Ok(None) => (Vec::new(), None),
        Err(e) => (Vec::new(), Some(e)),
    };
    let addresses = if addresses.is_empty() {
        let host = address.strip_prefix('[').unwrap_or(address);
        vec![host.strip_suffix(']').unwrap_or(host).to_string()]
    } else {
        addresses
    };
    (addresses.iter().map(|a| join_host_port(a, port)).collect(), discarded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(addresses: Option<&str>) -> HashMap<String, String> {
        addresses
            .map(|a| ("addresses".to_string(), a.to_string()))
            .into_iter()
            .collect()
    }

    #[test]
    fn hints() {
        assert_eq!(parse_multipath_addresses(&ctx(None)), Ok(None));
        assert_eq!(parse_multipath_addresses(&ctx(Some("[]"))), Ok(None));
        assert_eq!(parse_multipath_addresses(&ctx(Some("null"))), Ok(None));
        assert_eq!(
            parse_multipath_addresses(&ctx(Some(r#"["192.0.2.1","192.0.2.2","192.0.2.1","[2001:DB8::1]"]"#))),
            Ok(Some(vec!["192.0.2.1".into(), "192.0.2.2".into(), "2001:DB8::1".into()])),
            "de-duplicated in order, IPv6 unbracketed and not re-formatted"
        );
        for bad in [
            "not json",
            r#"["192.0.2.1:4420"]"#,
            r#"[" 192.0.2.1"]"#,
            r#"[""]"#,
            r#"["tcp://192.0.2.1"]"#,
            r#"["[192.0.2.1]"]"#,
            r#"["[2001:db8::1]:4420"]"#,
            r#"["host.example"]"#,
            r#"["fe80::1%eth0"]"#,
            r#"["192.0.2.1", 5]"#,
        ] {
            assert!(
                parse_multipath_addresses(&ctx(Some(bad))).is_err(),
                "{bad} was accepted"
            );
        }
    }

    #[test]
    fn portals() {
        let (p, bad) = ublk_portals(&ctx(Some(r#"["192.0.2.1","2001:db8::1"]"#)), "192.0.2.9", "4420");
        assert_eq!(
            (p, bad),
            (
                vec!["192.0.2.1:4420".to_string(), "[2001:db8::1]:4420".to_string()],
                None
            )
        );
        let (p, bad) = ublk_portals(&ctx(None), "[2001:db8::9]", "4420");
        assert_eq!((p, bad), (vec!["[2001:db8::9]:4420".to_string()], None));
        let (p, bad) = ublk_portals(&ctx(Some(r#"["bad:4420"]"#)), "192.0.2.9", "4420");
        assert_eq!(
            p,
            vec!["192.0.2.9:4420".to_string()],
            "a bad hint falls back to the single address"
        );
        assert!(bad.is_some());
    }
}
