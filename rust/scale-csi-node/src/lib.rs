//! scale-csi node agent: the CSI node service in Rust (M2 of the scale-csi
//! plan). It replaces the Go node plugin protocol by protocol and must stay
//! interchangeable with it on a live node.

pub mod args;
pub mod capability;
pub mod config;
pub mod csi;
pub mod discovery;
pub mod exec;
pub mod health;
pub mod locks;
pub mod metrics;
pub mod mount;
pub mod node_id;
pub mod service;
pub mod ublk_client;
