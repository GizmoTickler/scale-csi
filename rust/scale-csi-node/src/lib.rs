//! scale-csi node agent: the CSI node service in Rust (M2 of the scale-csi
//! plan). It replaces the Go node plugin protocol by protocol and must stay
//! interchangeable with it on a live node.

pub mod args;
pub mod capability;
pub mod capacity;
pub mod config;
pub mod csi;
pub mod discovery;
pub mod events;
pub mod exec;
pub mod health;
pub mod locks;
pub mod metrics;
pub mod mount;
pub mod node_id;
pub mod nvme;
pub mod nvme_addresses;
pub mod publish;
pub mod records;
pub mod service;
pub mod stage;
pub mod ublk_client;
pub mod ublk_stage;
pub mod ublk_state;

#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod publish_tests;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod ublk_stage_tests;
