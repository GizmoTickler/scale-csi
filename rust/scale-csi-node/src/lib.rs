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
pub mod iscsi;
pub mod iscsi_stage;
pub mod kube_api;
pub mod kube_events;
pub mod locks;
pub mod metrics;
pub mod mount;
pub mod nfs;
pub mod node_id;
pub mod nvme;
pub mod nvme_addresses;
pub mod nvme_kernel;
pub mod publish;
pub mod records;
pub mod service;
pub mod session_gc;
pub mod session_registry;
pub mod stage;
pub mod ublk_client;
pub mod ublk_stage;
pub mod ublk_state;

#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod iscsi_stage_tests;
#[cfg(test)]
mod iscsi_testing;
#[cfg(test)]
mod iscsi_tests;
#[cfg(test)]
mod kube_events_tests;
#[cfg(test)]
mod nfs_tests;
#[cfg(test)]
mod nvme_kernel_tests;
#[cfg(test)]
mod publish_tests;
#[cfg(test)]
mod session_gc_tests;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod ublk_stage_tests;
