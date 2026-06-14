//! mistd library surface (exposed for integration tests; the binary is `main.rs`).

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub mod config;

#[cfg(target_os = "linux")]
pub mod linux;
