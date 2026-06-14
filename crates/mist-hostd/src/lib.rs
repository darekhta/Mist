//! mist-hostd library: sessions, snapshot assembly, control plane.
//! See `session`, `control`, and `config` for the main daemon pieces.

pub mod config;
pub mod config_writer;
pub mod conflicts;
pub mod control;
pub mod mount;
pub mod resolve;
pub mod session;
pub mod sidestore;
pub mod surface;

/// Pin the current thread to the macOS USER_INTERACTIVE QoS class so Mist's serving path stays
/// on P-cores — Darwin demotes an idle daemon's threads to E-cores otherwise. No-op on
/// non-macOS (the daemon is macOS-resident; this keeps the crate compiling for Linux CI).
pub fn pin_thread_user_interactive() {
    #[cfg(target_os = "macos")]
    #[allow(unsafe_code)]
    // SAFETY: plain libc call affecting only the current thread.
    unsafe {
        libc::pthread_set_qos_class_self_np(libc::qos_class_t::QOS_CLASS_USER_INTERACTIVE, 0);
    }
}
