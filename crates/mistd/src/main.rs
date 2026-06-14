//! mistd — the Mist guest daemon (Linux only).
//!
//! Handles session transport, Hello/auth, share announcement, snapshot walking, the fanotify
//! journal, read/stat RPCs, and contained mutation application.

#[cfg(target_os = "linux")]
fn main() -> anyhow::Result<()> {
    mistd::linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("mistd runs inside the Linux guest; this build target is not Linux")
}
