// The privileged mount-helper's XPC contract (design 11 §8). The helper is the ONLY privileged
// grain: a tiny root SMAppService daemon whose sole job is `mount`/`unmount`. Running the whole
// hostd as root is rejected — it would run the NFS server + RAM replica + cache as root and prompt
// for admin on everything. The protocol is versioned so an updated app detects skew.

import Foundation

/// Bumped on any incompatible XPC change; `mist doctor` already checks version skew.
public let kMountHelperProtocolVersion = 1

/// The Mach service name the helper's launchd daemon registers (Contents/Library/LaunchDaemons).
public let kMountHelperMachServiceName = "dev.mist.mount-helper"

@objc public protocol MountHelperProtocol {
    /// Mount the loopback NFS server `port` (a hostd-owned ephemeral port on 127.0.0.1) at
    /// `mountpoint` (must be under ~/Mist). The helper validates a narrow argument allowlist before
    /// shelling `mount_nfs` — loopback-only server, ~/Mist mountpoint, syntactically valid share
    /// name, and port range. It does not yet prove the port is owned by hostd.
    func mount(
        share: String,
        mountpoint: String,
        port: Int,
        nfs41: Bool,
        withReply reply: @escaping (_ ok: Bool, _ message: String) -> Void
    )

    func unmount(
        mountpoint: String,
        withReply reply: @escaping (_ ok: Bool, _ message: String) -> Void
    )

    /// Returns the helper's protocol version so the app can detect skew after an update.
    func version(withReply reply: @escaping (_ version: Int) -> Void)
}

/// Code-signing requirement gating the XPC listener (design 11 §8): only the same-team
/// Developer-ID-signed Mist.app / mist-hostd may call the helper (Team 9YA6F7T5Z4).
public let kMountHelperClientRequirement =
    "anchor apple generic and certificate leaf[subject.OU] = \"9YA6F7T5Z4\" and "
    + "(identifier \"dev.mist.app\" or identifier \"dev.mist.hostd\")"
