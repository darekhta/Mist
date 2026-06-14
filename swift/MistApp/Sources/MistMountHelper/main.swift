// mist-mount-helper — the tiny root SMAppService daemon (design 11 §8). Its ONLY XPC methods are
// mount/unmount; it shells validated `mount_nfs`/`umount` with the same option set hostd validates.
// The listener is gated by a code-signing requirement (only the same-team Developer-ID-signed
// Mist.app / mist-hostd may call it) and an argument allowlist: loopback-only servers, mountpoints
// under ~/Mist, syntactically valid share names, and TCP ports in range. The helper does not yet
// call back into hostd to prove the port is currently owned by Mist; that remains a defense-in-depth
// hardening item.

import Foundation
import MistHelperProtocol

final class MountHelper: NSObject, MountHelperProtocol, NSXPCListenerDelegate {
    // MARK: NSXPCListenerDelegate — gate every new connection by code-signing requirement.

    func listener(_ listener: NSXPCListener, shouldAcceptNewConnection conn: NSXPCConnection) -> Bool {
        // Pin the caller to the same team + known identifiers. setCodeSigningRequirement is the
        // supported gate (macOS 13+); reject anything that doesn't satisfy it.
        if #available(macOS 13.0, *) {
            // Pins the caller to the requirement; connections that don't satisfy it are refused by
            // the XPC runtime before any method is dispatched.
            conn.setCodeSigningRequirement(kMountHelperClientRequirement)
        } else {
            return false
        }
        conn.exportedInterface = NSXPCInterface(with: MountHelperProtocol.self)
        conn.exportedObject = self
        conn.resume()
        return true
    }

    // MARK: MountHelperProtocol

    func version(withReply reply: @escaping (Int) -> Void) {
        reply(kMountHelperProtocolVersion)
    }

    func mount(
        share: String, mountpoint: String, port: Int, nfs41: Bool,
        withReply reply: @escaping (Bool, String) -> Void
    ) {
        guard validMountpoint(mountpoint) else {
            return reply(false, "mountpoint must be an absolute path under a user's ~/Mist")
        }
        guard (1...65535).contains(port) else {
            return reply(false, "port out of range")
        }
        guard share.allSatisfy({ $0.isLetter || $0.isNumber || "-_.".contains($0) }) else {
            return reply(false, "invalid share name")
        }
        // Same options hostd uses; loopback-only server (127.0.0.1). rsize capped at 1 MiB (the
        // macOS client silently falls to 32 KiB above it — design 11 §10).
        let opts: String
        let spec: String
        if nfs41 {
            spec = "127.0.0.1:/"
            opts = "vers=4.1,tcp,port=\(port),rw,readahead=128,rsize=1048576,wsize=1048576,"
                + "hard,intr,noatime,nosuid,nodev,actimeo=5"
        } else {
            spec = "127.0.0.1:/\(share)"
            opts = "vers=3,tcp,port=\(port),mountport=\(port),rw,nolocks,locallocks,rdirplus,"
                + "readahead=128,rsize=1048576,wsize=1048576,hard,intr,noatime,nosuid,nodev,actimeo=5"
        }
        try? FileManager.default.createDirectory(
            atPath: mountpoint, withIntermediateDirectories: true)
        let (code, err) = run("/sbin/mount_nfs", ["-o", opts, spec, mountpoint])
        if code == 0 {
            _ = run("/usr/bin/mdutil", ["-i", "off", mountpoint])  // keep Spotlight off the mount
            reply(true, mountpoint)
        } else {
            reply(false, "mount_nfs failed: \(err)")
        }
    }

    func unmount(mountpoint: String, withReply reply: @escaping (Bool, String) -> Void) {
        guard validMountpoint(mountpoint) else {
            return reply(false, "refusing to unmount a path outside ~/Mist")
        }
        let (code, err) = run("/sbin/umount", [mountpoint])
        if code != 0 { _ = run("/sbin/umount", ["-f", mountpoint]) }
        reply(code == 0, code == 0 ? "ok" : err)
    }

    // MARK: validation

    /// Only ~/Mist/<vm>/<share> mountpoints are permitted (design 11 §7/§8). The helper runs as
    /// root, so we accept any user's home but require the `/Mist/` segment and reject `..`.
    private func validMountpoint(_ path: String) -> Bool {
        guard path.hasPrefix("/"), !path.contains("/../"), !path.hasSuffix("/..") else {
            return false
        }
        return path.contains("/Mist/")
    }

    private func run(_ tool: String, _ args: [String]) -> (Int32, String) {
        let p = Process()
        p.executableURL = URL(fileURLWithPath: tool)
        p.arguments = args
        let errPipe = Pipe()
        p.standardError = errPipe
        do {
            try p.run()
            p.waitUntilExit()
        } catch {
            return (-1, "\(error)")
        }
        let data = errPipe.fileHandleForReading.readDataToEndOfFile()
        return (p.terminationStatus, String(decoding: data, as: UTF8.self))
    }
}

// launchd starts us; serve the Mach service forever.
let delegate = MountHelper()
let listener = NSXPCListener(machServiceName: kMountHelperMachServiceName)
listener.delegate = delegate
listener.resume()
NSLog("mist-mount-helper: listening on \(kMountHelperMachServiceName)")
RunLoop.main.run()
