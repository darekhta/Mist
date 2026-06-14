// Plain DTOs decoded from hostd's control JSON. These are *views* of state hostd owns; the app
// never computes them (ADR-14).

import Foundation

public struct ShareStatus: Identifiable, Hashable {
    public let name: String
    public let state: String  // seeding | live | degraded | offline
    public let nodes: Int
    public var id: String { name }

    public var isLive: Bool { state == "live" }
}

public struct VMStatus: Identifiable, Hashable {
    public let name: String
    public let state: String  // disconnected | connecting | ready | degraded
    public let endpoint: String
    public let vmUUID: String?
    public let shares: [ShareStatus]
    public var id: String { name }

    /// The status-bar state machine input (design 11 §7): reachability dot color.
    public var isReachable: Bool { state == "ready" }
}

public struct MountInfo: Identifiable, Hashable {
    public let vm: String
    public let share: String
    public let mountpoint: String
    public var id: String { "\(vm)/\(share)" }
}

public struct DoctorCheck: Identifiable, Hashable {
    public let level: String  // ok | warn | fail
    public let check: String
    public let detail: String
    public var id: String { check + detail }
}

public extension ControlClient {
    /// `status` → parsed VM + mount lists.
    func status() throws -> (vms: [VMStatus], mounts: [MountInfo]) {
        let r = try request(["cmd": "status"])
        let vms = (r["vms"] as? [[String: Any]] ?? []).map { v -> VMStatus in
            let shares = (v["shares"] as? [[String: Any]] ?? []).map {
                ShareStatus(
                    name: $0["name"] as? String ?? "?",
                    state: $0["state"] as? String ?? "?",
                    nodes: ($0["nodes"] as? NSNumber)?.intValue ?? 0
                )
            }
            return VMStatus(
                name: v["name"] as? String ?? "?",
                state: v["state"] as? String ?? "?",
                endpoint: v["endpoint"] as? String ?? "",
                vmUUID: v["vm_uuid"] as? String,
                shares: shares
            )
        }
        let mounts = (r["mounts"] as? [[String: Any]] ?? []).map {
            MountInfo(
                vm: $0["vm"] as? String ?? "?",
                share: $0["share"] as? String ?? "?",
                mountpoint: $0["mountpoint"] as? String ?? ""
            )
        }
        return (vms, mounts)
    }

    /// `doctor --json` → the health checks the UI renders verbatim (design 11 §7 "Health").
    func doctor() throws -> [DoctorCheck] {
        let r = try request(["cmd": "doctor"])
        return (r["checks"] as? [[String: Any]] ?? []).map {
            DoctorCheck(
                level: $0["level"] as? String ?? "ok",
                check: $0["check"] as? String ?? "?",
                detail: $0["detail"] as? String ?? ""
            )
        }
    }

    func mount(vm: String, share: String, nfs41: Bool = false) throws -> String {
        let r = try request(["cmd": "mount", "vm": vm, "share": share, "nfs41": nfs41])
        return r["mountpoint"] as? String ?? ""
    }

    func unmount(vm: String, share: String) throws {
        _ = try request(["cmd": "umount", "vm": vm, "share": share])
    }

    /// `discover` → guests advertising `_mist._tcp` on the network (autodiscovery, no token needed).
    func discover() throws -> [DiscoveredVM] {
        let r = try request(["cmd": "discover"])
        return (r["mdns"] as? [[String: Any]] ?? []).map {
            DiscoveredVM(
                instance: $0["instance"] as? String ?? "?",
                host: $0["host"] as? String ?? "?",
                port: ($0["port"] as? NSNumber)?.intValue ?? 6478,
                vmUUID: $0["vm_uuid"] as? String
            )
        }
    }

    /// `add` → register a discovered guest with its token. Mist binds `bridge="auto"` and brings it
    /// live. `tokenPath` is the file you copied from the guest's `/etc/mist/token`.
    func add(name: String, tokenPath: String, vmUUID: String?) throws {
        var req: [String: Any] = ["cmd": "add", "name": name, "token": tokenPath]
        if let u = vmUUID { req["uuid"] = u }
        _ = try request(req)
    }
}

/// A guest seen on the network but not yet added (autodiscovery result).
public struct DiscoveredVM: Identifiable, Hashable {
    public let instance: String
    public let host: String
    public let port: Int
    public let vmUUID: String?
    public var id: String { vmUUID ?? "\(host):\(port)" }

    /// A friendly default name for the `[vm.<name>]` block (derive from the host, sanitized).
    public var suggestedName: String {
        let base = host.replacingOccurrences(of: ".local", with: "")
        let cleaned = base.lowercased().filter { $0.isLetter || $0.isNumber || $0 == "-" }
        return cleaned.isEmpty ? "vm" : cleaned
    }
}
