// Service lifecycle via SMAppService (macOS 13+), per the onboarding research.
//
// Least privilege by default: mist-hostd registers as a LaunchAGENT — it runs as the user, so
// there is NO admin password, just the one-time "Allow in the Background" toggle. The privileged
// root mount-helper (a LaunchDAEMON, the only thing that needs admin) is DEFERRED: we register it
// only if an unprivileged mount fails on a locked-down Mac. Registration is idempotent and
// version-aware (re-register after an update so the new binary runs — Sparkle callbacks are
// unreliable, so we compare the bundle version at launch).

import Foundation
import MistHelperProtocol
import ServiceManagement

/// A background service's install/approval state, mapped from `SMAppService.Status` for the UI.
public enum ServiceState: Equatable {
    case enabled  // approved + running
    case requiresApproval  // registered, awaiting the user's Login-Items toggle
    case notRegistered
    case notFound  // bundle-layout problem (or unsigned dev build)
    case unsupported  // < macOS 13
    case failed(String)

    /// Drives the menu-bar "Action needed" badge.
    public var needsAttention: Bool {
        switch self {
        case .enabled, .notRegistered: return false
        default: return true
        }
    }

    public var label: String {
        switch self {
        case .enabled: return "running"
        case .requiresApproval: return "needs approval in System Settings → Login Items"
        case .notRegistered: return "not installed"
        case .notFound: return "not found (unsigned build, or bundle layout)"
        case .unsupported: return "requires macOS 13+"
        case .failed(let s): return "failed: \(s)"
        }
    }
}

enum MistServices {
    static let agentPlist = "dev.mist.hostd.plist"
    static let helperPlist = "dev.mist.mount-helper.plist"
    private static let versionKey = "MistLastRegisteredBuild"

    @available(macOS 13.0, *)
    private static func state(of svc: SMAppService) -> ServiceState {
        switch svc.status {
        case .enabled: return .enabled
        case .requiresApproval: return .requiresApproval
        case .notRegistered: return .notRegistered
        case .notFound: return .notFound
        @unknown default: return .failed("unknown status")
        }
    }

    private static var currentBuild: String {
        Bundle.main.infoDictionary?["CFBundleVersion"] as? String ?? "0"
    }

    /// Register the hostd LaunchAgent (no admin). Idempotent; re-registers on a version change so
    /// the updated binary runs. Returns the resulting state for the wizard/menu to render.
    static func ensureAgentRegistered() -> ServiceState {
        guard #available(macOS 13.0, *) else { return .unsupported }
        let svc = SMAppService.agent(plistName: agentPlist)
        let last = UserDefaults.standard.string(forKey: versionKey)

        if svc.status == .enabled, last == currentBuild {
            return .enabled  // already current — nothing to do (avoids Sonoma re-register pitfalls)
        }
        if svc.status == .enabled, last != currentBuild {
            try? svc.unregister()
            Thread.sleep(forTimeInterval: 0.4)  // dodge the Code=1 "Operation not permitted" race
        }
        do {
            if svc.status != .enabled { try svc.register() }
            UserDefaults.standard.set(currentBuild, forKey: versionKey)
        } catch {
            return .failed(error.localizedDescription)
        }
        return state(of: svc)
    }

    static func agentStatus() -> ServiceState {
        guard #available(macOS 13.0, *) else { return .unsupported }
        return state(of: SMAppService.agent(plistName: agentPlist))
    }

    static func helperStatus() -> ServiceState {
        guard #available(macOS 13.0, *) else { return .unsupported }
        return state(of: SMAppService.daemon(plistName: helperPlist))
    }

    /// Register the privileged mount-helper (one admin/Touch-ID prompt). DEFERRED by default — only
    /// invoke when an unprivileged mount fails on a policy-locked Mac.
    static func registerHelper() -> ServiceState {
        guard #available(macOS 13.0, *) else { return .unsupported }
        let svc = SMAppService.daemon(plistName: helperPlist)
        do { if svc.status != .enabled { try svc.register() } } catch {
            return .failed(error.localizedDescription)
        }
        return state(of: svc)
    }

    /// Start-at-login for the app itself (SMAppService.mainApp — also a Login Items entry).
    static func startAtLoginEnabled() -> Bool {
        guard #available(macOS 13.0, *) else { return false }
        return SMAppService.mainApp.status == .enabled
    }
    static func setStartAtLogin(_ on: Bool) {
        guard #available(macOS 13.0, *) else { return }
        do {
            if on { try SMAppService.mainApp.register() } else { try SMAppService.mainApp.unregister() }
        } catch {
            NSLog("mist: start-at-login toggle failed: \(error.localizedDescription)")
        }
    }

    /// Deep-link to System Settings → General → Login Items & Extensions (no API can auto-approve;
    /// the user toggles "Allow in the Background" there).
    static func openLoginItems() {
        if #available(macOS 13.0, *) { SMAppService.openSystemSettingsLoginItems() }
    }

    /// Clean uninstall — entries persist across reboot otherwise (by design).
    static func unregisterAll() {
        guard #available(macOS 13.0, *) else { return }
        try? SMAppService.agent(plistName: agentPlist).unregister()
        try? SMAppService.daemon(plistName: helperPlist).unregister()
    }
}

/// The privileged mount-helper XPC client (the policy-blocked-Mac fallback). The connection is
/// gated helper-side by a code-signing requirement; we just dial the Mach service.
enum HelperClient {
    static func mountViaHelper(
        share: String, mountpoint: String, port: Int, nfs41: Bool,
        completion: @escaping (Bool, String) -> Void
    ) {
        let conn = NSXPCConnection(machServiceName: kMountHelperMachServiceName, options: .privileged)
        conn.remoteObjectInterface = NSXPCInterface(with: MountHelperProtocol.self)
        conn.resume()
        let proxy = conn.remoteObjectProxyWithErrorHandler { err in
            completion(false, "helper unreachable: \(err.localizedDescription)")
        } as? MountHelperProtocol
        proxy?.mount(share: share, mountpoint: mountpoint, port: port, nfs41: nfs41) { ok, msg in
            completion(ok, msg)
            conn.invalidate()
        }
    }
}
