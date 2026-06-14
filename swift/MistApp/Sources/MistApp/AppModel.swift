// AppModel — the single ObservableObject the menu renders. It is driven by hostd's push stream
// (events --follow, *intent*) and reconciled against kernel reality from DiskArbitration
// (mount/unmount), so a manual Terminal `umount` or a v4.1 forced-remount restore is reflected
// without polling. All state hostd owns; this just mirrors it (ADR-14).

import Combine
import DiskArbitration
import Foundation
import MistControl

@MainActor
final class AppModel: ObservableObject {
    /// Shared instance so the AppDelegate (launch-time wizard) and the SwiftUI scenes use one model.
    static let shared = AppModel()

    @Published var vms: [VMStatus] = []
    @Published var mounts: [MountInfo] = []
    @Published var checks: [DoctorCheck] = []
    @Published var daemonReachable = false
    @Published var lastError: String?
    @Published var discovered: [DiscoveredVM] = []
    @Published var agentState: ServiceState = .notRegistered
    @Published var helperState: ServiceState = .notRegistered

    /// First-run onboarding gate (drives whether the wizard window opens at launch).
    var onboardingComplete: Bool { UserDefaults.standard.bool(forKey: "MistOnboardingComplete") }
    func completeOnboarding() { UserDefaults.standard.set(true, forKey: "MistOnboardingComplete") }

    /// Anything in the menu's "Action needed" row: a service awaiting approval, or hostd down.
    var needsAttention: Bool { !daemonReachable || agentState.needsAttention }

    private let client = ControlClient()
    private var pollTimer: Timer?
    private var discoveryTimer: Timer?
    private var eventsThread: Thread?
    private var daSession: DASession?
    private var stopEvents = false

    /// Status-bar icon state machine (design 11 §7): no VM → paired-idle → mounted → error.
    enum IconState { case none, idle, mounted, error }
    var iconState: IconState {
        if !daemonReachable { return .error }
        if vms.isEmpty { return .none }
        if !mounts.isEmpty { return .mounted }
        if vms.contains(where: { !$0.isReachable }) { return .error }
        return .idle
    }

    func start() {
        // Returning user: ensure the hostd agent is registered (re-registers after an update so the
        // new binary runs). First run leaves this to the wizard's explicit "Install" step.
        if onboardingComplete {
            agentState = MistServices.ensureAgentRegistered()
        }
        refreshServiceStates()
        refresh()
        refreshDiscovery()
        // Coarse poll as a backstop; the events stream + DiskArbitration provide the fast path.
        pollTimer = Timer.scheduledTimer(withTimeInterval: 3, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.refresh() }
        }
        // Discovery is slower (mDNS browse) — poll it less often.
        discoveryTimer = Timer.scheduledTimer(withTimeInterval: 15, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.refreshDiscovery() }
        }
        startEventsStream()
        startDiskArbitration()
    }

    func stop() {
        pollTimer?.invalidate()
        discoveryTimer?.invalidate()
        stopEvents = true
        if let s = daSession { DASessionUnscheduleFromRunLoop(s, CFRunLoopGetMain(), CFRunLoopMode.defaultMode.rawValue) }
    }

    func refresh() {
        Task.detached { [client] in
            do {
                let (vms, mounts) = try client.status()
                let checks = (try? client.doctor()) ?? []
                await MainActor.run {
                    self.vms = vms
                    self.mounts = mounts
                    self.checks = checks
                    self.daemonReachable = true
                    self.lastError = nil
                }
            } catch {
                await MainActor.run {
                    self.daemonReachable = false
                    self.lastError = "\(error)"
                }
            }
        }
    }

    // MARK: actions (each is one control verb; hostd does the work)

    func mount(vm: String, share: String) {
        Task.detached { [client] in
            do { _ = try client.mount(vm: vm, share: share) } catch { await self.setError("\(error)") }
            await MainActor.run { self.refresh() }
        }
    }

    func unmount(vm: String, share: String) {
        Task.detached { [client] in
            do { try client.unmount(vm: vm, share: share) } catch { await self.setError("\(error)") }
            await MainActor.run { self.refresh() }
        }
    }

    func reveal(_ mountpoint: String) {
        // Pop the mount under Finder's Locations so the user can drag-to-pin (design 11 §7).
        let url = URL(fileURLWithPath: mountpoint)
        #if canImport(AppKit)
        NSWorkspace.shared.activateFileViewerSelecting([url])
        #endif
    }

    // MARK: background services (SMAppService)

    func refreshServiceStates() {
        agentState = MistServices.agentStatus()
        helperState = MistServices.helperStatus()
    }

    /// Wizard "Install" step: register the hostd LaunchAgent (no admin prompt).
    func installAgent() {
        agentState = MistServices.ensureAgentRegistered()
    }

    func openLoginItems() { MistServices.openLoginItems() }

    /// Autodiscover guests on the network (`_mist._tcp`). The ones not already added become the
    /// "Add VM" candidates in the panel.
    func refreshDiscovery() {
        Task.detached { [client] in
            let found = (try? client.discover()) ?? []
            await MainActor.run { self.discovered = found }
        }
    }

    /// Guests seen on the network that aren't configured yet. Match on `vm_uuid` (stable identity);
    /// only fall back to the lossy host-name heuristic for guests/VMs that have no uuid.
    var addableGuests: [DiscoveredVM] {
        let knownUUIDs = Set(vms.compactMap { $0.vmUUID })
        let knownNames = Set(vms.map { $0.name })
        return discovered.filter { g in
            if let uuid = g.vmUUID { return !knownUUIDs.contains(uuid) }
            return !knownNames.contains(g.suggestedName)
        }
    }

    /// Add a discovered guest: copy its token (the file the user picked) and let Mist bind it.
    func addVM(name: String, tokenPath: String, vmUUID: String?) {
        Task.detached { [client] in
            do {
                try client.add(name: name, tokenPath: tokenPath, vmUUID: vmUUID)
                await MainActor.run {
                    self.refresh()
                    self.refreshDiscovery()
                }
            } catch {
                await self.setError("\(error)")
            }
        }
    }

    private func setError(_ s: String) async {
        await MainActor.run { self.lastError = s }
    }

    // MARK: push

    private func startEventsStream() {
        let client = self.client
        let t = Thread {
            while !self.stopEventsUnsafe() {
                client.followEvents(
                    onLine: { _ in Task { @MainActor in self.refresh() } },
                    shouldStop: { self.stopEventsUnsafe() }
                )
                Thread.sleep(forTimeInterval: 1)  // reconnect after a drop
            }
        }
        t.start()
        eventsThread = t
    }

    private nonisolated func stopEventsUnsafe() -> Bool {
        // `stopEvents` is set once on stop(); a benign read race here only delays thread exit.
        MainActor.assumeIsolated { self.stopEvents }
    }

    private func startDiskArbitration() {
        guard let session = DASessionCreate(kCFAllocatorDefault) else { return }
        daSession = session
        let ctx = Unmanaged.passUnretained(self).toOpaque()
        let cb: DADiskAppearedCallback = { _, context in
            guard let context else { return }
            let model = Unmanaged<AppModel>.fromOpaque(context).takeUnretainedValue()
            Task { @MainActor in model.refresh() }
        }
        DARegisterDiskAppearedCallback(session, nil, cb, ctx)
        let goneCb: DADiskDisappearedCallback = { _, context in
            guard let context else { return }
            let model = Unmanaged<AppModel>.fromOpaque(context).takeUnretainedValue()
            Task { @MainActor in model.refresh() }
        }
        DARegisterDiskDisappearedCallback(session, nil, goneCb, ctx)
        DASessionScheduleWithRunLoop(session, CFRunLoopGetMain(), CFRunLoopMode.defaultMode.rawValue)
    }
}

#if canImport(AppKit)
import AppKit
#endif
