// First-run onboarding wizard (research: OrbStack/Tailscale 5-stage shape; every OS prompt is
// preceded by an in-app explainer). Welcome → Install services → Discover & add VM → Automount →
// Done. Least-privilege: hostd is a LaunchAgent (no admin); the root helper is deferred.

import AppKit
import MistControl
import SwiftUI

struct WizardView: View {
    @ObservedObject var model: AppModel
    /// Closes the hosting window (the wizard runs in an AppDelegate-managed NSWindow, not a Scene).
    var onClose: () -> Void = {}
    @State private var stage: Stage = .welcome
    @State private var pickedVM: String?

    enum Stage: Int, CaseIterable {
        case welcome, install, addVM, automount, done
        var title: String {
            switch self {
            case .welcome: return "Welcome to Mist"
            case .install: return "Install the background service"
            case .addVM: return "Find your VM"
            case .automount: return "Mount your files"
            case .done: return "You're all set"
            }
        }
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Image(systemName: "cloud.fill").font(.largeTitle).foregroundStyle(.tint)
                VStack(alignment: .leading) {
                    Text(stage.title).font(.title2).bold()
                    Text("Step \(stage.rawValue + 1) of \(Stage.allCases.count)")
                        .font(.caption).foregroundStyle(.secondary)
                }
                Spacer()
            }
            .padding()
            Divider()
            ScrollView { content.padding() }.frame(maxHeight: .infinity)
            Divider()
            footer.padding()
        }
        .frame(width: 540, height: 440)
        .onAppear { model.refreshServiceStates(); model.refreshDiscovery() }
    }

    @ViewBuilder private var content: some View {
        switch stage {
        case .welcome:
            explainer(
                "Mist gives your Mac near-native access to your Linux VM's files at ~/Mist.",
                bullets: [
                    "Runs one small background service (mist-hostd) as you — no admin password.",
                    "Finds your VM automatically over the local network — no IP to type.",
                    "You copy a token once; mounts appear in Finder and reconnect on their own.",
                ])
        case .install:
            installStage
        case .addVM:
            addVMStage
        case .automount:
            automountStage
        case .done:
            explainer(
                "Mist is running in your menu bar (the ☁ icon).",
                bullets: [
                    "Your shares are at ~/Mist and auto-mount whenever the VM is up.",
                    "Add more VMs anytime from the menu's Discovered list.",
                    "Open Settings (⌘,) for updates, automount, and start-at-login.",
                ])
        }
    }

    // MARK: stages

    private var installStage: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Mist installs a background helper that runs as you (no admin password). macOS will "
                + "ask you to approve it once under Login Items & Extensions.")
            HStack {
                statusDot(model.agentState)
                Text("mist-hostd: \(model.agentState.label)")
                Spacer()
                Button("Install") { model.installAgent() }
            }
            if model.agentState == .requiresApproval {
                Button("Open Login Items in System Settings…") { model.openLoginItems() }
            }
            if case .failed = model.agentState {
                Text("(An unsigned dev build can't register a login item — that's expected; the app "
                    + "still works against a hostd you run yourself.)")
                    .font(.caption).foregroundStyle(.secondary)
            }
        }
    }

    private var addVMStage: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Mist looks for VMs advertising on your local network. macOS may ask for Local "
                + "Network permission — that's so Mist can find your VM.")
            if model.addableGuests.isEmpty && model.vms.isEmpty {
                HStack { ProgressView().controlSize(.small); Text("Searching…") }
                Button("Search again") { model.refreshDiscovery() }
            }
            ForEach(model.addableGuests) { g in
                HStack {
                    Image(systemName: "server.rack").foregroundStyle(.secondary)
                    Text(g.host)
                    Spacer()
                    Button("Add…") { addGuest(g) }
                }
            }
            ForEach(model.vms) { vm in
                HStack {
                    statusDotColor(vm.isReachable ? .green : .orange)
                    Text("\(vm.name) — added").bold()
                    Spacer()
                    Text(vm.endpoint).font(.caption2).foregroundStyle(.secondary)
                }
            }
            Text("You'll pick the token file you copied from the guest's /etc/mist/token.")
                .font(.caption).foregroundStyle(.secondary)
        }
    }

    private var automountStage: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Shares auto-mount at ~/Mist/<vm>/<share> whenever the VM is reachable.")
            if model.mounts.isEmpty {
                HStack { ProgressView().controlSize(.small); Text("Mounting (first mount seeds the tree)…") }
            }
            ForEach(model.mounts) { m in
                HStack {
                    Image(systemName: "externaldrive.connected.to.line.below.fill").foregroundStyle(.green)
                    Text("\(m.vm)/\(m.share)")
                    Spacer()
                    Button("Reveal in Finder") { model.reveal(m.mountpoint) }.controlSize(.small)
                }
            }
        }
    }

    // MARK: footer

    private var footer: some View {
        HStack {
            if stage != .welcome && stage != .done {
                Button("Back") { stage = Stage(rawValue: stage.rawValue - 1) ?? .welcome }
            }
            Spacer()
            Button(stage == .done ? "Finish" : "Continue") { advance() }
                .keyboardShortcut(.defaultAction)
                .disabled(!canAdvance)
        }
    }

    private var canAdvance: Bool {
        switch stage {
        case .addVM: return !model.vms.isEmpty  // need at least one VM added
        default: return true
        }
    }

    private func advance() {
        if stage == .done {
            model.completeOnboarding()
            onClose()
            return
        }
        stage = Stage(rawValue: stage.rawValue + 1) ?? .done
        if stage == .install { model.refreshServiceStates() }
        if stage == .addVM { model.refreshDiscovery() }
    }

    private func addGuest(_ g: DiscoveredVM) {
        let panel = NSOpenPanel()
        panel.title = "Select \(g.host)'s token file"
        panel.message = "Choose the token you copied from the guest's /etc/mist/token"
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        if panel.runModal() == .OK, let url = panel.url {
            model.addVM(name: g.suggestedName, tokenPath: url.path, vmUUID: g.vmUUID)
            pickedVM = g.suggestedName
        }
    }

    // MARK: bits

    private func explainer(_ headline: String, bullets: [String]) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(headline).font(.headline)
            ForEach(bullets, id: \.self) { b in
                Label(b, systemImage: "checkmark.circle.fill").foregroundStyle(.primary)
            }
        }
    }

    private func statusDot(_ s: ServiceState) -> some View {
        statusDotColor(s == .enabled ? .green : (s.needsAttention ? .orange : .secondary))
    }
    private func statusDotColor(_ c: Color) -> some View {
        Circle().fill(c).frame(width: 9, height: 9)
    }
}
