// The menu content — Tailscale's sectioned model (design 11 §7): This VM · Shares · Health ·
// Quick Actions. Pure presentation; every button is one control verb dispatched through AppModel.

import MistControl
import SwiftUI

struct MenuView: View {
    @ObservedObject var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            if !model.daemonReachable {
                Label("mist-hostd not reachable", systemImage: "exclamationmark.triangle.fill")
                    .foregroundStyle(.orange)
                Text(model.lastError ?? "Start it from Login Items, or run `mist-hostd`.")
                    .font(.caption).foregroundStyle(.secondary)
                Divider()
            }

            // -- This VM + Shares ------------------------------------------------
            if model.vms.isEmpty {
                Text("No VMs paired").foregroundStyle(.secondary)
            }
            ForEach(model.vms) { vm in
                vmSection(vm)
            }

            Divider()

            // -- Health (rendered straight from `mist doctor`) -------------------
            DisclosureGroup("Health") {
                ForEach(model.checks) { c in
                    Label(c.detail, systemImage: icon(for: c.level))
                        .foregroundStyle(color(for: c.level))
                        .font(.caption)
                }
            }

            Divider()

            // -- Discovered VMs (autodiscovery) ----------------------------------
            HStack {
                Text("Discovered").font(.headline)
                Spacer()
                Button { model.refreshDiscovery() } label: { Image(systemName: "arrow.clockwise") }
                    .buttonStyle(.borderless).controlSize(.small)
            }
            if model.addableGuests.isEmpty {
                Text("No new guests on the network").font(.caption).foregroundStyle(.secondary)
            }
            ForEach(model.addableGuests) { guest in
                HStack {
                    Image(systemName: "server.rack").foregroundStyle(.secondary)
                    VStack(alignment: .leading, spacing: 0) {
                        Text(guest.host).font(.callout)
                        Text(guest.vmUUID.map { "id \($0.prefix(8))…" } ?? "no identity")
                            .font(.caption2).foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("Add…") { addGuest(guest) }.controlSize(.small)
                }
            }

            Divider()

            // -- Quick Actions ---------------------------------------------------
            if model.agentState.needsAttention {
                HStack {
                    Image(systemName: "exclamationmark.triangle.fill").foregroundStyle(.orange)
                    Text("Background service: \(model.agentState.label)").font(.caption)
                    Spacer()
                    Button("Fix…") { model.openLoginItems() }.controlSize(.small)
                }
            }
            Button("Open Mist folder") { model.reveal(NSHomeDirectory() + "/Mist") }
            Button("Settings…") { openSettings() }
            Button("Run setup again…") {
                NotificationCenter.default.post(name: .openWizard, object: nil)
            }
            Button("Refresh") { model.refresh() }
            Divider()
            Button("Quit Mist") { NSApplication.shared.terminate(nil) }
        }
        .padding(12)
        .frame(width: 340)
    }

    /// Add a discovered guest: ask for its token file (the 32 bytes copied from `/etc/mist/token`),
    /// then Mist binds `bridge="auto"` and brings it live — no ssh, no IP.
    private func addGuest(_ guest: DiscoveredVM) {
        let panel = NSOpenPanel()
        panel.title = "Select \(guest.host)'s token file"
        panel.message = "Choose the token you copied from the guest's /etc/mist/token"
        panel.canChooseFiles = true
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = false
        if panel.runModal() == .OK, let url = panel.url {
            model.addVM(name: guest.suggestedName, tokenPath: url.path, vmUUID: guest.vmUUID)
        }
    }

    @ViewBuilder
    private func vmSection(_ vm: VMStatus) -> some View {
        HStack {
            Circle().fill(vm.isReachable ? Color.green : Color.orange).frame(width: 8, height: 8)
            Text(vm.name).bold()
            Spacer()
            Text(vm.endpoint).font(.caption2).foregroundStyle(.secondary)
        }
        ForEach(vm.shares) { share in
            HStack {
                Text(share.name).font(.callout)
                Spacer()
                if let m = mountFor(vm: vm.name, share: share.name) {
                    Button("Reveal") { model.reveal(m.mountpoint) }.controlSize(.small)
                    Button("Unmount") { model.unmount(vm: vm.name, share: share.name) }
                        .controlSize(.small)
                } else {
                    Button("Mount") { model.mount(vm: vm.name, share: share.name) }
                        .controlSize(.small)
                        .disabled(!share.isLive)
                }
            }
        }
    }

    private func mountFor(vm: String, share: String) -> MountInfo? {
        model.mounts.first { $0.vm == vm && $0.share == share }
    }

    /// Open the SwiftUI Settings scene from the menu-bar agent (needs the activation-policy switch).
    private func openSettings() {
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
        // macOS 13 renamed showPreferencesWindow: → showSettingsWindow:.
        NSApp.sendAction(Selector(("showSettingsWindow:")), to: nil, from: nil)
    }

    private func icon(for level: String) -> String {
        switch level {
        case "ok": return "checkmark.circle"
        case "warn": return "exclamationmark.triangle"
        default: return "xmark.octagon"
        }
    }
    private func color(for level: String) -> Color {
        switch level {
        case "ok": return .green
        case "warn": return .orange
        default: return .red
        }
    }
}
