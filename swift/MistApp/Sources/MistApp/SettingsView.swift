// The in-app Settings window (SwiftUI Settings scene, ⌘,) — the modern equivalent of a preference
// pane (third-party System Settings panes are gone on Ventura+). Tabs: General, Services, About.

import MistControl
import SwiftUI

struct SettingsView: View {
    @ObservedObject var model: AppModel
    @State private var startAtLogin = MistServices.startAtLoginEnabled()

    var body: some View {
        TabView {
            general.tabItem { Label("General", systemImage: "gearshape") }
            services.tabItem { Label("Services", systemImage: "bolt.horizontal") }
            about.tabItem { Label("About", systemImage: "info.circle") }
        }
        .frame(width: 460, height: 300)
        .onAppear { model.refreshServiceStates() }
    }

    private var general: some View {
        Form {
            Toggle("Start Mist at login", isOn: $startAtLogin)
                .onChange(of: startAtLogin) { on in MistServices.setStartAtLogin(on) }
            LabeledContent("Mount location", value: "~/Mist/<vm>/<share>")
            Text("Shares auto-mount whenever their VM is reachable. Turn automount off per-VM from "
                + "the menu bar.")
                .font(.caption).foregroundStyle(.secondary)
        }
        .padding()
    }

    private var services: some View {
        Form {
            LabeledContent("Background service (mist-hostd)") { stateRow(model.agentState) }
            LabeledContent("Mount helper (root, optional)") { stateRow(model.helperState) }
            HStack {
                Button("Re-install / repair") { model.installAgent(); model.refreshServiceStates() }
                Button("Open Login Items…") { model.openLoginItems() }
            }
            Text("mist-hostd runs as you (no admin). The mount helper is only needed on Macs that "
                + "block user mounts — Mist installs it on demand.")
                .font(.caption).foregroundStyle(.secondary)
        }
        .padding()
    }

    private var about: some View {
        VStack(spacing: 10) {
            Image(systemName: "cloud.fill").font(.system(size: 44)).foregroundStyle(.tint)
            Text("Mist").font(.title).bold()
            Text("Version \(appVersion)").foregroundStyle(.secondary)
            Text("Near-native macOS access to your Linux VM's files.")
                .font(.callout).foregroundStyle(.secondary)
            // Wired to Sparkle's updater in the signed build.
            Button("Check for Updates…") { NotificationCenter.default.post(name: .mistCheckForUpdates, object: nil) }
            Link("github.com/darekhta/Mist", destination: URL(string: "https://github.com/darekhta/Mist")!)
                .font(.caption)
        }
        .padding()
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private func stateRow(_ s: ServiceState) -> some View {
        HStack(spacing: 6) {
            Circle().fill(s == .enabled ? Color.green : (s.needsAttention ? .orange : .secondary))
                .frame(width: 8, height: 8)
            Text(s.label).foregroundStyle(.secondary)
        }
    }

    private var appVersion: String {
        Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String ?? "0.1.0"
    }
}

extension Notification.Name {
    /// Posted by the Settings "Check for Updates" button; the app wires it to Sparkle when signed.
    static let mistCheckForUpdates = Notification.Name("MistCheckForUpdates")
}
