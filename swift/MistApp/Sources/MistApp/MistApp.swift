// Mist.app — a SwiftUI MenuBarExtra menu-bar agent (LSUIElement, no Dock icon) + a Settings scene,
// plus an AppDelegate that runs the first-run wizard at launch. Thin client over hostd's control UDS
// (no Mist logic in Swift). Ships Developer-ID notarized; auto-updates via Sparkle.

import AppKit
import SwiftUI

@main
struct MistApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var delegate
    @ObservedObject private var model = AppModel.shared

    var body: some Scene {
        MenuBarExtra {
            MenuView(model: model)
        } label: {
            Image(systemName: glyph)
        }
        .menuBarExtraStyle(.window)

        Settings {
            SettingsView(model: model)
        }
    }

    /// Status-bar state machine: no VM → idle → mounted → error (design 11 §7).
    private var glyph: String {
        if model.needsAttention { return "exclamationmark.icloud" }
        switch model.iconState {
        case .none: return "cloud"
        case .idle: return "cloud.fill"
        case .mounted: return "externaldrive.connected.to.line.below.fill"
        case .error: return "exclamationmark.icloud"
        }
    }
}

/// Drives launch-time behavior the SwiftUI scene graph can't: start the model and present the
/// first-run wizard in an app-owned NSWindow (reliable for a MenuBarExtra agent app, where opening
/// a window needs an explicit activation-policy switch).
@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    private var wizardWindow: NSWindow?

    func applicationDidFinishLaunching(_ notification: Notification) {
        let model = AppModel.shared
        model.start()
        Updater.shared.start()  // Sparkle: check at launch + on demand (Settings/menu)
        if !model.onboardingComplete {
            showWizard()
        }
        // Menu's "Run setup again…" routes here.
        NotificationCenter.default.addObserver(
            forName: .openWizard, object: nil, queue: .main
        ) { [weak self] _ in
            Task { @MainActor in self?.showWizard() }
        }
    }

    func showWizard() {
        if wizardWindow == nil {
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 540, height: 440),
                styleMask: [.titled, .closable],
                backing: .buffered, defer: false)
            window.title = "Welcome to Mist"
            window.isReleasedWhenClosed = false
            let view = WizardView(model: AppModel.shared) { [weak self] in self?.closeWizard() }
            window.contentViewController = NSHostingController(rootView: view)
            window.center()
            wizardWindow = window
        }
        // A menu-bar agent (.accessory) must become .regular to own a focusable window.
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
        wizardWindow?.makeKeyAndOrderFront(nil)
    }

    private func closeWizard() {
        wizardWindow?.close()
        // Back to a pure menu-bar agent.
        NSApp.setActivationPolicy(.accessory)
    }
}

extension Notification.Name {
    /// Posted by the menu's "Run setup again…" to re-open the onboarding wizard.
    static let openWizard = Notification.Name("MistOpenWizard")
}
