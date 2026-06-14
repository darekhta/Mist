// Sparkle 2 auto-update. Reads SUFeedURL + SUPublicEDKey from Info.plist (set at release); checks
// for updates at launch and on demand. On update, the app re-registers its SMAppService entries at
// next launch (version-aware, see HelperClient) so the new hostd binary runs — mounts survive
// because hostd adopts surviving kernel mounts on restart.
//
// Guarded by canImport(Sparkle) so the package still builds if the SPM dependency isn't resolved
// (e.g. offline); the signed release build embeds + signs Sparkle.framework via build-app.sh.

import Foundation

#if canImport(Sparkle)
import Sparkle

@MainActor
final class Updater {
    static let shared = Updater()
    private let controller: SPUStandardUpdaterController

    private init() {
        controller = SPUStandardUpdaterController(
            startingUpdater: true, updaterDelegate: nil, userDriverDelegate: nil)
        NotificationCenter.default.addObserver(
            forName: .mistCheckForUpdates, object: nil, queue: .main
        ) { _ in
            Task { @MainActor in Updater.shared.checkForUpdates() }
        }
    }

    /// Touch to instantiate (start the updater + register the observer).
    func start() {}

    func checkForUpdates() { controller.updater.checkForUpdates() }
}

#else

/// No-Sparkle fallback (offline dev builds): the "Check for Updates" action is a no-op.
@MainActor
final class Updater {
    static let shared = Updater()
    private init() {}
    func start() {}
    func checkForUpdates() {}
}

#endif
