// swift-tools-version: 5.9
import PackageDescription

// Mist.app — a thin SwiftUI MenuBarExtra client over mist-hostd's control UDS, plus the tiny root
// mount-helper. Swift holds NO Mist logic (ADR-14): discovery, pairing, and mounting are Rust in
// hostd; the app triggers control verbs and renders results. See design/11-onboarding.md §7–§8.
let package = Package(
    name: "MistApp",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "Mist", targets: ["MistApp"]),
        .executable(name: "mist-mount-helper", targets: ["MistMountHelper"]),
        .library(name: "MistControl", targets: ["MistControl"]),
    ],
    dependencies: [
        // Sparkle 2 auto-update (EdDSA appcast over HTTPS). build-app.sh embeds + signs the framework.
        .package(url: "https://github.com/sparkle-project/Sparkle", from: "2.6.0"),
    ],
    targets: [
        // Logic-free transport + DTOs shared by the app and helper.
        .target(name: "MistControl"),
        .target(name: "MistHelperProtocol"),
        .executableTarget(
            name: "MistApp",
            dependencies: [
                "MistControl", "MistHelperProtocol",
                .product(name: "Sparkle", package: "Sparkle"),
            ]
        ),
        .executableTarget(
            name: "MistMountHelper",
            dependencies: ["MistHelperProtocol"]
        ),
        .testTarget(name: "MistControlTests", dependencies: ["MistControl"]),
    ]
)
