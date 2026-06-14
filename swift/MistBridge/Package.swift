// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "MistBridge",
    platforms: [.macOS(.v13)],
    products: [
        .library(name: "MistBridge", targets: ["MistBridge"]),
        .executable(name: "mist-vmshim", targets: ["mist-vmshim"]),
    ],
    targets: [
        .target(name: "MistBridge"),
        .executableTarget(name: "mist-vmshim", dependencies: ["MistBridge"]),
    ]
)
