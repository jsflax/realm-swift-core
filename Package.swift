// swift-tools-version:5.3

import PackageDescription

let package = Package(
    name: "Realm",
    platforms: [.macOS(.v10_15)],
    products: [
        .library(
            name: "RealmCore",
            targets: ["Core"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-atomics.git", .exact("0.0.1"))
    ],
    targets: [
        .target(
            name: "Core",
            dependencies: [.product(name: "Atomics", package: "swift-atomics")]),
        .testTarget(
            name: "CoreTests",
            dependencies: ["Core"],
            resources: [.process("Resources")]),
    ]
)
