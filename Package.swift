// swift-tools-version: 6.2
import PackageDescription

// Make sure that when the Swift Package Index builds our documentation,
// we enable BUILDING_DOCS.
import Foundation

var swiftSettings: [SwiftSetting] = []
if ProcessInfo.processInfo.environment["SPI_PROCESSING"] == "1"
  || ProcessInfo.processInfo.environment["BUILDING_DOCS"] == "1"
{
  swiftSettings.append(.define("BUILDING_DOCS"))
}

let package = Package(
  name: "swift-platform-executors",
  platforms: [
    .iOS("26.0"),
    .macOS("26.0"),
    .tvOS("26.0"),
    .watchOS("26.0"),
    .visionOS("26.0"),
  ],
  products: [
    .library(
      name: "PlatformExecutors",
      targets: [
        "PlatformExecutors"
      ]
    )
  ],
  dependencies: [
    .package(url: "https://github.com/apple/swift-docc-plugin", from: "1.0.0"),
    .package(url: "https://github.com/apple/swift-collections", branch: "fb-async" , traits: ["UnstableContainersPreview"]),
  ],
  targets: [
    .target(
      name: "PlatformExecutors",
      dependencies: [
        .target(name: "CPlatformExecutors"),
        .product(name: "ContainersPreview", package: "swift-collections"),
        .product(name: "BasicContainers", package: "swift-collections"),
      ],
      swiftSettings: swiftSettings + [
        .enableExperimentalFeature("Lifetimes"),
        .enableExperimentalFeature("LifetimeDependence"),
        .enableUpcomingFeature("LifetimeDependence"),
        .enableExperimentalFeature("BuiltinModule"),
        .enableUpcomingFeature("NonisolatedNonsendingByDefault"),
        // Required so Span+AsyncOverloads.swift can reach `_pointer`/`_count`
        // on `Span`/`MutableSpan`/`OutputSpan` to define async-throws
        // overloads of `withUnsafeBufferPointer` etc. — these are missing
        // upstream and we need them for the typed-method `IOExecutor` shape
        // (closure-form pooled reads, caller-owned reads with awaitable
        // syscalls). Pre-1.0 hack; remove once the stdlib ships them.
        .unsafeFlags(["-Xfrontend", "-disable-access-control"]),
      ]
    ),
    .target(
      name: "CPlatformExecutors",
      cSettings: [
        .define("_GNU_SOURCE")
      ]
    ),

    // Tests
    .testTarget(
      name: "PlatformExecutorsTests",
      dependencies: [
        .target(name: "PlatformExecutors")
      ]
    ),

    // Examples
    .executableTarget(
      name: "PlatformExecutorsExample",
      dependencies: [
        .target(name: "PlatformExecutors")
      ],
      path: "Examples/PlatformExecutors"
    ),
  ]
)
