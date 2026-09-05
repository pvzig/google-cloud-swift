// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "GoogleCloudSwift",
    platforms: [
        .macOS(.v15)
    ],
    products: [
        .library(name: "Auth", targets: ["Auth"]),
        .library(name: "PubSub", targets: ["PubSub"]),
        .executable(name: "PubSubTestbed", targets: ["PubSubTestbed"]),
    ],
    dependencies: [
        .package(url: "https://github.com/apple/swift-protobuf.git", from: "1.38.1"),
        .package(url: "https://github.com/grpc/grpc-swift-2.git", from: "2.4.2"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "2.4.1"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.9.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.15.0"),
        .package(url: "https://github.com/vapor/jwt-kit.git", from: "5.6.0"),
        // Token and metadata-server calls run on NIO rather than URLSession:
        // corelibs-foundation's URLSession is the weakest link on the platform this
        // client mostly targets, and NIO is already in the resolved graph.
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.36.0"),
    ],
    targets: [
        .target(
            name: "Auth",
            dependencies: [
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "JWTKit", package: "jwt-kit"),
                .product(name: "Logging", package: "swift-log"),
            ]
        ),
        .target(
            name: "PubSub",
            dependencies: [
                "Auth",
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
                .product(name: "Logging", package: "swift-log"),
                .product(name: "SwiftProtobuf", package: "swift-protobuf"),
            ]
        ),
        .testTarget(
            name: "AuthTests",
            dependencies: [
                "Auth",
                .product(name: "GRPCCore", package: "grpc-swift-2"),
            ]
        ),
        .testTarget(
            name: "PubSubTests",
            dependencies: [
                "Auth",
                "PubSub",
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ]
        ),
        .executableTarget(
            name: "PubSubTestbed",
            dependencies: ["PubSub"]
        ),
    ]
)
