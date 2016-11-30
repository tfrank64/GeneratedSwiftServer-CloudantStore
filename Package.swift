import PackageDescription

let package = Package(
    name: "GeneratedSwiftServer-CloudantStore",
    dependencies: [
        .Package(url: "https://github.com/IBM-Swift/Kitura-net.git", majorVersion: 1, minor: 1),
        .Package(url: "https://github.com/IBM-Swift/Kitura-CouchDB.git", majorVersion: 1, minor: 1),
        .Package(url: "https://github.com/tfrank64/GeneratedSwiftServer.git", majorVersion: 0, minor: 2)
    ]
)
