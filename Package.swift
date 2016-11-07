import PackageDescription

let package = Package(
    name: "GeneratedSwiftServer-CloudantStore",
    dependencies: [
        .Package(url: "https://github.com/IBM-Swift/Kitura-net.git", majorVersion: 1),
        .Package(url: "https://github.com/IBM-Swift/Kitura-CouchDB.git", majorVersion: 1),
        .Package(url: "https://github.com/IBM-Swift/GeneratedSwiftServer.git", majorVersion: 0)
    ]
)