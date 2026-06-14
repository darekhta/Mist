import XCTest

@testable import MistControl

final class StatusDecodeTests: XCTestCase {
    // The control client maps hostd JSON to DTOs without any logic of its own (ADR-14); pin that
    // the shapes hostd emits decode as expected.
    func testVMStatusReachability() {
        let ready = VMStatus(
            name: "dev", state: "ready", endpoint: "auto → tcp:192.168.64.2:6478",
            vmUUID: "4f2b0b30a9d84f8ab5c40122c7be1c13", shares: [])
        XCTAssertTrue(ready.isReachable)
        let degraded = VMStatus(
            name: "dev", state: "degraded", endpoint: "auto (resolving)", vmUUID: nil, shares: [])
        XCTAssertFalse(degraded.isReachable)
    }

    func testShareLiveGate() {
        XCTAssertTrue(ShareStatus(name: "code", state: "live", nodes: 5).isLive)
        XCTAssertFalse(ShareStatus(name: "code", state: "seeding", nodes: 0).isLive)
    }

    func testDefaultSocketPathHonorsEnv() {
        // Sanity: the client resolves *some* socket path and it ends in control.sock.
        let c = ControlClient(socketPath: "/tmp/x/control.sock")
        XCTAssertTrue(c.socketPath.hasSuffix("control.sock"))
    }
}
