// MistBridge — expose a VM's vsock as a unix socket using the Firecracker convention.
//
// The Virtualization framework only allows the process that owns the VZVirtualMachine to open
// vsock connections. MistBridge is the small shim a VM supervisor embeds so that mist-hostd
// (a separate daemon) can reach the guest: hostd connects to the unix socket, sends
// "CONNECT <port>\n", and on "OK <port>\n" the stream becomes a raw byte pipe to the guest's
// vsock listener. No Mist protocol knowledge lives here.
//
// The data pump is fully event-driven (DispatchSource → kqueue): no per-connection threads and
// no per-connect semaphore. Each direction is a non-blocking copy with backpressure (a write
// source drains buffered bytes when the destination is full) and half-close semantics (one
// direction's EOF shuts down the peer's write side without tearing down the other direction).

import Foundation
import Virtualization

public final class MistBridge {
    private let socketPath: String
    private let vmQueue: DispatchQueue
    private weak var device: VZVirtioSocketDevice?
    private var listenFD: Int32 = -1
    private let acceptQueue = DispatchQueue(label: "mist.bridge.accept")

    /// Start bridging `device` (the VM's virtio socket device) at `socketPath`.
    /// `vmQueue` must be the queue the VZVirtualMachine was created with.
    @discardableResult
    public static func attach(
        device: VZVirtioSocketDevice,
        socketPath: String,
        vmQueue: DispatchQueue
    ) throws -> MistBridge {
        let bridge = MistBridge(device: device, socketPath: socketPath, vmQueue: vmQueue)
        try bridge.start()
        return bridge
    }

    private init(device: VZVirtioSocketDevice, socketPath: String, vmQueue: DispatchQueue) {
        self.device = device
        self.socketPath = socketPath
        self.vmQueue = vmQueue
    }

    private func start() throws {
        unlink(socketPath)
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw Errno("socket") }

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = socketPath.utf8CString
        guard pathBytes.count <= MemoryLayout.size(ofValue: addr.sun_path) else {
            close(fd)
            throw Errno("socket path too long", code: ENAMETOOLONG)
        }
        withUnsafeMutableBytes(of: &addr.sun_path) { dst in
            pathBytes.withUnsafeBytes { src in
                dst.copyMemory(from: UnsafeRawBufferPointer(rebasing: src.prefix(dst.count)))
            }
        }
        let len = socklen_t(MemoryLayout<sockaddr_un>.size)
        let bound = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { bind(fd, $0, len) }
        }
        guard bound == 0 else {
            close(fd)
            throw Errno("bind \(socketPath)")
        }
        chmod(socketPath, 0o600)
        guard listen(fd, 64) == 0 else {
            close(fd)
            throw Errno("listen")
        }
        listenFD = fd

        acceptQueue.async { [weak self] in self?.acceptLoop(fd) }
        FileHandle.standardError.write(Data("MistBridge: \(socketPath) ready\n".utf8))
    }

    public func stop() {
        if listenFD >= 0 { close(listenFD) }
        listenFD = -1
        unlink(socketPath)
    }

    private func acceptLoop(_ fd: Int32) {
        while true {
            let conn = accept(fd, nil, nil)
            if conn < 0 {
                if errno == EINTR { continue }
                return // listener closed
            }
            DispatchQueue.global(qos: .userInitiated).async { [weak self] in
                self?.handle(conn)
            }
        }
    }

    private func handle(_ conn: Int32) {
        // Read the "CONNECT <port>\n" line, byte by byte (must not over-read — the rest is MWP).
        var line = [UInt8]()
        while line.count < 64 {
            var b: UInt8 = 0
            let n = read(conn, &b, 1)
            if n != 1 { close(conn); return }
            if b == 0x0A { break }
            line.append(b)
        }
        guard let text = String(bytes: line, encoding: .utf8),
              text.hasPrefix("CONNECT "),
              let port = UInt32(text.dropFirst("CONNECT ".count).trimmingCharacters(in: .whitespaces))
        else {
            _ = "ERR\n".withCString { write(conn, $0, 4) }
            close(conn)
            return
        }

        guard let device = self.device else { close(conn); return }
        // Async connect: no semaphore, no blocked worker thread. The completion runs on vmQueue.
        // AVF's connect can fail to ever complete while the guest's vsock is half-up (early boot,
        // crashed peer) — a watchdog replies ERR after 8 s so the dialer can retry instead of
        // wedging forever. `settled` arbitrates the watchdog vs. the (possibly late) completion.
        let settled = AtomicFlag()
        DispatchQueue.global().asyncAfter(deadline: .now() + 8) {
            if settled.trySet() {
                let msg = "ERR connect \(port): timeout\n"
                _ = msg.withCString { write(conn, $0, msg.utf8.count) }
                close(conn)
            }
        }
        vmQueue.async {
            device.connect(toPort: port) { result in
                switch result {
                case .success(let vc):
                    guard settled.trySet() else {
                        vc.close() // watchdog already failed this dial; drop the late socket
                        return
                    }
                    let ok = "OK \(port)\n"
                    if ok.withCString({ write(conn, $0, ok.utf8.count) }) != ok.utf8.count {
                        vc.close()
                        close(conn)
                        return
                    }
                    BridgeConnection.start(uds: conn, vc: vc)
                case .failure(let e):
                    guard settled.trySet() else { return }
                    let ns = e as NSError
                    FileHandle.standardError.write(
                        Data("MistBridge: connect \(port) failed: \(ns.domain)/\(ns.code)\n".utf8))
                    let msg = "ERR connect \(port): \(ns.localizedDescription)\n"
                    _ = msg.withCString { write(conn, $0, msg.utf8.count) }
                    close(conn)
                }
            }
        }
    }
}

/// First-caller-wins flag (connect watchdog vs. completion).
private final class AtomicFlag {
    private let lock = NSLock()
    private var fired = false
    /// Returns true exactly once — for whichever side calls first.
    func trySet() -> Bool {
        lock.lock()
        defer { lock.unlock() }
        if fired { return false }
        fired = true
        return true
    }
}

// MARK: - Event-driven bidirectional pump

/// Coordinates the two directional pumps for one bridged connection and owns the fds + the
/// retained vsock connection. Retained via a process-wide registry for its lifetime.
final class BridgeConnection {
    private let uds: Int32
    private let vc: VZVirtioSocketConnection
    private let vfd: Int32
    private let queue: DispatchQueue
    private var pumps: [Pump] = []
    private var torn = false

    private static let lock = NSLock()
    private static var active: [ObjectIdentifier: BridgeConnection] = [:]

    static func start(uds: Int32, vc: VZVirtioSocketConnection) {
        let c = BridgeConnection(uds: uds, vc: vc)
        lock.lock(); active[ObjectIdentifier(c)] = c; lock.unlock()
        c.begin()
    }

    private init(uds: Int32, vc: VZVirtioSocketConnection) {
        self.uds = uds
        self.vc = vc
        self.vfd = vc.fileDescriptor
        self.queue = DispatchQueue(label: "mist.bridge.conn")
        setNonBlocking(uds)
        setNonBlocking(vfd)
    }

    private func begin() {
        let ab = Pump(src: uds, dst: vfd, queue: queue) { [weak self] hardError in
            self?.pumpDone(hardError: hardError)
        }
        let ba = Pump(src: vfd, dst: uds, queue: queue) { [weak self] hardError in
            self?.pumpDone(hardError: hardError)
        }
        pumps = [ab, ba]
        ab.start()
        ba.start()
    }

    /// A pump finished (EOF after flushing, or hard error): tear the whole connection down.
    ///
    /// No waiting for the second direction's EOF: AVF does not propagate half-close to the
    /// guest, and MWP lanes where the guest only writes (journal, bulk) would never close their
    /// side — every such connection would leak its VZVirtioSocketConnection until the device's
    /// connection slots run out and all future connects hang (exactly what a killed-and-
    /// restarted hostd used to hit). MWP never uses half-close legitimately: one dead direction
    /// means the lane is dead.
    private func pumpDone(hardError: Bool) {
        // Always invoked on `queue` (serial), so no extra locking needed for these fields.
        _ = hardError
        if torn { return }
        teardown()
    }

    private func teardown() {
        if torn { return }
        torn = true
        for p in pumps { p.cancel() }
        pumps.removeAll()
        vc.close() // closes vfd
        close(uds)
        BridgeConnection.lock.lock()
        BridgeConnection.active[ObjectIdentifier(self)] = nil
        BridgeConnection.lock.unlock()
    }
}

/// One direction of the copy: non-blocking read on `src` → write on `dst`, with a write source
/// that drains buffered bytes under backpressure. All handlers run on the shared serial `queue`.
private final class Pump {
    private let src: Int32
    private let dst: Int32
    private let queue: DispatchQueue
    private let onDone: (_ hardError: Bool) -> Void

    private var readSource: DispatchSourceRead!
    private var writeSource: DispatchSourceWrite?
    private var readSuspended = false
    private var writeActive = false
    private var finished = false

    private let bufSize = 256 * 1024
    private var pending = [UInt8]()
    private var pendingOff = 0
    private var sawEof = false

    init(src: Int32, dst: Int32, queue: DispatchQueue, onDone: @escaping (Bool) -> Void) {
        self.src = src
        self.dst = dst
        self.queue = queue
        self.onDone = onDone
    }

    func start() {
        readSource = DispatchSource.makeReadSource(fileDescriptor: src, queue: queue)
        readSource.setEventHandler { [weak self] in self?.onReadable() }
        readSource.resume()
    }

    /// Cancel both sources (resuming any suspended one first so the cancel is delivered).
    func cancel() {
        finished = true
        if readSuspended { readSource.resume(); readSuspended = false }
        readSource?.cancel()
        if let ws = writeSource {
            if !writeActive { ws.resume() }
            ws.cancel()
            writeActive = false
        }
    }

    private func onReadable() {
        guard !finished, !readSuspended else { return }
        var buf = [UInt8](repeating: 0, count: bufSize)
        let n = buf.withUnsafeMutableBytes { read(src, $0.baseAddress, bufSize) }
        if n == 0 {
            handleEof()
            return
        }
        if n < 0 {
            let e = errno
            if e == EAGAIN || e == EINTR { return }
            fail()
            return
        }
        // Write what we read; buffer the remainder under backpressure.
        let written = writeChunk(buf, 0, n)
        if written < 0 { return } // fail() already called
        if written < n {
            pending = Array(buf[written..<n])
            pendingOff = 0
            suspendRead()
            ensureWriteSource()
        }
    }

    /// Write `buf[off..<end]` to dst; returns bytes written (0..<count), or -1 after a hard error.
    private func writeChunk(_ buf: [UInt8], _ off: Int, _ end: Int) -> Int {
        var o = off
        while o < end {
            let w = buf.withUnsafeBytes { ptr -> Int in
                write(dst, ptr.baseAddress!.advanced(by: o), end - o)
            }
            if w > 0 { o += w; continue }
            if w < 0 {
                let e = errno
                if e == EAGAIN { break }
                if e == EINTR { continue }
                fail()
                return -1
            }
            break // w == 0: treat as would-block
        }
        return o - off
    }

    private func ensureWriteSource() {
        if writeSource == nil {
            let ws = DispatchSource.makeWriteSource(fileDescriptor: dst, queue: queue)
            ws.setEventHandler { [weak self] in self?.onWritable() }
            writeSource = ws
        }
        if !writeActive {
            writeSource!.resume()
            writeActive = true
        }
    }

    private func onWritable() {
        guard !finished else { return }
        while pendingOff < pending.count {
            let w = pending.withUnsafeBytes { ptr -> Int in
                write(dst, ptr.baseAddress!.advanced(by: pendingOff), pending.count - pendingOff)
            }
            if w > 0 { pendingOff += w; continue }
            if w < 0 {
                let e = errno
                if e == EAGAIN { return } // still full; wait for the next writable event
                if e == EINTR { continue }
                fail()
                return
            }
            return
        }
        // Drained. Stop the write source and resume reading; if EOF was deferred, finish it now.
        pending.removeAll(keepingCapacity: true)
        pendingOff = 0
        if writeActive {
            writeSource?.suspend()
            writeActive = false
        }
        if sawEof {
            shutdown(dst, SHUT_WR)
            finished = true
            onDone(false)
        } else {
            resumeRead()
        }
    }

    private func handleEof() {
        sawEof = true
        // If there's still buffered data for dst, let the write source flush it first.
        if pendingOff < pending.count {
            suspendRead()
            ensureWriteSource()
            return
        }
        shutdown(dst, SHUT_WR)
        finished = true
        onDone(false)
    }

    private func fail() {
        if finished { return }
        finished = true
        onDone(true)
    }

    private func suspendRead() {
        if !readSuspended {
            readSource.suspend()
            readSuspended = true
        }
    }

    private func resumeRead() {
        if readSuspended {
            readSource.resume()
            readSuspended = false
        }
    }
}

private func setNonBlocking(_ fd: Int32) {
    let flags = fcntl(fd, F_GETFL, 0)
    if flags >= 0 { _ = fcntl(fd, F_SETFL, flags | O_NONBLOCK) }
}

struct Errno: Error, CustomStringConvertible {
    let what: String
    let code: Int32
    init(_ what: String, code: Int32 = errno) {
        self.what = what
        self.code = code
    }
    var description: String { "\(what): \(String(cString: strerror(code)))" }
}
