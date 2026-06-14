// ControlClient — the only thing Swift does on the wire: speak mist-hostd's control protocol
// (newline-delimited JSON over a unix socket). No Mist logic lives here (ADR-14); every verb is
// resolved, paired, and mounted by Rust in hostd. This file just frames requests and decodes
// replies so the SwiftUI layer can render them.

import Foundation

/// Errors surfaced to the UI. All are non-fatal — the menu shows the message and offers a retry.
public enum ControlError: Error, CustomStringConvertible {
    case connect(String)
    case io(String)
    case daemon(String)

    public var description: String {
        switch self {
        case .connect(let s): return "cannot reach mist-hostd: \(s)"
        case .io(let s): return "control I/O: \(s)"
        case .daemon(let s): return s
        }
    }
}

/// A blocking unix-socket JSON client. The app calls these off the main actor (see AppModel).
/// Immutable (just a path) → `Sendable`, so it can cross into the events-follow thread.
public struct ControlClient: Sendable {
    public let socketPath: String

    public init(socketPath: String? = nil) {
        if let p = socketPath {
            self.socketPath = p
        } else if let dir = ProcessInfo.processInfo.environment["MIST_STATE_DIR"] {
            self.socketPath = (dir as NSString).appendingPathComponent("control.sock")
        } else {
            let home = NSHomeDirectory()
            self.socketPath = "\(home)/Library/Application Support/Mist/control.sock"
        }
    }

    /// Send one request object and return the single JSON reply. Throws `.daemon` when the reply
    /// has `ok == false`, mirroring the `mist` CLI's behavior.
    public func request(_ obj: [String: Any]) throws -> [String: Any] {
        let fd = try openSocket()
        defer { close(fd) }
        try writeLine(fd, json: obj)
        let line = try readLine(fd)
        guard
            let data = line.data(using: .utf8),
            let reply = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            throw ControlError.io("malformed reply")
        }
        if (reply["ok"] as? Bool) != true {
            throw ControlError.daemon((reply["error"] as? String) ?? "unknown daemon error")
        }
        return reply
    }

    /// Open + connect a `AF_UNIX` stream socket to the control path.
    private func openSocket() throws -> Int32 {
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw ControlError.connect("socket() failed") }

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = Array(socketPath.utf8)
        guard pathBytes.count < MemoryLayout.size(ofValue: addr.sun_path) else {
            close(fd)
            throw ControlError.connect("socket path too long")
        }
        withUnsafeMutablePointer(to: &addr.sun_path) { ptr in
            ptr.withMemoryRebound(to: CChar.self, capacity: pathBytes.count + 1) { c in
                for (i, b) in pathBytes.enumerated() { c[i] = CChar(bitPattern: b) }
                c[pathBytes.count] = 0
            }
        }
        let len = socklen_t(MemoryLayout<sockaddr_un>.size)
        let rc = withUnsafePointer(to: &addr) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { connect(fd, $0, len) }
        }
        guard rc == 0 else {
            close(fd)
            throw ControlError.connect("\(socketPath) — is mist-hostd running?")
        }
        return fd
    }

    private func writeLine(_ fd: Int32, json: [String: Any]) throws {
        var data = try JSONSerialization.data(withJSONObject: json)
        data.append(0x0a)
        try data.withUnsafeBytes { raw in
            var off = 0
            let base = raw.bindMemory(to: UInt8.self).baseAddress!
            while off < data.count {
                let n = write(fd, base + off, data.count - off)
                if n <= 0 { throw ControlError.io("write failed") }
                off += n
            }
        }
    }

    /// Read bytes up to the first newline (control replies are one line each).
    private func readLine(_ fd: Int32) throws -> String {
        var out = [UInt8]()
        var byte: UInt8 = 0
        while true {
            let n = read(fd, &byte, 1)
            if n == 0 { break }
            if n < 0 { throw ControlError.io("read failed") }
            if byte == 0x0a { break }
            out.append(byte)
        }
        return String(decoding: out, as: UTF8.self)
    }
}
