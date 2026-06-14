// The push channel (design 11 §7 "Push, not poll"): hostd's `events --follow` streams the journal
// as JSON lines. This is *intent*; the app reconciles it against kernel reality (DiskArbitration).

import Foundation

public extension ControlClient {
    /// Connect, request the follow stream, and invoke `onLine` for each event line until the
    /// connection drops or `shouldStop()` returns true. Blocking — call on a background queue.
    func followEvents(onLine: @escaping (String) -> Void, shouldStop: @escaping () -> Bool) {
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { return }
        defer { close(fd) }

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let pathBytes = Array(socketPath.utf8)
        guard pathBytes.count < MemoryLayout.size(ofValue: addr.sun_path) else { return }
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
        guard rc == 0 else { return }

        let req = #"{"cmd":"events","follow":true}"# + "\n"
        _ = req.withCString { write(fd, $0, strlen($0)) }

        var buffer = [UInt8]()
        var chunk = [UInt8](repeating: 0, count: 4096)
        while !shouldStop() {
            let n = read(fd, &chunk, chunk.count)
            if n <= 0 { break }
            buffer.append(contentsOf: chunk[0..<n])
            while let nl = buffer.firstIndex(of: 0x0a) {
                let line = String(decoding: buffer[0..<nl], as: UTF8.self)
                buffer.removeSubrange(0...nl)
                if !line.isEmpty { onLine(line) }
            }
        }
    }
}
