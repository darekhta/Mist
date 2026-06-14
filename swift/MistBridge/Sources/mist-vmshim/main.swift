// mist-vmshim — minimal reference VM supervisor for Mist development and e2e tests.
//
// Boots a Linux VM via Virtualization.framework (EFI + virtio-blk + NAT + vsock + serial
// console on stdio) and attaches MistBridge so mist-hostd can reach the guest's vsock.
//
//   mist-vmshim --disk debian.raw --bridge-sock /tmp/dev.sock [--cpus 4] [--memory 4]
//               [--efi-store nvram.bin] [--console]
//
// Requires the com.apple.security.virtualization entitlement (scripts/build-vmshim.sh signs it).

import Foundation
import MistBridge
import Virtualization

struct Options {
    var disks: [String] = []
    var bridgeSock: String = "/tmp/mist-vm.sock"
    var cpus: Int = 4
    var memoryGiB: UInt64 = 4
    var efiStore: String = ""
    var console: Bool = false
    var macAddress: String = ""
    var selftestPort: UInt32 = 0
}

func parseArgs() -> Options {
    var o = Options()
    var it = CommandLine.arguments.dropFirst().makeIterator()
    func next(_ flag: String) -> String {
        guard let v = it.next() else { fatalError("missing value for \(flag)") }
        return v
    }
    while let a = it.next() {
        switch a {
        case "--disk": o.disks.append(next(a))
        case "--bridge-sock": o.bridgeSock = next(a)
        case "--cpus": o.cpus = Int(next(a)) ?? 4
        case "--memory": o.memoryGiB = UInt64(next(a)) ?? 4
        case "--efi-store": o.efiStore = next(a)
        case "--console": o.console = true
        case "--mac": o.macAddress = next(a)
        case "--selftest-vsock": o.selftestPort = UInt32(next(a)) ?? 0
        default:
            FileHandle.standardError.write(Data("unknown arg \(a)\n".utf8))
            exit(2)
        }
    }
    if o.disks.isEmpty {
        FileHandle.standardError.write(Data("usage: mist-vmshim --disk IMG [--bridge-sock P] [--cpus N] [--memory GiB] [--efi-store P] [--console]\n".utf8))
        exit(2)
    }
    if o.efiStore.isEmpty { o.efiStore = o.disks[0] + ".nvram" }
    return o
}

let opts = parseArgs()
let vmQueue = DispatchQueue(label: "mist.vmshim.vm")

func makeConfig() throws -> VZVirtualMachineConfiguration {
    let cfg = VZVirtualMachineConfiguration()
    cfg.cpuCount = min(max(opts.cpus, 1), VZVirtualMachineConfiguration.maximumAllowedCPUCount)
    cfg.memorySize = min(
        max(opts.memoryGiB * 1024 * 1024 * 1024, VZVirtualMachineConfiguration.minimumAllowedMemorySize),
        VZVirtualMachineConfiguration.maximumAllowedMemorySize
    )

    // EFI boot.
    let boot = VZEFIBootLoader()
    let storeURL = URL(fileURLWithPath: opts.efiStore)
    if FileManager.default.fileExists(atPath: opts.efiStore) {
        boot.variableStore = VZEFIVariableStore(url: storeURL)
    } else {
        boot.variableStore = try VZEFIVariableStore(creatingVariableStoreAt: storeURL)
    }
    cfg.bootLoader = boot

    // Disks (in order; .iso images attach read-only).
    cfg.storageDevices = try opts.disks.map { path in
        let ro = path.hasSuffix(".iso")
        let att = try VZDiskImageStorageDeviceAttachment(
            url: URL(fileURLWithPath: path),
            readOnly: ro,
            cachingMode: .automatic,
            synchronizationMode: .fsync
        )
        return VZVirtioBlockDeviceConfiguration(attachment: att)
    }

    // NAT network.
    let net = VZVirtioNetworkDeviceConfiguration()
    net.attachment = VZNATNetworkDeviceAttachment()
    if !opts.macAddress.isEmpty, let mac = VZMACAddress(string: opts.macAddress) {
        net.macAddress = mac
    }
    cfg.networkDevices = [net]

    // vsock + entropy + balloon.
    cfg.socketDevices = [VZVirtioSocketDeviceConfiguration()]
    cfg.entropyDevices = [VZVirtioEntropyDeviceConfiguration()]
    cfg.memoryBalloonDevices = [VZVirtioTraditionalMemoryBalloonDeviceConfiguration()]

    // Serial console on stdio (always attached; interactive only with --console).
    let serial = VZVirtioConsoleDeviceSerialPortConfiguration()
    serial.attachment = VZFileHandleSerialPortAttachment(
        fileHandleForReading: FileHandle.standardInput,
        fileHandleForWriting: FileHandle.standardOutput
    )
    cfg.serialPorts = [serial]

    try cfg.validate()
    return cfg
}

func selfTestConnect(_ device: VZVirtioSocketDevice, port: UInt32, after seconds: Int) {
    vmQueue.asyncAfter(deadline: .now() + .seconds(seconds)) {
        FileHandle.standardError.write(Data("selftest: connecting to guest vsock port \(port)\n".utf8))
        device.connect(toPort: port) { result in
            switch result {
            case .success(let c):
                FileHandle.standardError.write(Data("selftest: OK fd=\(c.fileDescriptor) port=\(c.destinationPort)\n".utf8))
                c.close()
            case .failure(let e):
                let ns = e as NSError
                FileHandle.standardError.write(Data("selftest: FAIL domain=\(ns.domain) code=\(ns.code) desc=\(ns.localizedDescription)\n".utf8))
            }
        }
    }
}

do {
    let cfg = try makeConfig()
    let vm = VZVirtualMachine(configuration: cfg, queue: vmQueue)

    vmQueue.async {
        vm.start { result in
            switch result {
            case .success:
                FileHandle.standardError.write(Data("mist-vmshim: VM running\n".utf8))
                guard let sock = vm.socketDevices.first as? VZVirtioSocketDevice else {
                    FileHandle.standardError.write(Data("mist-vmshim: no vsock device!\n".utf8))
                    exit(1)
                }
                do {
                    try MistBridge.attach(device: sock, socketPath: opts.bridgeSock, vmQueue: vmQueue)
                } catch {
                    FileHandle.standardError.write(Data("mist-vmshim: bridge failed: \(error)\n".utf8))
                    exit(1)
                }
                if opts.selftestPort != 0 {
                    selfTestConnect(sock, port: opts.selftestPort, after: 8)
                }
            case .failure(let error):
                FileHandle.standardError.write(Data("mist-vmshim: start failed: \(error)\n".utf8))
                exit(1)
            }
        }
    }

    signal(SIGINT, SIG_IGN)
    let sigint = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
    sigint.setEventHandler {
        FileHandle.standardError.write(Data("mist-vmshim: stopping\n".utf8))
        vmQueue.async {
            if vm.canStop {
                vm.stop { _ in exit(0) }
            } else {
                exit(0)
            }
        }
    }
    sigint.resume()

    dispatchMain()
} catch {
    FileHandle.standardError.write(Data("mist-vmshim: \(error)\n".utf8))
    exit(1)
}
