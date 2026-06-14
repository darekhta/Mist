# Homebrew formula (tap: mist-fs/homebrew-mist). `brew install mist-fs/mist/mist`.
class Mist < Formula
  desc "Near-native macOS access to Linux-VM files (loopback NFS + RAM replica)"
  homepage "https://github.com/mist-fs/mist"
  url "https://github.com/mist-fs/mist/archive/refs/tags/v0.1.0.tar.gz"
  sha256 "REPLACED_BY_RELEASE_SCRIPT"
  license "Apache-2.0"

  depends_on "rust" => :build
  depends_on arch: :arm64

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/mist-cli")
    system "cargo", "install", *std_cargo_args(path: "crates/mist-hostd")
  end

  service do
    run [opt_bin/"mist-hostd"]
    keep_alive true
    log_path var/"log/mist-hostd.log"
    error_log_path var/"log/mist-hostd.log"
  end

  test do
    assert_match "mist", shell_output("#{bin}/mist --help")
  end
end
