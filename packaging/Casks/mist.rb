# Homebrew Cask wrapping the notarized DMG (design 11 §8). `auto_updates true` so brew defers to
# Sparkle for upgrades; `livecheck` tracks the GitHub appcast. The CLI is vendored inside the bundle
# with an "Install CLI to PATH" menu action — don't brew-compile the SwiftUI app, ship it prebuilt.
# (The source formula in packaging/mist.rb stays for CLI/server-only users.)
cask "mist" do
  version "0.1.0"
  sha256 :no_check # replace with the DMG sha256 at release

  url "https://github.com/darekhta/Mist/releases/download/v#{version}/Mist-#{version}.dmg",
      verified: "github.com/darekhta/Mist/"
  name "Mist"
  desc "Near-native macOS access to Linux VM files"
  homepage "https://github.com/darekhta/Mist"

  auto_updates true
  depends_on macos: ">= :ventura"

  app "Mist.app"

  livecheck do
    url "https://github.com/darekhta/Mist/releases/latest/download/appcast.xml"
    strategy :sparkle
  end

  zap trash: [
    "~/Library/Application Support/Mist",
    "~/Library/Caches/dev.mist.app",
    "~/Library/Preferences/dev.mist.app.plist",
  ]
end
