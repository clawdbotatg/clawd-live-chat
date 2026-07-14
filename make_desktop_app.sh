#!/bin/bash
# make_desktop_app.sh — build "Clawd Live.app" on the Desktop.
#
# A double-clickable launcher in the family of Harness.app / Clawd Scribe.app:
# if nothing answers on :8790 it starts server.py itself (nohup, logs to
# ~/Library/Logs/clawd-live-chat.log), waits until it's healthy, then opens a
# standalone Chrome --app window on http://localhost:8790 (localhost, not
# 127.0.0.1 — the mic needs a secure context). Icon is generated locally:
# 🦞 on the UI's dark navy + a green "live" dot, rendered by a throwaway
# Swift script → iconutil. Rerun any time; it rebuilds in place.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
APP="$HOME/Desktop/Clawd Live.app"
NAME="ClawdLive"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ── icon ────────────────────────────────────────────────────────────────────
cat > "$TMP/icon.swift" <<'SWIFT'
import AppKit

let outDir = CommandLine.arguments[1]
// pixel size -> iconset filename(s)
let plan: [(Int, [String])] = [
    (16,   ["icon_16x16.png"]),
    (32,   ["icon_16x16@2x.png", "icon_32x32.png"]),
    (64,   ["icon_32x32@2x.png"]),
    (128,  ["icon_128x128.png"]),
    (256,  ["icon_128x128@2x.png", "icon_256x256.png"]),
    (512,  ["icon_256x256@2x.png", "icon_512x512.png"]),
    (1024, ["icon_512x512@2x.png"]),
]
for (px, names) in plan {
    let s = CGFloat(px)
    let rep = NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: px,
        pixelsHigh: px, bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true,
        isPlanar: false, colorSpaceName: .deviceRGB, bytesPerRow: 0,
        bitsPerPixel: 0)!
    NSGraphicsContext.saveGraphicsState()
    NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: rep)
    // dark navy rounded square (matches the app UI background)
    let inset = NSRect(x: 0, y: 0, width: s, height: s)
        .insetBy(dx: s * 0.045, dy: s * 0.045)
    let bg = NSBezierPath(roundedRect: inset, xRadius: s * 0.21, yRadius: s * 0.21)
    NSColor(calibratedRed: 0.063, green: 0.078, blue: 0.122, alpha: 1).setFill()
    bg.fill()
    NSColor(calibratedRed: 0.16, green: 0.20, blue: 0.31, alpha: 1).setStroke()
    bg.lineWidth = max(1, s * 0.01)
    bg.stroke()
    // the lobster
    let glyph = "🦞" as NSString
    let font = NSFont.systemFont(ofSize: s * 0.56)
    let attrs: [NSAttributedString.Key: Any] = [.font: font]
    let gs = glyph.size(withAttributes: attrs)
    glyph.draw(at: NSPoint(x: (s - gs.width) / 2, y: (s - gs.height) / 2 + s * 0.02),
               withAttributes: attrs)
    // green "live" dot, bottom right
    let r = s * 0.085
    let dot = NSBezierPath(ovalIn: NSRect(x: s * 0.66, y: s * 0.15,
                                          width: r * 2, height: r * 2))
    NSColor(calibratedRed: 0.31, green: 0.91, blue: 0.53, alpha: 1).setFill()
    dot.fill()
    NSGraphicsContext.restoreGraphicsState()
    let png = rep.representation(using: .png, properties: [:])!
    for n in names {
        try! png.write(to: URL(fileURLWithPath: outDir + "/" + n))
    }
}
SWIFT
mkdir -p "$TMP/ClawdLive.iconset"
swift "$TMP/icon.swift" "$TMP/ClawdLive.iconset"
iconutil -c icns "$TMP/ClawdLive.iconset" -o "$TMP/ClawdLive.icns"

# ── bundle ──────────────────────────────────────────────────────────────────
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "$TMP/ClawdLive.icns" "$APP/Contents/Resources/ClawdLive.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleName</key>
	<string>Clawd Live</string>
	<key>CFBundleDisplayName</key>
	<string>Clawd Live</string>
	<key>CFBundleIdentifier</key>
	<string>com.austingriffith.clawdlive</string>
	<key>CFBundleVersion</key>
	<string>1.0</string>
	<key>CFBundleShortVersionString</key>
	<string>1.0</string>
	<key>CFBundleInfoDictionaryVersion</key>
	<string>6.0</string>
	<key>CFBundlePackageType</key>
	<string>APPL</string>
	<key>CFBundleExecutable</key>
	<string>${NAME}</string>
	<key>CFBundleIconFile</key>
	<string>ClawdLive</string>
	<key>LSUIElement</key>
	<true/>
	<key>LSMinimumSystemVersion</key>
	<string>10.13</string>
</dict>
</plist>
PLIST

cat > "$APP/Contents/MacOS/$NAME" <<'LAUNCHER'
#!/bin/bash
# Clawd Live launcher — start the live-chat server if it's down, then open
# the app window. (Clawd Scribe pattern: the server is NOT a daemon; whoever
# clicks first after a reboot boots it, and it stays up afterwards.)
PROJECT="$HOME/clawd/clawd-harness/projects/clawd-live-chat"
PORT=8790
URL="http://localhost:$PORT"   # localhost, not 127.0.0.1 — mic needs a secure context
LOG="$HOME/Library/Logs/clawd-live-chat.log"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
USERDATA="$HOME/Library/Application Support/Google/Chrome"

mkdir -p "$(dirname "$LOG")"

if ! curl -s -o /dev/null --max-time 1 "$URL"; then
  cd "$PROJECT" || exit 1
  PY="/opt/homebrew/bin/python3"
  [ -x "$PY" ] || PY="/usr/bin/python3"
  # Detached subshell (Backchannel pattern): reparent to launchd so the server
  # survives this short-lived LSUIElement launcher exiting.
  ( nohup "$PY" server.py >> "$LOG" 2>&1 & )
  for i in $(seq 1 30); do
    curl -s -o /dev/null --max-time 1 "$URL" && break
    sleep 0.5
  done
fi

if [ -x "$CHROME" ]; then
  # Standalone chromeless window on the main (blue) profile. arch -arm64:
  # don't let a translated launcher drag Chrome under Rosetta (Harness.app fix).
  ( nohup arch -arm64 "$CHROME" --user-data-dir="$USERDATA" \
      --profile-directory="Default" --app="$URL" >/dev/null 2>&1 & )
else
  open "$URL"
fi
LAUNCHER
chmod +x "$APP/Contents/MacOS/$NAME"

codesign --force -s - "$APP" 2>/dev/null || true
touch "$APP"   # nudge Finder/LaunchServices to pick up the icon
echo "built: $APP"
