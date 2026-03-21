# LalaCore Launchd Setup

Use these launch agents:

- `com.lalacore.monitor.plist`: minute-level Google Sheets monitor (long-running)
- `com.lalacore.daily-health.plist`: forced daily health sync at 07:00
- `com.lalacore.serial-queue.plist`: serial queue worker (1 question per run)

## Install

```bash
cp /Users/ritamsaha/lalacore_omega/config/launchd/com.lalacore.monitor.plist ~/Library/LaunchAgents/
cp /Users/ritamsaha/lalacore_omega/config/launchd/com.lalacore.daily-health.plist ~/Library/LaunchAgents/
cp /Users/ritamsaha/lalacore_omega/config/launchd/com.lalacore.serial-queue.plist ~/Library/LaunchAgents/

launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.monitor.plist 2>/dev/null || true
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.daily-health.plist 2>/dev/null || true
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.serial-queue.plist 2>/dev/null || true

launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.monitor.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.daily-health.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lalacore.serial-queue.plist
```

## Check status

```bash
launchctl print gui/$(id -u)/com.lalacore.monitor
launchctl print gui/$(id -u)/com.lalacore.daily-health
launchctl print gui/$(id -u)/com.lalacore.serial-queue
```

