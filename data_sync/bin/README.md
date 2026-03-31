# Data Sync Scripts

This directory contains shell entrypoints for legacy migration and sync operations.

## Canonical Entry Point

Use `./bin/init.sh` for all day-to-day CMG/Xenium orchestration.

- Explicit per-app actions (no implicit defaults for app selection)
- Shared `--target-db` for all selected actions in one run
- Managed poller lifecycle with PID files for start/stop/restart
- `--dry-run` support for preflight validation

## Main Scripts

- `init.sh` - primary orchestrator for CMG + Xenium bigbang and poller lifecycle
- `bigbang_cmg.sh` - CMG-only bigbang wrapper
- `bigbang_xenium.sh` - Xenium-only bigbang wrapper
- `start_pollers_cmg.sh` - CMG poller runner (foreground)
- `start_pollers_xenium.sh` - Xenium poller runner (foreground)
- `sync_conversion_logs.sh` - CMG conversion logs backfill utility
- `pull_logs_from_prod.sh` - fetch data_sync logs from production

## `init.sh` Flags (Current)

Action flags (at least one required):
- `--cmg-run-bigbang`
- `--xenium-run-bigbang`
- `--cmg-start-pollers`
- `--cmg-stop-pollers`
- `--cmg-restart-pollers`
- `--xenium-start-pollers`
- `--xenium-stop-pollers`
- `--xenium-restart-pollers`

CMG options:
- `--cmg-clear-locks`
- `--cmg-clear-target-data`
- `--cmg-skip-sessions`
- `--cmg-skip-conversion-logs`
- `--cmg-uri <uri>`

Xenium options:
- `--xenium-clear-locks`
- `--xenium-clear-target-data`

Shared/global options:
- `--target-db <sandbox|app|custom>` (default: `sandbox`)
- `--dry-run`
- `-h`, `--help`

Removed flags (intentionally rejected):
- `--cmg-run-pollers`, `--xenium-run-pollers`
- `--cmg-target-db`, `--xenium-target-db`
- generic/ambiguous flags like `--all`, `--both`, `--pollers`, `--bigbang`

## Quick Usage

Run both bigbangs against sandbox:
```bash
./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db sandbox
```

Reset both source datasets and rerun both bigbangs:
```bash
./bin/init.sh \
  --cmg-run-bigbang --xenium-run-bigbang \
  --cmg-clear-target-data --xenium-clear-target-data \
  --cmg-clear-locks --xenium-clear-locks \
  --target-db sandbox
```

Start both pollers in managed mode:
```bash
./bin/init.sh --cmg-start-pollers --xenium-start-pollers --target-db app
```

Restart CMG poller only:
```bash
./bin/init.sh --cmg-restart-pollers --target-db app
```

Stop both pollers:
```bash
./bin/init.sh --cmg-stop-pollers --xenium-stop-pollers
```

Preview before execution:
```bash
./bin/init.sh --cmg-run-bigbang --xenium-run-bigbang --target-db app --dry-run
```

## PID Files and Cleanup

Managed poller lifecycle uses PID files under `data_sync/run/`:
- `run/cmg_poller.pid`
- `run/xenium_poller.pid`

Behavior:
- `--*-start-pollers` writes PID after successful launch
- `--*-stop-pollers` prefers PID file, then falls back to process lookup
- stale PID files are auto-ignored/removed when PID is not running

Manual cleanup scenarios (rare):
- Host reboot / abrupt process kill left stale pid file
- Manual poller launch outside `init.sh`

Cleanup commands:
```bash
rm -f data_sync/run/cmg_poller.pid data_sync/run/xenium_poller.pid
```

## Logs

Runtime logs:
- local: `data_sync/logs/`
- production host: `/tmp/data_sync_logs/`

Use:
```bash
./bin/pull_logs_from_prod.sh --last 5
```

## Related Docs

- `../README.md`
- `../SETUP_GUIDE.md`
- `../BIGBANG_SYNC_USAGE.md`
- `../POLLER_SYNC_USAGE.md`
- `../TARGET_DATABASE_CONFIGURATION.md`
- `../LOGS.md`

---

**Last Updated:** 2026-03-12

