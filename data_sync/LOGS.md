# Sync Logs Documentation

## Log Location

### Inside Container

Logs are written to `/tmp/` inside the container with the naming pattern:
```
/tmp/<script_name>_<timestamp>.log
```

Example:
```
/tmp/bigbang_sync_2026-01-14T12-30-45.log
/tmp/poller_sync_2026-01-14T13-15-22.log
```

### On Host Filesystem

The container's `/tmp/` directory is mounted to the host at:
```
<data_sync_directory>/logs/
```

**Production:**
```
/opt/sca/cmg/data_sync/logs/
```

**Local Development:**
```
./data_sync/logs/
```

## Log Contents

Each log file contains:
- Timestamp for each log entry
- Log level (info, warn, error, debug)
- Detailed migration progress
- Error messages with stack traces
- Database connection info (credentials sanitized)
- Per-step timing and counts

## Accessing Logs

### Local Development

```bash
# View latest bigbang log
ls -lt data_sync/logs/bigbang_sync_*.log | head -1 | xargs cat

# Tail poller logs
tail -f data_sync/logs/poller_sync_*.log

# View all logs
ls -lh data_sync/logs/
```

### Production - From Container

```bash
# List logs
docker compose -f docker-compose.sandbox.yml exec db_sandbox ls -lh /tmp/*.log

# View specific log
docker compose -f docker-compose.sandbox.yml exec db_sandbox cat /tmp/bigbang_sync_2026-01-14T12-30-45.log

# Tail poller log
docker compose -f docker-compose.sandbox.yml exec db_sandbox tail -f /tmp/poller_sync_*.log
```

### Production - From Host

```bash
# On production host
cd /opt/sca/cmg/data_sync

# List logs
ls -lh logs/

# View log
cat logs/bigbang_sync_2026-01-14T12-30-45.log

# Tail log
tail -f logs/poller_sync_*.log
```

## Pulling Logs from Production

Use the `pull_logs_from_prod.sh` script to download logs from the production host:

### Basic Usage

```bash
# Pull latest log from production (default, uses 'cmg-bioloop' SSH alias)
cd data_sync
./bin/pull_logs_from_prod.sh
```

**Note:** This uses the `cmg-bioloop` SSH alias from `~/.ssh/config`, which handles:
- Hostname: `cmg-new-service1.sca.iu.edu`
- User: `cmguser`
- ProxyJump through `jump.sca.iu.edu`

Logs will be downloaded to `./logs_from_prod/` directory.

**Important:** Remote logs are always read from `/tmp/data_sync_logs/` on the production host (not the container mount path). Files are sorted by timestamp (newest first).

### List Available Logs

```bash
# See what logs are available without downloading
./bin/pull_logs_from_prod.sh --list

# List last 5 logs
./bin/pull_logs_from_prod.sh --list --last 5

# List all available logs
./bin/pull_logs_from_prod.sh --list --all
```

### Pull Multiple Logs

```bash
# Pull last 3 log files (by timestamp, newest first)
./bin/pull_logs_from_prod.sh --last 3

# Pull all available log files
./bin/pull_logs_from_prod.sh --all
```

### Custom Options

```bash
# Pull from different host (using another SSH alias)
./bin/pull_logs_from_prod.sh --host bioloop

# Pull to specific directory
./bin/pull_logs_from_prod.sh --output ~/cmg_migration_logs

# Override SSH user (if needed)
./bin/pull_logs_from_prod.sh --user ripandey

# Use full hostname instead of alias
./bin/pull_logs_from_prod.sh --host cmg-new-service1.sca.iu.edu --user cmguser
```

### Full Example

```bash
# Pull last 5 logs with all custom options
./bin/pull_logs_from_prod.sh \
  --host cmg-bioloop \
  --last 5 \
  --output ~/migration_analysis/logs_$(date +%Y%m%d)
```

### All Options

| Option | Description | Default |
|--------|-------------|---------|
| `-h, --host` | Production host (SSH alias or hostname) | `cmg-bioloop` |
| `-u, --user` | SSH user | From SSH config (`cmguser`) |
| `-o, --output` | Local output directory | `./logs_from_prod` |
| `-n, --last N` | Download last N log files by timestamp | `1` |
| `--all` | Download all log files | (disabled) |
| `-l, --list` | List available log files without downloading | (disabled) |
| `--help` | Show help message | (disabled) |

## Log Analysis

### Search for Errors

```bash
# Find all errors in bigbang log
grep -i error logs/bigbang_sync_*.log

# Find failed operations
grep -i "failed\|error" logs/*.log | less
```

### Check Progress

```bash
# See what step it's on
grep -E "\[[0-9]+/[0-9]+\]" logs/bigbang_sync_*.log | tail -5

# Count processed records
grep "Processed.*datasets" logs/bigbang_sync_*.log
```

### View Statistics

```bash
# See final counts
grep -E "converted|created|skipped" logs/bigbang_sync_*.log | tail -20

# See timing info
grep -E "completed in|took|duration" logs/bigbang_sync_*.log
```

## Log Rotation

Logs are **not automatically rotated**. Each script run creates a new timestamped log file.

### Manual Cleanup

```bash
# List logs older than 7 days
find logs/ -name "*.log" -mtime +7

# Delete logs older than 7 days
find logs/ -name "*.log" -mtime +7 -delete

# Archive old logs
tar -czf logs_archive_$(date +%Y%m%d).tar.gz logs/*.log
```

### Production Cleanup

On the production host:

```bash
cd /opt/sca/cmg/data_sync

# Archive logs before cleanup
tar -czf logs_archive_$(date +%Y%m%d).tar.gz logs/*.log

# Move archive to backup location
mv logs_archive_*.tar.gz /opt/sca/backups/

# Clean up old logs (older than 30 days)
find logs/ -name "*.log" -mtime +30 -delete
```

## Troubleshooting

### Log File Not Found

**Symptom:** No logs in `logs/` directory

**Causes:**
1. Script hasn't been run yet
2. Logger initialization failed
3. Directory permissions issue

**Solution:**
```bash
# Check if directory exists
ls -la data_sync/logs/

# Check container logs
docker compose -f docker-compose.sandbox.yml logs db_sandbox

# Manually create directory if needed
mkdir -p data_sync/logs
chmod 755 data_sync/logs
```

### Cannot Pull Logs from Production

**Symptom:** `pull_logs_from_prod.sh` fails with SSH error

**Causes:**
1. SSH keys not configured
2. Wrong host/user
3. Network issue
4. Wrong remote path

**Solution:**
```bash
# Test SSH connection
ssh ripandey@cmg-new-service1.sca.iu.edu "echo OK"

# Check if logs directory exists
ssh ripandey@cmg-new-service1.sca.iu.edu "ls -la /opt/sca/cmg/data_sync/logs/"

# Run with correct parameters
./bin/pull_logs_from_prod.sh \
  --host cmg-new-service1.sca.iu.edu \
  --user ripandey \
  --path /opt/sca/cmg/data_sync
```

### Logs Taking Too Much Space

**Symptom:** `logs/` directory is very large

**Solution:**
```bash
# Check disk usage
du -sh data_sync/logs/

# Compress old logs
gzip data_sync/logs/*.log

# Archive and remove
tar -czf logs_backup_$(date +%Y%m%d).tar.gz data_sync/logs/*.log
rm data_sync/logs/*.log
```

## Log Levels

The logger supports different log levels via `LOG_LEVEL` environment variable:

```bash
# Set in .env file
LOG_LEVEL=debug

# Or via command line
LOG_LEVEL=debug node src/bigbang_cmg_sync.js
```

**Levels:**
- `error` - Only errors
- `warn` - Warnings and errors
- `info` - Normal operation (default)
- `debug` - Detailed debugging info
- `verbose` - Very detailed output

## Quick Reference

| Task | Command |
|------|---------|
| View local logs | `ls -lh data_sync/logs/` |
| Tail local log | `tail -f data_sync/logs/bigbang_sync_*.log` |
| List prod logs | `./bin/pull_logs_from_prod.sh --list` |
| Pull prod logs | `./bin/pull_logs_from_prod.sh` |
| Search for errors | `grep -i error logs/*.log` |
| Clean old logs | `find logs/ -name "*.log" -mtime +30 -delete` |
| Archive logs | `tar -czf logs_$(date +%Y%m%d).tar.gz logs/*.log` |

