# Migration Documentation Checklists

This file is the central index for upcoming documentation work across legacy migrations.

Use this as the first stop before creating/updating migration docs.

---

## CMG Migration Checklist

Primary feature memory:
- `.ai/customizations/features/cmg-database-migration.md`

Checklist:
- [ ] Validate final CLI examples for CMG-only orchestration (`--cmg-run-*`) in user-facing docs.
- [ ] Document unified clear semantics (`--clear-target-db`) and safety notes.
- [ ] Document CMG poller lifecycle controls (`--cmg-start-pollers`, `--cmg-stop-pollers`, `--cmg-restart-pollers`).
- [ ] Document explicit per-app flag model in `data_sync/bin/init.sh` (no implicit app/action defaults).
- [ ] Add runbook examples for partial reruns (CMG only) after dual-source migration.
- [ ] Add troubleshooting for lock handling (`--cmg-clear-locks`) and poller startup sequencing.

---

## Xenium Migration Checklist

Primary feature memory:
- `.ai/features/xenium-migration.md`

Checklist:
- [ ] Validate final CLI examples for Xenium-only orchestration (`--xenium-run-*`) in user-facing docs.
- [ ] Document unified clear semantics (`--clear-target-db`) and safety notes.
- [ ] Document Xenium poller lifecycle controls (`--xenium-start-pollers`, `--xenium-stop-pollers`, `--xenium-restart-pollers`).
- [ ] Document explicit per-app flag model in `data_sync/bin/init.sh` (no implicit app/action defaults).
- [ ] Add runbook examples for partial reruns (Xenium only) after dual-source migration.
- [ ] Add troubleshooting for lock handling (`--xenium-clear-locks`) and poller startup sequencing.

---

## Cross-App Orchestrator Checklist

Source:
- `data_sync/bin/init.sh`

Checklist:
- [ ] Document canonical action flags (`--cmg-run-bigbang`, `--xenium-run-bigbang`, plus per-app poller start/stop/restart flags).
- [ ] Document managed poller lifecycle flags and PID file locations under `data_sync/run/`.
- [ ] Document explicit rejection of deprecated/ambiguous flags.
- [ ] Add recommended `--dry-run` preflight workflow in operational docs.
- [ ] Add examples showing shared `--target-db` usage across single-app and dual-app runs.
- [ ] Add examples with independent clear-lock and clear-target flags per app.

---

## Bigbang Dev Documentation List (Feature Memory Seed)

Primary feature memories:
- `.ai/customizations/features/cmg-database-migration.md`
- `.ai/features/xenium-migration.md`

Checklist:
- [ ] Document host-side wrapper behavior for `data_sync/bin/bigbang.sh`, `data_sync/bin/bigbang_cmg.sh`, and `data_sync/bin/bigbang_xenium.sh` as thin launchers that execute Node scripts in `db_sandbox`.
- [ ] Document unified target reset semantics (`--clear-target-db`) and when to pair with `--clear-locks`.
- [ ] Document full bigbang phase order for CMG and Xenium, including post-bigbang cursor initialization expectations.
- [ ] Document rerun strategy (full rerun vs scoped rerun) and required preflight checks before execution.
- [ ] Document schema-alignment caveats for source-specific Prisma clients (CMG target client vs Xenium source client).
- [ ] Document Prisma migration caveat: when schema changes affect bigbang/poller code paths, restart both containers before running bigbang:
  - API container (main app Prisma client/runtime)
  - `db_sandbox` container (data_sync Prisma client/runtime)
- [ ] Document prerequisite: the API container must be fully up and healthy before running bigbang (bigbang relies on the target DB being reachable via the API's Postgres instance).
