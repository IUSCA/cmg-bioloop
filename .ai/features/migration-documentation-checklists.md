# Migration Documentation Checklists

This file is the central index for upcoming documentation work across legacy migrations.

Use this as the first stop before creating/updating migration docs.

---

## CMG Migration Checklist

Primary feature memory:
- `.ai/customizations/features/cmg-database-migration.md`

Checklist:
- [ ] Validate final CLI examples for CMG-only orchestration (`--cmg-run-*`) in user-facing docs.
- [ ] Document CMG-scoped clear semantics (`--clear-cmg-target-data`) and safety notes.
- [ ] Document explicit per-app flag model in `data_sync/bin/init.sh` (no implicit app/action defaults).
- [ ] Add runbook examples for partial reruns (CMG only) after dual-source migration.
- [ ] Add troubleshooting for lock handling (`--cmg-clear-locks`) and poller startup sequencing.

---

## Xenium Migration Checklist

Primary feature memory:
- `.ai/features/xenium-migration.md`

Checklist:
- [ ] Validate final CLI examples for Xenium-only orchestration (`--xenium-run-*`) in user-facing docs.
- [ ] Document Xenium-scoped clear semantics (`--clear-xenium-target-data`) and safety notes.
- [ ] Document explicit per-app flag model in `data_sync/bin/init.sh` (no implicit app/action defaults).
- [ ] Add runbook examples for partial reruns (Xenium only) after dual-source migration.
- [ ] Add troubleshooting for lock handling (`--xenium-clear-locks`) and poller startup sequencing.

---

## Cross-App Orchestrator Checklist

Source:
- `data_sync/bin/init.sh`

Checklist:
- [ ] Document canonical action flags (`--cmg-run-bigbang`, `--cmg-run-pollers`, `--xenium-run-bigbang`, `--xenium-run-pollers`).
- [ ] Document explicit rejection of deprecated/ambiguous flags.
- [ ] Add recommended `--dry-run` preflight workflow in operational docs.
- [ ] Add examples with independent target DB selection per app.
- [ ] Add examples with independent clear-lock and clear-target flags per app.
