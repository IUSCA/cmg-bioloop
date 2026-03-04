# Worker Integration Testing

Feature changelog for the worker integration test suite (pytest-based tests for watch.py, integrated workflow, stage workflow, and future CMG-specific workflow steps).

---

## 2026-03-01

- Decision: Worker tests are **integration tests** running against real Docker services (API, Postgres, Redis, Celery, Rhythm). No mocks. Consistent with the project's existing philosophy (e2e Playwright tests also use no mocks).
- Decision: Unit tests are used only for pure logic in `watch.py` with no I/O dependencies (e.g., `Register.is_a_reject()`, `_compute_dataset_origin()`). All workflow-touching tests are integration tests.
- Decision: Tests trigger the `Observer` (not `Register` directly) to test the actual watch mechanism: `Observer.watch()` is called once (single deterministic poll cycle), which diffs the filesystem against known state and calls `Register.register('add', [...])` for new directories. This validates the Observer → Register → API → Rhythm chain.
- Decision: `observer.watch()` is called directly rather than via `Poller.poll()` (infinite blocking loop). This is the minimal surface needed to test the watch behavior without timing complexity.
- Decision: Small real dataset fixtures are committed to `workers/tests/watch/fixtures/raw_data_dataset/` (a tiny FASTQ file + `CopyComplete.txt` completion marker). No synthetic mocks or random binary files for watch tests.
- Decision: Test datasets are registered using `RAW_DATA` type with `default_wf_name='integrated'`, matching the primary production use case.
- Decision: Configurable skip via `SKIP_WATCH_SCRIPT_TESTS=true` in `workers/.env`. Useful for forks with customized registration or workflow logic. Skip is enforced at pytest level (`@pytest.mark.skipif`), not in CI config.
- Decision: pytest markers added to `pyproject.toml`: `integration`, `slow`, `requires_celery`, `watch_script`, `integrated_wf`, `stage_wf`. Default `addopts = "-m 'not integration'"` so `pytest` works locally without Docker.
- Decision: All dataset lifecycle testing lives in Python (worker tests). Playwright e2e tests only test UI behavior against already-processed data. No cross-language coordination needed.
- Decision: For future genome-browser/sessions/tracks and Conversions e2e tests, datasets will be pre-created and archived/staged by Python integration test fixtures. Dataset IDs/names are exported as test constants for Playwright tests to consume.
- Constraint: `await_stability` wait time must be kept small in CI (controlled via `recency_threshold_seconds` in `workers/workers/config/docker.py`). Currently set to `0` in docker config, meaning stability is achieved immediately for test datasets.
- Clarification: Watch script kickoff tests assert workflow was created and is in PENDING, RUNNING, or COMPLETED state. They do not assert full workflow step completion (that is the scope of `test_integrated_workflow_steps.py`, to be written next).

---
