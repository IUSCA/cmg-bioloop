# Worker Integration Testing

Feature changelog for the worker integration test suite (pytest-based tests for watch.py, integrated workflow, stage workflow, and future CMG-specific workflow steps).

---

## 2026-03-01

- Decision: Worker tests are **integration tests** running against real Docker services (API, Postgres, Redis, Celery, Rhythm). No mocks. Consistent with the project's existing philosophy (e2e Playwright tests also use no mocks).
- Decision: Unit tests are used only for pure logic in `watch.py` with no I/O dependencies (e.g., `Register.is_a_reject()`, `_compute_dataset_origin()`). All workflow-touching tests are integration tests.
- Decision: Tests trigger the `Observer` (not `Register` directly) to test the actual watch mechanism: `Observer.watch()` is called once (single deterministic poll cycle), which diffs the filesystem against known state and calls `Register.register('add', [...])` for new directories. This validates the Observer → Register → API → Rhythm chain.
- Decision: `observer.watch()` is called directly rather than via `Poller.poll()` (infinite blocking loop). This is the minimal surface needed to test the watch behavior without timing complexity.
- Decision: Small real dataset fixtures are committed to `workers/tests/watch/fixtures/` per type: `raw_data_type_dataset/` (with `CopyComplete.txt` completion marker + minimal FASTQ) and `data_product_type_dataset/`. No synthetic mocks.
- Decision: Tests are parameterized over both dataset types (`RAW_DATA`, `DATA_PRODUCT`) using pytest's `params` fixture. Each test runs twice - once per type.
- Decision: `registered_dataset` fixture yields `{'dataset': ..., 'path': ...}` so tests have access to both the API response and the filesystem path for asserting `origin_path`.
- Decision: Assertions include `type`, `origin_path` (equals resolved path of the watched directory), `create_method == 'SCAN'`, workflow creation, and workflow status.
- Decision: Configurable skip via `SKIP_WATCH_SCRIPT_TESTS=true` in `workers/.env`. Useful for forks with customized registration or workflow logic. Skip is enforced at pytest level (`@pytest.mark.skipif`), not in CI config.
- Decision: pytest markers added to `pyproject.toml`: `integration`, `slow`, `requires_celery`, `watch_script`, `integrated_wf`, `stage_wf`. Default `addopts = "-m 'not integration'"` so `pytest` works locally without Docker.
- Decision: All dataset lifecycle testing lives in Python (worker tests). Playwright e2e tests only test UI behavior against already-processed data. No cross-language coordination needed.
- Decision: For future genome-browser/sessions/tracks and Conversions e2e tests, datasets will be pre-created and archived/staged by Python integration test fixtures. Dataset IDs/names are exported as test constants for Playwright tests to consume.
- Constraint: `await_stability` wait time must be kept small in CI (controlled via `recency_threshold_seconds` in `workers/workers/config/docker.py`). Currently set to `0` in docker config, meaning stability is achieved immediately for test datasets.
- Decision: Test dataset directories must reside under the configured `source_dir` for the dataset type (e.g. `/opt/sca/data/origin/raw_data/`) — a Docker `landing_volume` mount shared by all worker containers. Using `tmp_path` (pytest default) would cause Celery worker tasks to fail when resolving `origin_path` if the test runner and worker run in separate containers. The `watched_dir` fixture creates a UUID-prefixed `_test_<uuid>/` subdirectory inside `source_dir` and tears it down after each test.
- Decision: Tests are run via `docker compose exec celery_worker pytest -m integration` (or equivalent) — inside the container that already runs the Celery worker, so both test code and task execution share the same filesystem mount.
- Clarification: Watch script kickoff tests assert workflow was created and is in PENDING, RUNNING, or COMPLETED state. They do not assert full workflow step completion (that is the scope of `test_integrated_workflow_steps.py`, to be written next).
- Fix: `watch.py` `register_candidate` and `register_batch` were not passing `create_method` in the dataset payload. Both now pass `'create_method': 'SCAN'`. The API JSDoc already declared SCAN as the default but the code did not enforce it.

## 2026-03-13 (workflow assertion design)

- Decision: For asserting "workflow was kicked off", use `GET /workflows?dataset_id=<id>` (not `GET /datasets/:id?workflows=True`). Reason: the dataset endpoint catches Rhythm errors silently and returns `workflows: []` on failure. The workflows endpoint returns `{total: 0}` from Postgres alone when no record exists (no Rhythm call), and returns Rhythm-enriched results when records do exist — raising a 5xx if Rhythm is unreachable. No silent false negatives.
- Decision: Assert exactly `total == 1` workflow for the dataset, workflow name == `'integrated'`, and step names + count match `config['workflows']['integrated']['steps']`. The config is the single source of truth; if the workflow definition changes, the test automatically checks the new steps without code changes.
- Decision: Step-level success assertion lives in a separate `test_integrated_workflow_steps_all_succeed` method, marked `@pytest.mark.slow` and `@pytest.mark.integrated_wf`. It polls `GET /workflows/:id?last_task_runs=true&prev_task_runs=true` at 10s intervals for up to 300s.
- Decision: If Rhythm is unreachable during step polling, `raise_for_status()` propagates the 5xx as an exception — test fails loudly, not silently. If the response is 200 but lacks the `steps` key, test calls `pytest.fail()` immediately with a clear "API-to-Rhythm connection may be broken" message.
- Decision: Any step reaching `FAILED`, `FAILURE`, or `REVOKED` causes immediate test failure (no point waiting for other steps). Timeout failure includes a snapshot of the final step statuses.
- Clarification: `GET /workflows?dataset_id=<id>` returns `{total: 0}` early (no Rhythm call) when no Postgres `workflow` record exists. This means "workflow was never linked" is distinguishable from "Rhythm is down" at the assertion level.
- Added to `workers/workers/api.py`: `get_workflows_for_dataset(dataset_id, last_task_runs, prev_task_runs)` and `get_workflow(workflow_id, last_task_runs, prev_task_runs)` with full type annotations and docstrings.

## 2026-03-13 (logging)

### Log Configuration

- Decision: `pytest.ini` is the single config location for log behavior. Set `log_file`, `log_file_mode`, `log_cli`, and related keys there. Do not hardcode log file paths in test code.
- Decision: Two log outputs per test run (by design):
  1. **Per-run timestamped file**: `test_logs/test_run_YYYYMMDD_HHMMSS.log` — created by `pytest_sessionstart` hook in `tests/conftest.py`. One file per run; useful for comparing across runs.
  2. **Persistent cumulative file**: `test_logs/watch_tests.log` — written by pytest via `pytest.ini log_file = test_logs/watch_tests.log`. Append mode (`log_file_mode = a`). All runs accumulate in one file.
- Decision: `log_cli = true` in `pytest.ini` shows logs live in the terminal during a run. Disable with `log_cli = false` or `--no-header` at CLI.

### Log Locations

**Docker environment:**
| Location | Path |
|---|---|
| Container path | `/opt/sca/app/test_logs/` |
| Host path | `workers/test_logs/` |
| Volume mount | `./workers/:/opt/sca/app` in `docker-compose.yml` |
| Per-run file | `workers/test_logs/test_run_YYYYMMDD_HHMMSS.log` |
| Persistent file | `workers/test_logs/watch_tests.log` |

**Local/non-Docker:**
| Location | Path |
|---|---|
| Per-run file | `workers/test_logs/test_run_YYYYMMDD_HHMMSS.log` |
| Persistent file | `workers/test_logs/watch_tests.log` |

**Toggling log persistence:**
- Disable file output: comment out `log_file` in `pytest.ini`, or pass `--log-file=/dev/null` at CLI.
- Enable per-run only: remove `log_file` from `pytest.ini` (timestamped files still written by `pytest_sessionstart`).
- Override at CLI: `poetry run pytest ... --log-file=test_logs/my_run.log`

---

## 2026-03-13 (recency_threshold override)

- Decision: `await_stability` already supports a `recency_threshold` kwarg override via `**kwargs`. Rather than relying on `config['registration']['recency_threshold_seconds'] = 0` in `docker.py` (which affects all workers globally), tests now pass `recency_threshold=5` explicitly via `wf.start()`.
- Change: Added `wf_start_kwargs: dict[str, Any] | None = None` to `Register.__init__()`. Stored as `self.wf_start_kwargs` and forwarded as `**self.wf_start_kwargs` in `wf.start(dataset_id, ...)`.
- Change: `type_observer` fixture reads `WATCH_TEST_RECENCY_THRESHOLD` env var (default `5`) and passes it as `wf_start_kwargs={'recency_threshold': _RECENCY_THRESHOLD}`.
- Change: `docker.py` `recency_threshold_seconds` changed from `0` to `5`. Non-zero is more realistic; tests are now independent of this value.
- Rationale: The docker config `0` is a blunt global override — it silences the stability check for all registrations in docker (manual and test alike). The per-invocation kwarg approach makes test intent explicit and leaves the config at a sensible value.

---

## Base Bioloop Merge Checklist

When merging these tests to `IUSCA/bioloop` (the upstream), apply the items below.
The base repo uses `workers/` as module root; this fork uses `workers/workers/`.

### 1. Bug fix - `create_method: SCAN` in `watch.py`
- **File:** `workers/scripts/watch.py` (base path, not `workers/workers/`)
- **Change:** Add `'create_method': 'SCAN'` to the `dataset_payload` dict inside both `Register.register_candidate()` and `Register.register_batch()`
- **Status:** Required - this is a correctness fix, not CMG-specific
- **Note:** Base `Register` class is simpler (no `_compute_dataset_origin`, no legacy app logic) - apply only the `create_method` addition, do not copy CMG-specific code

### 2. pytest dev dependencies in `workers/pyproject.toml`
- **File:** `workers/pyproject.toml`
- **Change:** Add to `[tool.poetry.group.dev.dependencies]`:
  ```toml
  pytest = "^8.0.0"
  pytest-asyncio = "^0.23.0"
  pytest-timeout = "^2.2.0"
  ```
- **Status:** Required - base currently has no pytest deps
- **Note:** Do not copy `blake3` (CMG-specific, used for upload checksum tests)

### 3. pytest config in `workers/pyproject.toml`
- **File:** `workers/pyproject.toml`
- **Change:** Add entire `[tool.pytest.ini_options]` section with markers and `addopts`
- **Status:** Required - base has no pytest config at all
- **Note:** Copy the full block as-is; all markers are generic

### 4. Test infrastructure - `workers/tests/watch/`
- **Files to copy:**
  - `workers/tests/watch/__init__.py` — copy as-is
  - `workers/tests/watch/fixtures/raw_data_type_dataset/CopyComplete.txt` — copy as-is
  - `workers/tests/watch/fixtures/raw_data_type_dataset/reads_R1.fastq` — copy as-is
  - `workers/tests/watch/fixtures/data_product_type_dataset/metadata.txt` — copy as-is
- **Status:** Required - none of these exist in base

### 5. `workers/tests/watch/conftest.py` — requires import path adaptation
- **File:** `workers/tests/watch/conftest.py`
- **Change:** Copy the file as-is — import paths already match base bioloop:
  ```python
  from workers.config import config
  from workers.scripts.watch import Register
  from workers.services.watchlib import Observer
  ```
- **Note:** `watched_dir` derives its path from `config['registration'][dataset_type]['source_dir']` (the Docker volume source dir). Base bioloop's docker config must define this key for both `RAW_DATA` and `DATA_PRODUCT`. Verify `workers/config/docker.py` in base has matching paths.
- **Status:** Copy as-is (no adaptation needed)

### 6. `workers/tests/watch/test_watch_registration.py` — copy as-is
- **File:** `workers/tests/watch/test_watch_registration.py`
- **Change:** Copy as-is — no import path differences (only uses `workers.api` which is the same in both repos)
- **Status:** Required

### 7. `workers/tests/conftest.py` — check before creating
- The top-level `workers/tests/conftest.py` in this fork contains CMG-specific upload test fixtures (do not copy). Base has no `conftest.py` at this level.
- The pytest marker logging hooks (`pytest_runtest_setup`, `pytest_runtest_teardown`) and the `api_client` fixture in that file are generic. If base wants them, extract only those to a new `workers/tests/conftest.py`. The upload-specific fixtures (`test_data_dir`, `test_files`, `test_dataset`) are CMG-only.
- **Status:** Optional / partial

### Items that do NOT go to base bioloop
- `_LEGACY_APP_SOURCE_DIR_KEYS`, `_get_legacy_app_source_dirs`, `_compute_dataset_origin` in `watch.py` — CMG-specific legacy app integration
- `legacy_application_active` config logic — CMG-specific
- `intake_integrated` workflow references in watch.py `__main__` — CMG-specific
- CMG-specific observer setup (k2/k3/k4/nanopore/slate hosts) in `watch.py` `__main__` — CMG-specific
- `workers/pyproject.toml` deps: `pymongo`, `pika`, `fabric`, `blake3` — CMG-specific
- `workers/tests/conftest.py` upload fixtures — CMG-specific
- `.ai/` directory — CMG-specific AI memory artifacts

---
