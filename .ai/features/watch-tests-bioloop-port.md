# Watch.py Integration Test Suite: Bioloop Port Guide

This document is for an AI agent working in the **`IUSCA/bioloop`** repository. It contains the complete, exact changes needed to port the `watch.py` integration test suite (and related bug fix) from the `cmg-bioloop` fork to the base bioloop repo. Apply each section in order.

---

## Context

These changes were developed in the `cmg-bioloop` fork for convenience but target the base `IUSCA/bioloop` repo. The test suite is **not CMG-specific** — it tests the generic watch script registration → integrated workflow kickoff flow that both repos share. The CMG fork just happened to be where the work was done first.

**Path note:** In the CMG fork, worker source lives at `workers/workers/` (repo-relative) but has the Python module root `workers` (container path `/opt/sca/app/` → `workers/`). In base bioloop, worker source lives at `workers/` (repo-relative) with the same module root. All Python import statements in the test files are **already correct for bioloop** as-is.

---

## Overview of Changes

| Type | Files | CMG fork path | Bioloop path |
|---|---|---|---|
| Bug fix | `watch.py` `create_method` | `workers/workers/scripts/watch.py` | `workers/scripts/watch.py` |
| New functions | `api.py` workflow helpers | `workers/workers/api.py` | `workers/api.py` |
| New files | Test fixtures + conftest + test | `workers/tests/watch/` | `workers/tests/watch/` (same) |
| New file | `pytest.ini` | `workers/pytest.ini` | `workers/pytest.ini` |
| New file | `tests/conftest.py` (generic hooks only) | `workers/tests/conftest.py` | `workers/tests/conftest.py` |
| Update | `pyproject.toml` dev deps | `workers/pyproject.toml` | `workers/pyproject.toml` |
| Update | `worker_conventions.md` | `.ai/bioloop/worker_conventions.md` | `.ai/bioloop/worker_conventions.md` |
| Update | `api/bin/entrypoint.sh` | `api/bin/entrypoint.sh` | `api/bin/entrypoint.sh` |

---

## 1. `Register.__init__` — Add `wf_start_kwargs` Parameter

**File:** `workers/scripts/watch.py`

Add a `wf_start_kwargs` parameter to `Register.__init__` and forward it to `wf.start()`. This allows callers (including tests) to pass per-invocation step overrides (such as `recency_threshold`) without touching the shared config.

In `Register.__init__`, add the import and parameter:

```python
from typing import Any  # add if not already present

class Register:
    def __init__(
        self,
        dataset_type: str,
        default_wf_name: str = 'integrated',
        wf_start_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.dataset_type = dataset_type
        # ... existing fields ...
        self.wf_start_kwargs: dict[str, Any] = wf_start_kwargs or {}
        self.batch_size: int = 100
        self.metadata = kwargs
```

In `run_workflows()`, change the `wf.start()` call:

```python
# Before:
wf.start(dataset_id)

# After:
wf.start(dataset_id, **self.wf_start_kwargs)
```

---

## 2. Bug Fix — `create_method: 'SCAN'` in `watch.py`

**File:** `workers/scripts/watch.py`

In `Register.register_candidate()` and `Register.register_batch()`, the `dataset_payload` dict was missing `create_method`. The API accepts it and documents `SCAN` as the value for watch-script-registered datasets, but the code never set it.

Find the `dataset_payload` dict in **both** methods and add `'create_method': 'SCAN'`.

In `register_candidate()`, the payload should look like:
```python
dataset_payload = {
    'name': candidate.name,
    'type': self.dataset_type,
    'origin_path': str(candidate.resolve()),
    'create_method': 'SCAN',
}
```

In `register_batch()`, the per-candidate dict inside the loop should look like:
```python
dataset_payload = {
    'name': candidate.name,
    'type': self.dataset_type,
    'origin_path': str(candidate.resolve()),
    'create_method': 'SCAN',
}
```

No other changes to `watch.py` are needed. Do **not** copy any CMG-specific code from the fork (`_LEGACY_APP_SOURCE_DIR_KEYS`, `_compute_dataset_origin`, legacy app observer setup, etc.).

---

## 2. New Functions in `workers/api.py`

**File:** `workers/api.py`

First, ensure `from typing import Any` is present at the top of the file (add it if missing alongside the other standard imports).

Then add the two functions below. Place them after the existing `get_dataset()` function (before `DatasetAlreadyExistsError` class or `create_dataset()`, wherever makes sense in the existing file's ordering):

```python
def get_workflows_for_dataset(
    dataset_id: int,
    last_task_runs: bool = False,
    prev_task_runs: bool = False,
) -> dict[str, Any]:
    """
    Fetch all workflows linked to a dataset via GET /workflows?dataset_id=<id>.

    Returns { metadata: { total: N, ... }, results: [...] }.

    When total == 0 the API returns immediately from Postgres without calling
    the Rhythm API.  When total > 0 the results are Rhythm-enriched; if Rhythm
    is unreachable the API returns a 5xx and raise_for_status() will raise,
    failing the caller loudly rather than silently returning an empty list.
    """
    with APIServerSession() as s:
        r = s.get(
            'workflows',
            params={
                'dataset_id': dataset_id,
                'last_task_runs': last_task_runs,
                'prev_task_runs': prev_task_runs,
            },
        )
        r.raise_for_status()
        return r.json()


def get_workflow(
    workflow_id: str,
    last_task_runs: bool = True,
    prev_task_runs: bool = True,
) -> dict[str, Any]:
    """
    Fetch a single workflow by ID via GET /workflows/<workflow_id>.

    Returns the full Rhythm-enriched workflow document including per-step
    status and task-run history.  Raises HTTPError if Rhythm is unreachable.
    """
    with APIServerSession() as s:
        r = s.get(
            f'workflows/{workflow_id}',
            params={
                'last_task_runs': last_task_runs,
                'prev_task_runs': prev_task_runs,
            },
        )
        r.raise_for_status()
        return r.json()
```

---

## 3. New Test Infrastructure — `workers/tests/watch/`

Create the following files. All paths are relative to the repo root.

### 3.1 `workers/tests/watch/__init__.py`

Create as an empty file (standard Python package marker).

### 3.2 `workers/tests/watch/fixtures/raw_data_type_dataset/CopyComplete.txt`

```
Copy is complete and verified.
```

(Trailing newline after the text. This file is required by the `await_stability` task for standard Illumina-style datasets — it signals that the sequencer has finished writing.)

### 3.3 `workers/tests/watch/fixtures/raw_data_type_dataset/reads_R1.fastq`

```
@SEQ_ID_1
GATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT
+
!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65
@SEQ_ID_2
ACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGTACGT
+
IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII
```

(Minimal valid FASTQ content — 2 reads.)

### 3.4 `workers/tests/watch/fixtures/data_product_type_dataset/metadata.txt`

```
Sample data product dataset for watch.py integration testing.
```

(Trailing newline. DATA_PRODUCT datasets have no completion marker requirement.)

### 3.5 `workers/tests/watch/conftest.py`

Create with the following exact content:

```python
"""
Fixtures for watch.py integration tests.

These tests run against real Docker services (API, Postgres, Redis, Celery).
They test the full Observer -> Register -> API -> Rhythm chain.

Requirements:
    - APP_ENV=docker (or equivalent) in workers/.env
    - Docker stack running: api, postgres, redis, celery worker, rhythm
"""

import logging
import os
import shutil
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

import workers.api as api
from workers.config import config
from workers.scripts.watch import Register
from workers.services.watchlib import Observer

logger = logging.getLogger(__name__)

# Seconds passed as recency_threshold to await_stability via wf.start() kwargs.
# Overrides the shared config value for this test's workflow invocation only,
# so docker.py/common.py thresholds are unaffected.
# Override via env var: WATCH_TEST_RECENCY_THRESHOLD=10 poetry run pytest ...
_RECENCY_THRESHOLD: int = int(os.getenv('WATCH_TEST_RECENCY_THRESHOLD', '5'))

FIXTURES_DIR: Path = Path(__file__).parent / 'fixtures'

# Fixture directory name per dataset type.
# RAW_DATA: includes CopyComplete.txt (required by await_stability for standard Illumina datasets).
# DATA_PRODUCT: no completion markers required.
_FIXTURE_DIR_BY_TYPE: dict[str, Path] = {
    'RAW_DATA': FIXTURES_DIR / 'raw_data_type_dataset',
    'DATA_PRODUCT': FIXTURES_DIR / 'data_product_type_dataset',
}


@pytest.fixture(scope='function')
def watched_dir(dataset_type: str) -> Generator[Path, None, None]:
    """
    A fresh isolated subdirectory inside the configured source_dir for this
    dataset_type (e.g. /opt/sca/data/origin/raw_data/_test_<uuid>/).

    Using the source_dir (a Docker landing_volume mount) ensures that both
    the test runner process and the Celery worker process share the same
    filesystem view of the dataset files via the same absolute path.

    If the source_dir does not exist (running outside Docker), falls back to
    a local temp directory with a warning. In that case tests that depend on
    Celery workers accessing origin_path will fail or behave unexpectedly.
    """
    source_dir: Path = Path(config['registration'][dataset_type]['source_dir'])
    test_session_dir: Path = (
        source_dir if source_dir.exists() else Path('/tmp/bioloop_watch_tests')
    ) / f'_test_{uuid.uuid4().hex[:12]}'

    if not source_dir.exists():
        logger.warning(
            f'source_dir {source_dir} not found. '
            f'Falling back to {test_session_dir}. '
            f'Celery tasks that access origin_path may fail outside Docker.'
        )

    test_session_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f'Created isolated test watch dir: {test_session_dir}')

    yield test_session_dir

    if test_session_dir.exists():
        shutil.rmtree(test_session_dir)
        logger.info(f'Removed test watch dir: {test_session_dir}')


@pytest.fixture(params=['RAW_DATA', 'DATA_PRODUCT'], scope='function')
def dataset_type(request: pytest.FixtureRequest) -> str:
    """
    Parameterized fixture providing each dataset type in turn.
    Tests that depend on this fixture run once per type.
    """
    return request.param


@pytest.fixture(scope='function')
def type_observer(
    dataset_type: str,
    watched_dir: Path,
) -> Observer:
    """
    An Observer instance watching watched_dir, wired to a Register for the
    current dataset_type.

    The Observer's first watch() call is made here (with an empty directory),
    establishing the initial known-state. Subsequent watch() calls in tests
    or in the registered_dataset fixture will detect newly added directories
    as 'add' events.

    This tests the Observer -> Register chain exactly as watch.py does in
    production, just without the Poller's blocking loop.
    """
    register: Register = Register(
        dataset_type=dataset_type,
        default_wf_name='integrated',
        wf_start_kwargs={'recency_threshold': _RECENCY_THRESHOLD},
    )
    obs: Observer = Observer(
        name=f'test_{dataset_type.lower()}_observer',
        dir_path=str(watched_dir),
        callback=register.register,
        interval=1,
    )
    # Establish initial state: Observer sees an empty directory.
    # This mirrors what happens when watch.py starts up before any datasets arrive.
    obs.watch()
    logger.info(f'Observer initialized with empty watched dir: {watched_dir} (type: {dataset_type})')
    return obs


@pytest.fixture(scope='function')
def registered_dataset(
    dataset_type: str,
    watched_dir: Path,
    type_observer: Observer,
) -> Generator[dict[str, Any], None, None]:
    """
    Creates a small real test dataset directory inside watched_dir (copied from
    tests/watch/fixtures/<type>_type_dataset/), then triggers one Observer.watch()
    cycle to simulate the watch script detecting a new dataset.

    Yields a dict with the registered dataset (fetched from API) and its
    filesystem path:
        {
            'dataset': <dataset dict from API>,
            'path': <Path to dataset directory>,
        }

    Teardown: deletes the dataset from the API and removes the directory.
    """
    dataset_name: str = f'test_watch_{uuid.uuid4().hex[:12]}'
    dataset_path: Path = watched_dir / dataset_name

    fixture_src: Path = _FIXTURE_DIR_BY_TYPE[dataset_type]
    shutil.copytree(fixture_src, dataset_path)
    logger.info(f'Copied {dataset_type} fixture to: {dataset_path}')

    # Trigger the Observer: it diffs current dirs against known state and
    # calls Register.register('add', [dataset_path]) for the new directory.
    type_observer.watch()
    logger.info(f'Observer.watch() triggered - should have registered: {dataset_name} (type: {dataset_type})')

    matches: list[dict[str, Any]] = api.get_all_datasets(
        dataset_type=dataset_type,
        name=dataset_name,
        match_name_exact=True,
    )
    if not matches:
        pytest.fail(
            f'Dataset "{dataset_name}" (type: {dataset_type}) was not found in the API '
            f'after Observer.watch(). Check API reachability and APP_API_TOKEN.'
        )

    dataset: dict[str, Any] = api.get_dataset(dataset_id=matches[0]['id'], workflows=True)
    logger.info(f'Registered dataset: id={dataset["id"]}, name={dataset["name"]}, type={dataset_type}')

    yield {'dataset': dataset, 'path': dataset_path}

    # Teardown: remove dataset record from API.
    # The dataset directory on disk is cleaned up by the watched_dir fixture
    # (it removes the entire test session dir).
    try:
        api.delete_dataset(dataset['id'])
        logger.info(f'Deleted test dataset from API: {dataset["id"]}')
    except Exception as e:
        logger.warning(f'Failed to delete test dataset {dataset["id"]} from API: {e}')
```

**Before copying this file:** Section 1 of this guide adds `wf_start_kwargs` to `Register.__init__()` — apply that change first, otherwise the `type_observer` fixture call above will raise a `TypeError`. Also verify that `Register` accepts `dataset_type` and `default_wf_name` in bioloop's `watch.py`.

**Config key note:** In the CMG fork, workflow definitions live under `config['workflow_registry']['integrated']['steps']`. The test file references this key. In base bioloop, check whether the key is `config['workflows']['integrated']['steps']` or `config['workflow_registry']['integrated']['steps']` and update the two references in `test_watch_registration.py` accordingly (lines in `test_observer_detects_new_directory_and_triggers_integrated_workflow` and `test_integrated_workflow_steps_all_succeed`).

### 3.6 `workers/tests/watch/test_watch_registration.py`

Create with the following exact content:

```python
"""
Integration tests: watch.py dataset registration -> integrated workflow kickoff.

Each test class runs once per dataset type (RAW_DATA, DATA_PRODUCT) via the
parameterized `dataset_type` fixture in conftest.py.

What is tested:
    - Observer detects a new directory in the watched path ('add' event)
    - Register.register() is called via the Observer callback
    - The API creates a dataset record with the correct type, origin_path,
      and create_method == 'SCAN'
    - An 'integrated' workflow is kicked off (created in Rhythm + linked to dataset)
    - A second Observer.watch() cycle does NOT re-register the same directory

What is NOT tested here:
    - Full workflow step execution (archive, validate, etc.) - those go in
      tests/watch/test_integrated_workflow_steps.py (to be written)
    - The Poller's scheduling loop - thin scheduling glue, not tested here

How it works:
    Observer.watch() is called directly (not via Poller's blocking loop).
    This is the deterministic equivalent of one polling cycle in watch.py.
    It tests the Observer -> Register -> API -> Rhythm chain in full.

Markers:
    integration       - requires running Docker stack
    watch_script      - tests specific to the watch.py script behavior
    requires_celery   - Celery worker must be running (workflow tasks are queued)

Skip configuration:
    Set SKIP_WATCH_SCRIPT_TESTS=true in workers/.env to skip these tests.
    Useful for forks that have customized registration or workflow logic.
"""

import logging
import os
import time
from pathlib import Path
from typing import Any

import pytest

import workers.api as api
from workers.config import config
from workers.services.watchlib import Observer

# Maximum seconds to wait for all workflow steps to reach SUCCESS.
# The integrated workflow includes archive + stage which can take real time
# even for tiny test datasets.  Keep well below the pytest timeout (600s).
_WORKFLOW_COMPLETION_TIMEOUT_SECONDS: int = 300

# How long to wait between polls when checking step progress.
# Steps take seconds-to-minutes each; polling more frequently than this
# wastes log noise without adding value.
_WORKFLOW_POLL_INTERVAL_SECONDS: float = 10.0

# Step statuses from which the workflow will never recover.
_TERMINAL_FAILURE_STATUSES: frozenset[str] = frozenset({'FAILED', 'FAILURE', 'REVOKED'})

logger = logging.getLogger(__name__)

_skip_watch_tests: bool = os.getenv('SKIP_WATCH_SCRIPT_TESTS', 'false').lower() == 'true'


@pytest.mark.integration
@pytest.mark.watch_script
@pytest.mark.requires_celery
@pytest.mark.skipif(_skip_watch_tests, reason='SKIP_WATCH_SCRIPT_TESTS=true in environment')
class TestWatchRegistration:
    """
    Tests that the watch script correctly detects new directories and registers
    datasets with the expected attributes and kicks off workflows.

    Runs for both RAW_DATA and DATA_PRODUCT via the parameterized dataset_type fixture.
    """

    def test_observer_detects_new_directory_and_creates_dataset(
        self,
        registered_dataset: dict[str, Any],
        dataset_type: str,
    ) -> None:
        """
        When a new directory appears in the watched path, the Observer should
        detect it as an 'add' event and the Register callback should create a
        dataset record via the API with:
            - correct type
            - origin_path pointing to the watched directory
            - create_method == 'SCAN'
        """
        dataset: dict[str, Any] = registered_dataset['dataset']
        dataset_path: Path = registered_dataset['path']

        assert dataset['id'] is not None
        assert dataset['type'] == dataset_type

        assert dataset['origin_path'] == str(dataset_path.resolve()), (
            f'Expected origin_path to be "{dataset_path.resolve()}", '
            f'got: "{dataset["origin_path"]}"'
        )

        assert dataset['create_method'] == 'SCAN', (
            f'Expected create_method to be "SCAN" for a watch-script-registered dataset, '
            f'got: "{dataset["create_method"]}"'
        )

        logger.info(
            f'Dataset created: id={dataset["id"]}, type={dataset_type}, '
            f'origin_path={dataset["origin_path"]}, create_method={dataset["create_method"]}'
        )

    def test_observer_detects_new_directory_and_triggers_integrated_workflow(
        self,
        registered_dataset: dict[str, Any],
        dataset_type: str,
    ) -> None:
        """
        After Observer detects a new directory, exactly one 'integrated' workflow
        should be linked to the dataset in Postgres and visible via Rhythm.

        Asserts:
            - Exactly 1 workflow is linked to the dataset (Postgres: prisma.workflow)
            - Workflow name is 'integrated' (Rhythm-enriched)
            - Step count and step names match config['workflows']['integrated']['steps']
              — the single source of truth for the workflow definition

        Uses GET /workflows?dataset_id=<id> which:
            - Returns { total: 0, results: [] } from Postgres alone when no workflow
              exists (no Rhythm call -> no silent false negative).
            - Returns Rhythm-enriched results when a workflow does exist; if Rhythm is
              unreachable the API raises 5xx -> raise_for_status() fails loudly here
              rather than returning an empty list.

        The POST /datasets/:id/workflows call inside watch.py is synchronous, so the
        Postgres record exists by the time Observer.watch() returns. No polling needed.
        """
        dataset: dict[str, Any] = registered_dataset['dataset']

        result: dict[str, Any] = api.get_workflows_for_dataset(
            dataset_id=dataset['id'],
        )
        total: int = result.get('metadata', {}).get('total', 0)
        workflows: list[dict[str, Any]] = result.get('results', [])

        assert total == 1, (
            f'Expected exactly 1 workflow for dataset {dataset["id"]} '
            f'(type: {dataset_type}), found {total}. '
            f'The integrated workflow was either not kicked off or kicked off more than once.'
        )
        assert len(workflows) == 1

        workflow: dict[str, Any] = workflows[0]
        assert workflow.get('name') == 'integrated', (
            f'Expected workflow name "integrated" for dataset {dataset["id"]} '
            f'(type: {dataset_type}), got: {workflow.get("name")!r}'
        )

        expected_steps: list[dict[str, Any]] = config['workflows']['integrated']['steps']
        expected_step_names: list[str] = [s['name'] for s in expected_steps]
        actual_steps: list[dict[str, Any]] = workflow.get('steps', [])
        actual_step_names: list[str] = [s['name'] for s in actual_steps]

        assert len(actual_steps) == len(expected_steps), (
            f'Step count mismatch for workflow {workflow["id"]} '
            f'(dataset {dataset["id"]}, type: {dataset_type}). '
            f'Config defines {len(expected_steps)} steps, Rhythm returned {len(actual_steps)}. '
            f'Expected: {expected_step_names}, Got: {actual_step_names}'
        )
        assert actual_step_names == expected_step_names, (
            f'Step names mismatch for workflow {workflow["id"]} '
            f'(dataset {dataset["id"]}, type: {dataset_type}). '
            f'Expected: {expected_step_names}, Got: {actual_step_names}'
        )

        logger.info(
            f'Workflow kicked off: id={workflow["id"]}, name={workflow["name"]}, '
            f'steps={len(actual_steps)}, dataset_id={dataset["id"]}, type={dataset_type}'
        )

    @pytest.mark.slow
    @pytest.mark.integrated_wf
    def test_integrated_workflow_steps_all_succeed(
        self,
        registered_dataset: dict[str, Any],
        dataset_type: str,
    ) -> None:
        """
        After the integrated workflow is kicked off, all steps defined in
        config['workflows']['integrated']['steps'] should eventually reach
        SUCCESS status.

        Polls GET /workflows/:id?last_task_runs=true&prev_task_runs=true until:
            - All config-defined steps are SUCCESS -> pass
            - Any step reaches a terminal failure (FAILED / REVOKED) -> fail immediately
            - Timeout (_WORKFLOW_COMPLETION_TIMEOUT_SECONDS) -> fail with step snapshot
            - Rhythm unreachable (API 5xx) -> raise_for_status() fails loudly;
              no silent empty-list behaviour

        Step names are read from config (single source of truth). If the workflow
        definition changes, this test automatically validates the updated steps
        against the Rhythm response without any test code changes.

        If the Rhythm response contains a 'steps' key the Rhythm connection is
        working.  If 'steps' is absent the connection is broken and the test
        fails immediately with a clear message.
        """
        dataset: dict[str, Any] = registered_dataset['dataset']

        result: dict[str, Any] = api.get_workflows_for_dataset(
            dataset_id=dataset['id'],
        )
        workflows: list[dict[str, Any]] = result.get('results', [])

        if not workflows:
            pytest.fail(
                f'No workflow found for dataset {dataset["id"]} (type: {dataset_type}). '
                f'Cannot poll for step completion without a workflow.'
            )

        workflow_id: str = workflows[0]['id']
        expected_steps: list[dict[str, Any]] = config['workflows']['integrated']['steps']
        expected_step_names: list[str] = [s['name'] for s in expected_steps]

        last_step_statuses: dict[str, str] = {}
        elapsed: float = 0.0

        while elapsed < _WORKFLOW_COMPLETION_TIMEOUT_SECONDS:
            wf: dict[str, Any] = api.get_workflow(
                workflow_id=workflow_id,
                last_task_runs=True,
                prev_task_runs=True,
            )

            steps: list[dict[str, Any]] | None = wf.get('steps')
            if steps is None:
                pytest.fail(
                    f'Rhythm API returned workflow {workflow_id} without a "steps" key. '
                    f'The API-to-Rhythm connection may be broken. '
                    f'Response keys present: {list(wf.keys())}'
                )

            last_step_statuses = {s['name']: s['status'] for s in steps}

            failed_steps: dict[str, str] = {
                name: status
                for name, status in last_step_statuses.items()
                if status in _TERMINAL_FAILURE_STATUSES
            }
            if failed_steps:
                pytest.fail(
                    f'Workflow {workflow_id} has steps in a terminal failure state '
                    f'for dataset {dataset["id"]} (type: {dataset_type}): {failed_steps}'
                )

            pending_steps: list[str] = [
                name for name in expected_step_names
                if last_step_statuses.get(name) != 'SUCCESS'
            ]
            if not pending_steps:
                logger.info(
                    f'All {len(expected_step_names)} steps succeeded for workflow '
                    f'{workflow_id}, dataset {dataset["id"]} (type: {dataset_type})'
                )
                return

            done_count: int = len(expected_step_names) - len(pending_steps)
            logger.debug(
                f'Workflow {workflow_id}: {done_count}/{len(expected_step_names)} steps done. '
                f'Still waiting: {pending_steps}'
            )

            time.sleep(_WORKFLOW_POLL_INTERVAL_SECONDS)
            elapsed += _WORKFLOW_POLL_INTERVAL_SECONDS

        pytest.fail(
            f'Workflow {workflow_id} did not complete within '
            f'{_WORKFLOW_COMPLETION_TIMEOUT_SECONDS}s for dataset '
            f'{dataset["id"]} (type: {dataset_type}). '
            f'Final step statuses: {last_step_statuses}'
        )

    def test_observer_does_not_register_same_directory_twice(
        self,
        registered_dataset: dict[str, Any],
        dataset_type: str,
        type_observer: Observer,
    ) -> None:
        """
        If the same directory is present in the watched path on a second
        Observer.watch() cycle, it should NOT attempt to register it again.

        The Observer's known-state diffing (added = current - known) prevents
        re-submission. DatasetAlreadyExistsError in register_candidate() is a
        secondary safety net but should never be reached here.
        """
        dataset_name: str = registered_dataset['dataset']['name']

        # Second watch cycle - the directory is already in the Observer's known state
        type_observer.watch()

        matches: list[dict[str, Any]] = api.get_all_datasets(
            dataset_type=dataset_type,
            name=dataset_name,
            match_name_exact=True,
        )
        assert len(matches) == 1, (
            f'Expected exactly 1 dataset named "{dataset_name}" (type: {dataset_type}), '
            f'but found {len(matches)}. Observer may have re-registered the directory.'
        )
        logger.info(
            f'Idempotency confirmed: "{dataset_name}" (type: {dataset_type}) exists exactly once'
        )
```

---

## 4. New `workers/pytest.ini`

Check if `workers/pytest.ini` already exists in bioloop. If it does, merge in the markers and logging block shown below. If it does not exist, create it with this full content:

```ini
[pytest]
# Test discovery
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*

# Output
addopts =
    --verbose
    --strict-markers
    --tb=short
    --disable-warnings

# Markers
markers =
    integration: Integration tests (run against Docker services)
    slow: Slow tests (may take minutes)
    requires_celery: Tests that require Celery worker running
    watch_script: Tests for watch.py Observer/Register behavior
    integrated_wf: Tests for the integrated workflow steps (archive, validate, etc.)
    stage_wf: Tests for the stage workflow steps

# Logging
# log_file and log_file_mode are the single config knobs for log persistence.
# - To disable persistent logs: comment out log_file, or pass --log-file=/dev/null at CLI.
# - To use a per-run file: pass --log-file=test_logs/run_$(date +%s).log at CLI.
# - log_file_mode = a (append) keeps history across runs in one file.
#   Change to w to truncate on each run.
#
# Docker:  container path /opt/sca/app/test_logs/  == host path workers/test_logs/
#          (volume mounted: ./workers/:/opt/sca/app in docker-compose.yml)
# Local:   workers/test_logs/  (rootdir-relative)
log_cli = true
log_cli_level = INFO
log_file = test_logs/watch_tests.log
log_file_mode = a
log_file_level = DEBUG
log_format = %(asctime)s %(levelname)-8s %(name)s:%(filename)s:%(lineno)d %(message)s
log_date_format = %Y-%m-%d %H:%M:%S

# Test timeout: 10 minutes max per test (for slow tests)
timeout = 600
```

If `[tool.pytest.ini_options]` exists in `workers/pyproject.toml`, **remove it entirely** — `pytest.ini` takes precedence and having both causes confusion.

---

## 5. New/Updated `workers/tests/conftest.py`

Check if `workers/tests/conftest.py` exists in bioloop.

- **If it does not exist:** Create it with the content below.
- **If it does exist:** Merge in only the `pytest_sessionstart`, `pytest_runtest_setup`, and `pytest_runtest_teardown` hooks (the logging hooks). Do not discard any existing content.

Content to add (create as a new file if absent, or append the hooks if present):

```python
"""
Pytest Configuration and Shared Fixtures

Provides reusable hooks for all worker integration tests.
Tests run against Docker services (API, Postgres, Redis, Celery).
"""

import logging
from datetime import datetime
from pathlib import Path

import pytest

# Log output is configured in pytest.ini (log_file, log_file_level, log_cli).
# pytest_sessionstart below also writes a per-run timestamped file alongside
# the persistent watch_tests.log managed by pytest itself.
#
# Log locations (Docker):
#   container: /opt/sca/app/test_logs/
#   host:      workers/test_logs/   (volume: ./workers/:/opt/sca/app)
# Log locations (local/non-Docker):
#   workers/test_logs/
#
# Per-run file:   test_logs/test_run_YYYYMMDD_HHMMSS.log
# Persistent log: test_logs/watch_tests.log  (controlled by pytest.ini log_file)
TEST_LOGS_DIR: Path = Path(__file__).parent.parent / 'test_logs'

logging.getLogger().setLevel(logging.DEBUG)

logger: logging.Logger = logging.getLogger(__name__)


def pytest_sessionstart(session: pytest.Session) -> None:
    """
    Pytest hook: Add a per-run timestamped FileHandler to the root logger.

    This creates test_logs/test_run_YYYYMMDD_HHMMSS.log alongside the
    persistent watch_tests.log that pytest writes via pytest.ini log_file.
    Both files land in the same test_logs/ directory.
    """
    TEST_LOGS_DIR.mkdir(exist_ok=True)
    timestamp: str = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_log_path: Path = TEST_LOGS_DIR / f'test_run_{timestamp}.log'

    handler: logging.FileHandler = logging.FileHandler(run_log_path)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter('%(asctime)s %(levelname)-8s %(name)s %(message)s')
    )
    logging.getLogger().addHandler(handler)

    logger.info("=" * 80)
    logger.info(f"TEST RUN STARTED: {timestamp}")
    logger.info(f"Per-run log:   {run_log_path}")
    logger.info(f"Persistent log: {TEST_LOGS_DIR / 'watch_tests.log'} (pytest.ini log_file)")
    logger.info("=" * 80)


def pytest_runtest_setup(item: pytest.Item) -> None:
    """
    Pytest hook: Log test start.
    """
    logger.info("\n" + "=" * 80)
    logger.info(f"TEST STARTED: {item.name}")
    logger.info(f"Module: {item.module.__name__}")
    logger.info("=" * 80)


def pytest_runtest_teardown(
    item: pytest.Item,
    nextitem: pytest.Item | None,
) -> None:
    """
    Pytest hook: Log test completion.
    """
    logger.info("=" * 80)
    logger.info(f"TEST COMPLETED: {item.name}")
    logger.info("=" * 80 + "\n")
```

---

## 6. `workers/pyproject.toml` — Add pytest Dev Dependencies

Add the following to the `[tool.poetry.group.dev.dependencies]` section (create the section if it doesn't exist):

```toml
pytest = "^8.0.0"
pytest-asyncio = "^0.23.0"
pytest-timeout = "^2.2.0"
```

Do **not** add `blake3` (CMG-specific, used for upload checksum tests in the fork).

---

## 7. `api/bin/entrypoint.sh` — Fix `APP_API_TOKEN` Generation

**File:** `api/bin/entrypoint.sh`

Find the section that generates `APP_API_TOKEN` (it calls `node src/scripts/issue_token.js`). The existing code silently writes stderr output from the Node script into `workers/.env` when the script fails, resulting in `APP_API_TOKEN=<error message>` instead of `APP_API_TOKEN=<token>`.

Replace the existing token generation block with this robust version:

```bash
echo "Generating APP_API_TOKEN"
APP_API_TOKEN=$(node src/scripts/issue_token.js <username>)
if [ $? -ne 0 ] || [ -z "$APP_API_TOKEN" ]; then
  echo "ERROR: Failed to generate APP_API_TOKEN. Error from issue_token.js:"
  node src/scripts/issue_token.js <username>
  exit 1
fi
echo "Writing APP_API_TOKEN to workers/.env"
echo "APP_API_TOKEN=$APP_API_TOKEN" > workers/.env
```

Replace `<username>` with whatever username the bioloop entrypoint uses (e.g. `admin`, `bioloopuser`, etc. — check the existing entrypoint for the correct value).

The key changes:
- Check `$?` (exit code) after the subshell command before using the result
- Check for empty string (`-z "$APP_API_TOKEN"`) as a secondary guard
- If either check fails, print a clear error and `exit 1` instead of writing garbage to the env file

---

## 8. `.ai/bioloop/worker_conventions.md` — Add Coding Conventions

Add the following two sections to the top of `worker_conventions.md` (before any existing content, or after any existing header line):

```markdown
## Type Annotations

**All Python code in `workers/` must include type annotations.** This applies to:
- Function and method signatures (parameters and return types)
- Module-level variables
- Local variables where the type is not immediately obvious from the right-hand side

### Standard imports for annotations

```python
from collections.abc import Generator
from pathlib import Path
from typing import Any
```

Use built-in generic syntax (Python 3.10+):

```python
# Correct - built-in generics, union with |
def get_datasets(
    dataset_type: str,
    name: str | None = None,
) -> list[dict[str, Any]]:
    ...

# Correct - module-level variables
FIXTURES_DIR: Path = Path(__file__).parent / 'fixtures'
_TYPE_MAP: dict[str, Path] = {'RAW_DATA': FIXTURES_DIR / 'raw_data'}

# Correct - generator (yield fixture or iterator)
def dataset_fixture() -> Generator[dict[str, Any], None, None]:
    yield {...}

# Wrong - missing annotations entirely
def get_datasets(dataset_type, name=None):
    ...

# Wrong - old-style Optional / Union from typing
from typing import Optional, Union
def get_datasets(dataset_type: str, name: Optional[str] = None) -> list:
    ...
```

---

## Method Signature Style: One Argument Per Line

When a function or method declaration has **2 or more parameters**, each parameter goes on its own line. The closing parenthesis and return annotation are on their own line.

```python
# Correct - 2+ params, one per line
def register_candidate(
    self,
    candidate: Path,
) -> None:
    ...

def await_stability(
    celery_task: WorkflowTask,
    dataset_id: int,
    wait_seconds: int | None = None,
    **kwargs: Any,
) -> tuple[int]:
    ...

# Correct - single param may stay on one line
def is_a_reject(self, name: str) -> bool:
    ...

# Wrong - multiple params on one line
def register_candidate(self, candidate: Path) -> None:
    ...

def await_stability(celery_task, dataset_id, wait_seconds=None, **kwargs):
    ...
```

This rule applies to:
- All function and method definitions in `workers/`
- Pytest fixture definitions in `workers/tests/`
- Does **not** apply to call sites (arguments at call sites follow normal line-length rules)
```

---

## 9. `workers/config/docker.py` — `recency_threshold_seconds`

The docker config should have a non-zero, realistic value for `recency_threshold_seconds`. A value of `0` causes `await_stability` to return immediately without actually checking file stability, which can mask bugs. Tests inject their own override via `wf_start_kwargs` (see conftest section), so the docker config value only affects non-test manual registrations in the docker dev environment.

```python
'recency_threshold_seconds': 5,  # fast enough for docker dev without being instant
```

---

## 10. Docker Config — Verify `source_dir` Keys

The `watched_dir` fixture reads `config['registration'][dataset_type]['source_dir']` for both `RAW_DATA` and `DATA_PRODUCT`. Open `workers/config/docker.py` (or equivalent) in bioloop and verify that both keys exist under `registration`:

```python
config = {
    'registration': {
        'RAW_DATA': {
            'source_dir': '/opt/sca/data/origin/raw_data',  # must exist
            ...
        },
        'DATA_PRODUCT': {
            'source_dir': '/opt/sca/data/origin/data_product',  # must exist
            ...
        },
    },
    ...
}
```

The exact paths depend on the bioloop docker-compose volume mounts. If these keys are absent, add them pointing to the correct `landing_volume` mount paths used by the docker-compose setup.

---

## 10. Running the Tests

From inside the `celery_worker` container (shares the same filesystem mounts as the worker tasks):

```bash
# Run only the watch script integration tests (fast, no workflow step polling)
poetry run pytest -m "integration and watch_script" -v

# Run the full suite including slow workflow step completion tests
poetry run pytest -m "integration and watch_script and integrated_wf" -v

# Skip watch tests for forks with customized logic (no code change needed)
SKIP_WATCH_SCRIPT_TESTS=true poetry run pytest -m "integration" -v
```

Log files are written to `workers/test_logs/` (inside container: `/opt/sca/app/test_logs/`, accessible on the host via the volume mount).

---

## What Does NOT Go to Bioloop

The following exist in the CMG fork but are **CMG-specific** and must not be ported:

- `_LEGACY_APP_SOURCE_DIR_KEYS`, `_get_legacy_app_source_dirs`, `_compute_dataset_origin` in `watch.py`
- `legacy_application_active` config logic in `watch.py`
- `intake_integrated` workflow references and CMG-specific observer setup (k2/k3/k4/nanopore/slate hosts) in `watch.py`
- `workers/pyproject.toml` deps: `pymongo`, `pika`, `fabric`, `blake3`, `beautifulsoup4`, `python-slugify`
- `workers/tests/conftest.py` upload fixtures (`test_data_dir`, `test_files`, `test_dataset`, `upload_log`, `temp_upload_dir`)
- `api/prisma/migrations/20260313000000_add_xenium_id/` — xenium-specific DB migration
- Any `xenium_id` column additions to Prisma schema
- CMG-specific observer metadata (`origin: 'legacy'`, CMG instrument sources)
- `.ai/` directory contents (AI memory is repo-specific)
