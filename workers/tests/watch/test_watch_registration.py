"""
Integration tests: watch.py dataset registration -> integrated workflow kickoff.

What is tested:
    - Observer detects a new directory in the watched path (the 'add' event)
    - Register.register() is called via the Observer callback
    - The API creates a dataset record for the new directory
    - An 'integrated' workflow is kicked off (created in Rhythm + linked to dataset)

What is NOT tested here:
    - Full workflow step execution (archive, validate, etc.) - those are in
      tests/watch/test_integrated_workflow_steps.py (to be written)
    - The Poller's scheduling loop - that is thin scheduling glue tested separately

How it works:
    Observer.watch() is called directly (not via the Poller's blocking loop).
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

import pytest

import workers.api as api

logger = logging.getLogger(__name__)

_skip_watch_tests = os.getenv('SKIP_WATCH_SCRIPT_TESTS', 'false').lower() == 'true'


@pytest.mark.integration
@pytest.mark.watch_script
@pytest.mark.requires_celery
@pytest.mark.skipif(_skip_watch_tests, reason='SKIP_WATCH_SCRIPT_TESTS=true in environment')
class TestWatchRegistration:
    """
    Tests that the watch script correctly detects new directories and registers
    datasets with workflows via the API.
    """

    def test_observer_detects_new_directory_and_creates_dataset(self, registered_raw_dataset):
        """
        When a new directory appears in the watched path, the Observer should
        detect it as an 'add' event and the Register callback should create a
        dataset record via the API.
        """
        dataset = registered_raw_dataset
        assert dataset is not None
        assert dataset['id'] is not None
        assert dataset['type'] == 'RAW_DATA'
        logger.info(f'Dataset created: id={dataset["id"]}, name={dataset["name"]}')

    def test_observer_detects_new_directory_and_kicks_off_integrated_workflow(self, registered_raw_dataset):
        """
        After the Observer detects a new directory and the Register creates the
        dataset, an 'integrated' workflow should be created in Rhythm and linked
        to the dataset via the API.

        Asserts:
            - The dataset has at least one associated workflow
            - The workflow was started (status is PENDING or RUNNING, not null)
        """
        dataset = registered_raw_dataset
        dataset_with_workflows = api.get_dataset(dataset_id=dataset['id'], workflows=True)

        workflows = dataset_with_workflows.get('workflows', [])

        assert len(workflows) > 0, (
            f'Expected dataset {dataset["id"]} to have at least one workflow, '
            f'but found none. The integrated workflow was not kicked off.'
        )

        workflow = workflows[0]
        workflow_id = workflow.get('id') or workflow.get('workflow_id')
        assert workflow_id is not None, (
            f'Workflow linked to dataset {dataset["id"]} has no ID. '
            f'Workflow object: {workflow}'
        )

        workflow_status = workflow.get('status')
        assert workflow_status in ('PENDING', 'RUNNING', 'COMPLETED'), (
            f'Expected workflow status to be PENDING, RUNNING, or COMPLETED, '
            f'got: {workflow_status!r}. Workflow may have failed immediately.'
        )

        logger.info(
            f'Workflow kicked off: id={workflow_id}, status={workflow_status}, '
            f'dataset_id={dataset["id"]}'
        )

    def test_observer_does_not_register_same_directory_twice(self, watched_dir, raw_data_observer, registered_raw_dataset):
        """
        If the same directory is present in the watched path on a second
        Observer.watch() cycle, it should NOT attempt to register it again
        (idempotency via the Observer's known-state diffing).

        The DatasetAlreadyExistsError path in register_candidate() handles
        the case where the directory somehow gets submitted twice, but the
        Observer itself should prevent duplicate submissions entirely.
        """
        dataset_name = registered_raw_dataset['name']

        # Trigger a second watch cycle - the directory is now in known state
        raw_data_observer.watch()

        # The dataset count should still be exactly 1
        matches = api.get_all_datasets(
            dataset_type='RAW_DATA',
            name=dataset_name,
            match_name_exact=True,
        )
        assert len(matches) == 1, (
            f'Expected exactly 1 dataset named "{dataset_name}", '
            f'but found {len(matches)}. Observer may have re-registered the directory.'
        )
        logger.info(f'Idempotency confirmed: dataset "{dataset_name}" exists exactly once')
