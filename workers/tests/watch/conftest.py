"""
Fixtures for watch.py integration tests.

These tests run against real Docker services (API, Postgres, Redis, Celery).
They test the full Observer -> Register -> API -> Rhythm chain.

Requirements:
    - APP_ENV=docker (or equivalent) in workers/.env
    - Docker stack running: api, postgres, redis, celery worker, rhythm
"""

import logging
import shutil
import uuid
from pathlib import Path

import pytest

import workers.api as api
from workers.config import config
from workers.workers.scripts.watch import Register
from workers.workers.services.watchlib import Observer

logger = logging.getLogger(__name__)

FIXTURES_DIR = Path(__file__).parent / 'fixtures'


@pytest.fixture(scope='function')
def watched_dir(tmp_path):
    """
    A fresh temporary directory for the Observer to watch.
    Each test gets its own isolated watched directory.
    """
    logger.info(f'Watch test: using watched dir {tmp_path}')
    return tmp_path


@pytest.fixture(scope='function')
def raw_data_observer(watched_dir):
    """
    An Observer instance watching watched_dir, wired to a RAW_DATA Register.

    The Observer's first watch() call is made here (with an empty directory),
    establishing the initial known-state. Subsequent watch() calls in tests
    will detect newly added directories as 'add' events.

    This tests the Observer -> Register chain exactly as watch.py does in
    production, just without the Poller's blocking loop.
    """
    register = Register(dataset_type='RAW_DATA', default_wf_name='integrated')
    obs = Observer(
        name='test_raw_data_observer',
        dir_path=str(watched_dir),
        callback=register.register,
        interval=1,
    )
    # Establish initial state: Observer sees an empty directory.
    # This mirrors what happens when watch.py starts up before any datasets arrive.
    obs.watch()
    logger.info(f'Observer initialized with empty watched dir: {watched_dir}')
    return obs


@pytest.fixture(scope='function')
def registered_raw_dataset(watched_dir, raw_data_observer):
    """
    Creates a small real test dataset directory inside watched_dir (copied from
    tests/watch/fixtures/raw_data_dataset/), then triggers one Observer.watch()
    cycle to simulate the watch script detecting a new dataset.

    Yields the registered dataset dict (fetched from the API after registration).

    Teardown: deletes the dataset from the API and removes the directory.
    """
    # Give the dataset a unique name so parallel test runs don't collide
    dataset_name = f'test_watch_{uuid.uuid4().hex[:12]}'
    dataset_path = watched_dir / dataset_name

    fixture_src = FIXTURES_DIR / 'raw_data_dataset'
    shutil.copytree(fixture_src, dataset_path)
    logger.info(f'Copied fixture to: {dataset_path}')

    # Trigger the Observer: it diffs current dirs against known state and
    # calls Register.register('add', [dataset_path]) for the new directory.
    raw_data_observer.watch()
    logger.info(f'Observer.watch() triggered - should have registered: {dataset_name}')

    # Fetch the created dataset from the API to verify and return it
    matches = api.get_all_datasets(
        dataset_type='RAW_DATA',
        name=dataset_name,
        match_name_exact=True,
    )
    if not matches:
        pytest.fail(
            f'Dataset "{dataset_name}" was not found in the API after Observer.watch(). '
            'Check that the API is reachable and APP_API_TOKEN is set correctly.'
        )

    dataset = api.get_dataset(dataset_id=matches[0]['id'], workflows=True)
    logger.info(f'Registered dataset: id={dataset["id"]}, name={dataset["name"]}')

    yield dataset

    # Teardown: remove dataset from API and filesystem
    try:
        api.delete_dataset(dataset['id'])
        logger.info(f'Deleted test dataset from API: {dataset["id"]}')
    except Exception as e:
        logger.warning(f'Failed to delete test dataset {dataset["id"]} from API: {e}')

    if dataset_path.exists():
        shutil.rmtree(dataset_path)
        logger.info(f'Removed test dataset directory: {dataset_path}')
