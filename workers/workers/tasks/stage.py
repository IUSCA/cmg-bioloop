import os
import shutil
import tarfile
import tempfile
from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger
from sca_rhythm import WorkflowTask

import workers.api as api
import workers.utils as utils
from workers.config import config
import workers.config.celeryconfig as celeryconfig
import workers.workflow_utils as wf_utils
from workers.dataset import compute_staging_path
from workers.dataset import get_bundle_staged_path
from workers import exceptions as exc

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


def extract_tarfile(tar_path: Path, target_dir: Path, override_arcname=False):
    """
    tar_path: path to the tar file to extract
    target_dir: path to the top level directory after extraction

    extracts the tar file to  target_dir.parent directory.

    The directory created here after extraction will have the same name as the top level directory inside the archive.

    If that is not desired and the name of the directory created needs to be the same as target_dir.name,
    then set override_arcname = True

    If a directory with the same name as extracted dir already exists, it will be deleted.
    @param tar_path:
    @param target_dir:
    @param override_arcname:
    """
    with tarfile.open(tar_path, mode='r') as archive:
        # find the top-level directory in the extracted archive
        archive_name = os.path.commonprefix(archive.getnames())
        extraction_dir = target_dir if override_arcname else (target_dir.parent / archive_name)

        # if extraction_dir exists then delete it
        if extraction_dir.exists():
            shutil.rmtree(extraction_dir)

        # create parent directories if missing
        extraction_dir.parent.mkdir(parents=True, exist_ok=True)

        # extracts the tar contents to a temp directory
        # move the contents to the extraction_dir
        with tempfile.TemporaryDirectory(dir=extraction_dir.parent) as tmp_dir:
            archive.extractall(path=tmp_dir)
            shutil.move(Path(tmp_dir) / archive_name, extraction_dir)


def stage(celery_task: WorkflowTask, dataset: dict) -> (str, str):
    """
    gets the tar from SDA or local archive and extracts it

    input: dataset['name'], dataset['archive_path'] should exist
    returns: stage_path
    """
    dataset_name = dataset.get('name', dataset.get('id'))
    staging_dir, alias = compute_staging_path(dataset)

    archived_bundle_path = dataset['archive_path']
    alias_dir = staging_dir.parent
    alias_dir.mkdir(parents=True, exist_ok=True)

    bundle = dataset["bundle"]
    bundle_md5 = bundle["md5"]
    bundle_download_path = Path(get_bundle_staged_path(dataset=dataset))

    logger.info(
        f'{dataset_name} - stage: archive_path={archived_bundle_path}, '
        f'bundle_download_path={bundle_download_path}, staging_dir={staging_dir}, alias={alias}'
    )

    # Ensure parent directory exists
    bundle_download_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f'{dataset_name} - retrieving bundle from archive')
    wf_utils.stage(archive_path=archived_bundle_path,
                   local_file_path=bundle_download_path,
                   celery_task=celery_task)

    logger.info(f'{dataset_name} - verifying bundle checksum (expected md5={bundle_md5})')
    evaluated_checksum = utils.checksum(bundle_download_path)
    if evaluated_checksum != bundle_md5:
        raise exc.ValidationFailed(f'Expected checksum of downloaded/copied file to be {bundle_md5},'
                                   f' but evaluated checksum was {evaluated_checksum}')

    logger.info(f'{dataset_name} - bundle checksum verified: {evaluated_checksum}')

    # extract the tar file to stage directory
    logger.info(f'{dataset_name} - extracting tar {bundle_download_path} to {staging_dir}')
    extract_tarfile(tar_path=bundle_download_path, target_dir=staging_dir, override_arcname=True)
    logger.info(f'{dataset_name} - extraction complete at {staging_dir}')

    # delete the local tar copy after extraction
    # bundle_path.unlink()

    return str(staging_dir), alias


def stage_dataset(celery_task, dataset_id, **kwargs):
    logger.info(f'stage_dataset called for dataset_id={dataset_id}')

    dataset = api.get_dataset(dataset_id=dataset_id, bundle=True)
    dataset_name = dataset.get('name', dataset_id)

    staged_path, alias = stage(celery_task, dataset)
    logger.info(f'{dataset_name} - staged at: {staged_path}, alias={alias}')

    update_data = {
        'staged_path': staged_path,
        'metadata': {
            'stage_alias': alias,
        }
    }
    logger.info(f'{dataset_name} - saving staged_path and alias to database')
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)

    logger.info(f'{dataset_name} - marking dataset as FETCHED')
    api.add_state_to_dataset(dataset_id=dataset_id, state='FETCHED')

    logger.info(f'{dataset_name} - stage_dataset complete')
    return dataset_id,
