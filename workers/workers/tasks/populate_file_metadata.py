from pathlib import Path

from celery import Celery
from celery.utils.log import get_task_logger

import workers.api as api
import workers.cmd as cmd
import workers.config.celeryconfig as celeryconfig
import workers.utils as utils
from workers import exceptions as exc
from workers.config import config
from workers.tasks.inspect import generate_metadata

app = Celery("tasks")
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)

# This task is used to hydrate the file metadata of a previously-archived dataset.
# The Dataset is expected to have been staged before running this task.
def populate_file_metadata(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id=dataset_id)
    source = Path(dataset['staged_path']).resolve()
    du_size = cmd.total_size(source)
    num_files, num_directories, size, num_genome_files, metadata = generate_metadata(celery_task, source)

    update_data = {
        'du_size': du_size,
        'size': size,
        'num_files': num_files,
        'num_directories': num_directories,
        'metadata': {
            'num_genome_files': num_genome_files,
        }

    }
    api.update_dataset(dataset_id=dataset_id, update_data=update_data)
    # split metadata into batches and add to dataset
    # this is to avoid large payloads to the API
    for batch in utils.batched(metadata, n=config['inspect']['file_metadata_batch_size']):
        api.add_files_to_dataset(dataset_id=dataset_id, files=batch)

    return dataset_id,
