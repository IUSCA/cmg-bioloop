import argparse
import logging
from datetime import datetime

from sca_rhythm import Workflow

import workers.api as api
import workers.workflow_utils as wf_utils
from workers.celery_app import app as celery_app

# Setup logging
logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    """Setup logging configuration with file and console handlers."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    log_file = f'/tmp/initiate_file_info_population_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file)
        ]
    )

    logger.info(f'Logging to file: {log_file}')


class FileInfoPopulation:
    def __init__(self):
        logger.info('Initializing FileInfoPopulation')
        self.wf_body = wf_utils.get_wf_body(wf_name='file_info_population')
        logger.debug(f'Workflow body retrieved: {self.wf_body.get("name", "unknown")}')

    def start_workflow(self, dataset_id):
        logger.info(f'Starting file_info_population workflow for dataset {dataset_id}')

        # Check if dataset exists
        logger.debug(f'Fetching dataset {dataset_id} from API')
        dataset = api.get_dataset(dataset_id=dataset_id, workflows=True)
        logger.info(f'Dataset retrieved: {dataset.get("name", "unknown")} (type: {dataset.get("type", "unknown")})')

        # Check if workflow is already running
        logger.debug('Checking for existing file_info_population workflows')
        active_workflows = [
            wf for wf in dataset.get('workflows', [])
            if wf['name'] == 'file_info_population' and wf['status'] not in ['SUCCESS', 'FAILURE', 'REVOKED']
        ]

        if active_workflows:
            logger.warning(f'Workflow already running for dataset {dataset_id}: {active_workflows[0]["id"]}')
            return

        # Start new workflow
        logger.info('Creating new workflow instance')
        wf = Workflow(celery_app=celery_app, **self.wf_body)
        workflow_id = wf.workflow['_id']
        logger.info(f'Workflow created with ID: {workflow_id}')

        logger.info(f'Adding workflow {workflow_id} to dataset {dataset_id}')
        api.add_workflow_to_dataset(dataset_id=dataset_id, workflow_id=workflow_id)

        logger.info(f'Starting workflow {workflow_id} for dataset {dataset_id}')
        wf.start(dataset_id)

        logger.info(f'Successfully started workflow {workflow_id} for dataset {dataset_id}')


if __name__ == '__main__':
    # argument parser
    parser = argparse.ArgumentParser(
        description='Initiate file_info_population workflow on a given dataset')
    parser.add_argument('dataset_id', type=str, help='Dataset ID')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')

    # Parse the command line arguments
    args = parser.parse_args()
    dataset_id = args.dataset_id

    # Setup logging
    setup_logging(args.log_level)

    logger.info(f'Script started with dataset ID: {dataset_id}')

    fip = FileInfoPopulation()
    fip.start_workflow(dataset_id)

    logger.info('Script completed')

