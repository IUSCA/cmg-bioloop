import fnmatch
import logging
from pathlib import Path

from sca_rhythm import Workflow

import workers.api as api
import workers.workflow_utils as wf_utils
from workers.api import DatasetAlreadyExistsError
from workers.celery_app import app as celery_app
from workers.config import config
from workers.services.watchlib import Observer, Poller
from workers.utils import batched

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Register:
    def __init__(self, dataset_type, default_wf_name='integrated', **kwargs):
        self.dataset_type = dataset_type
        self.reg_config = config['registration'][self.dataset_type]
        self.reject_patterns: set[str] = set(self.reg_config['rejects'])
        self.default_wf_name = default_wf_name
        self.batch_size: int = 100
        self.metadata = kwargs

    def is_a_reject(self, name) -> bool:
        return any([fnmatch.fnmatchcase(name, pat) for pat in self.reject_patterns])

    def register(self, event: str, new_dirs: list[Path]) -> None:
        logger.info(f'=== REGISTRATION EVENT START ===')
        logger.info(f'Event type: {event}, Total directories found: {len(new_dirs)}')
        for new_dir in new_dirs:
            logger.info(f'Found directory: {new_dir.name}')
        
        if event not in ['add', 'full_scan']:
            logger.info(f'Skipping registration - event type "{event}" not in allowed types [add, full_scan]')
            return

        logger.info(f'Processing {len(new_dirs)} directories for {self.dataset_type} registration')
        logger.info(f'Reject patterns configured: {list(self.reject_patterns)}')

        # apply node level rules to filter out bad directories
        candidates = [p for p in new_dirs if not self.is_a_reject(p.name)]
        rejected_dirs = [p.name for p in new_dirs if self.is_a_reject(p.name)]

        logger.info(f'Directories rejected by patterns: {len(rejected_dirs)}')
        if rejected_dirs:
            logger.info(f'Rejected directories: {rejected_dirs}')
        
        logger.info(f'Valid candidates for registration: {len(candidates)}')
        if candidates:
            logger.info(f'Candidate directories: {[c.name for c in candidates]}')

        # for candidate in candidates:
        #     try:
        #         self.register_candidate(candidate)
        #     except Exception as e:
        #         logger.error(f'Error registering dataset {candidate.name}: {e}')

        if not candidates:
            logger.info('No valid candidates found - skipping API calls')
            logger.info('=== REGISTRATION EVENT END ===')
            return

        # if we are ingesting a full directory of 1000+ subdirectories, we may want to batch the requests
        logger.info(f'Processing candidates in batches of {self.batch_size}')
        batch_count = 0
        for batch in batched(candidates, n=self.batch_size):
            batch_count += 1
            logger.info(f'Processing batch {batch_count} with {len(batch)} candidates')
            self.register_batch(batch)
        
        logger.info('=== REGISTRATION EVENT END ===')

    def register_candidate(self, candidate: Path) -> None:
        # idempotence: if dataset already exists, do nothing
        # fault tolerance:
        #  possibility 1: error happened before dataset creation - skipping is okay, because we can try again
        #  possibility 2: error happened after dataset creation - somehow need to add workflow to dataset
        #       because when we retry, it will raise DatasetAlreadyExistsError.
        #  Track datasets without workflows on the UI and trigger a workflow manually.
        #  Option 1: infer from the dataset state
        #  - Avoid datasets that are just created
        #  - Avoid datasets that are already processed but their associated workflows are deleted (updated date will be recent)
        #  Option 2:
        #   - keep track of failures
        logger.info(f'--- REGISTERING SINGLE CANDIDATE ---')
        logger.info(f'Candidate: {candidate.name} (type: {self.dataset_type})')
        logger.info(f'Origin path: {candidate.resolve()}')
        
        dataset_payload = {
            'name': candidate.name,
            'type': self.dataset_type,
            'origin_path': str(candidate.resolve()),
        }
        if self.metadata:
            dataset_payload['metadata'] = self.metadata
            logger.info(f'Including metadata: {self.metadata}')
        
        logger.info(f'Making API call to create dataset: {candidate.name}')
        try:
            created_dataset = api.create_dataset(dataset_payload)
            logger.info(f'Successfully created dataset: {created_dataset["name"]} (ID: {created_dataset["id"]})')
            self.run_workflows(created_dataset)
        except DatasetAlreadyExistsError:
            logger.info(f'Dataset {candidate.name} already exists - skipping')
            return

    def register_batch(self, candidates: list[Path]) -> None:
        # fault tolerance: similar to register_candidate, the problem is when failure happens after the dataset creation
        # - we can track datasets without workflows on the UI and trigger a workflow manually
        logger.info(f'--- REGISTERING BATCH OF {len(candidates)} CANDIDATES ---')
        logger.info(f'Batch candidates: {[c.name for c in candidates]}')
        
        data = []
        for candidate in candidates:
            dataset_payload = {
                'name': candidate.name,
                'type': self.dataset_type,
                'origin_path': str(candidate.resolve()),
            }
            if self.metadata:
                dataset_payload['metadata'] = self.metadata
            data.append(dataset_payload)

        logger.info(f'Making bulk API call to create {len(data)} datasets')
        try:
            # failure point but has built in retry ability
            result = api.bulk_create_datasets(data)
            # result looks like {created: [], conflicted: [], errored: []}
            logger.info(f'Bulk API call result - Created: {len(result["created"])}, Conflicted: {len(result["conflicted"])}, Errored: {len(result["errored"])}')
            
            if result['created']:
                logger.info(f'Successfully created datasets: {[d["name"] for d in result["created"]]}')
            if result['conflicted']:
                logger.info(f'Datasets already existed: {[d["name"] for d in result["conflicted"]]}')
            if result['errored']:
                logger.error(f'Failed to create datasets: {[d["name"] for d in result["errored"]]}')
            
            # only create workflows for created datasets
            for dataset in result['created']:
                try:
                    self.run_workflows(dataset)
                except Exception as e:
                    logger.error(f'Error running workflows for dataset {dataset["name"]}: {e}')
        except Exception as e:
            logger.error(f'Error bulk creating datasets: {e}')

    def run_workflows(self, dataset):
        logger.info(f'--- STARTING WORKFLOW FOR DATASET ---')
        logger.info(f'Dataset: {dataset["name"]} (ID: {dataset["id"]}, Type: {self.dataset_type})')
        dataset_id = dataset['id']
        
        logger.info(f'Getting workflow body for: {self.default_wf_name}')
        wf_body = wf_utils.get_wf_body(wf_name=self.default_wf_name)

        logger.info(f'Creating workflow in MongoDB')
        # connects to mongodb to create a document in the workflows collection - failure point
        wf = Workflow(celery_app=celery_app, **wf_body)
        logger.info(f'Created workflow with ID: {wf.workflow["_id"]}')

        logger.info(f'Adding workflow to dataset via API call')
        # connects to API - failure point - has built in retry ability
        api.add_workflow_to_dataset(dataset_id=dataset_id, workflow_id=wf.workflow['_id'])
        logger.info(f'Successfully linked workflow {wf.workflow["_id"]} to dataset {dataset_id}')

        logger.info(f'Starting workflow execution via Celery')
        # connects to celery - failure point
        wf.start(dataset_id)
        logger.info(f'Workflow started for dataset {dataset["name"]}')


class RegisterDataProduct(Register):
    def __init__(self):
        super().__init__(dataset_type='DATA_PRODUCT')

    def run_workflows(self, dataset):
        pass


if __name__ == "__main__":
    obs1 = Observer(
        name='raw_data_obs_1',
        dir_path=config['registration']['RAW_DATA']['source_dir_cmguser_1'],
        callback=Register('RAW_DATA').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs2 = Observer(
        name='raw_data_obs_2',
        dir_path=config['registration']['RAW_DATA']['source_dir_cmguser_2'],
        callback=Register('RAW_DATA').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    # obs3 = Observer(
    #     name='data_products_obs',
    #     dir_path=config['registration']['DATA_PRODUCT']['source_dir'],
    #     callback=Register('DATA_PRODUCT').register,
    #     # callback=RegisterDataProduct().register,
    #     interval=config['registration']['poll_interval_seconds'],
    #     full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    # )

    poller = Poller()
    poller.register(obs1)
    poller.register(obs2)
    poller.poll()

    # try:
    #     with RabbitMqConsumer(queue_name='registration') as consumer:
    #         while True:
    #             for message in consumer.consume_messages():
    #                 logger.info(f"Received message: {message}")
    #                 # process messages here
    #             poller.poll(loop=False)
    # except KeyboardInterrupt:
    #     logger.info('KeyboardInterrupt received. Exiting.')
