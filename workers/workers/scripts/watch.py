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


# _LEGACY_APP_SOURCE_DIR_KEYS: Config keys for the source directories watched by the
#  legacy CMG application for the appearance of new Datasets.
# 
# - Datasets found under these paths originate from (i.e. were registered by) the
#  legacy CMG application, not CMG-Bioloop, and are therefore tagged with
#  metadata.origin='legacy' in CMG-Bioloop.
_LEGACY_APP_SOURCE_DIR_KEYS = [
    'source_dir_ns2000',        # obs6  - k2 NS2000
    'source_dir_miseq',         # obs7  - k3 MiSeq
    'source_dir_novaseq2',      # obs8  - k3 NovaSeq2
    'source_dir_ns6000',        # obs9  - k3 NS6000
    'source_dir_novaseqx1',     # obs10 - k4 NovaSeqX1
    'source_dir_nanopore_p2solo', # obs11 - Nanopore P2Solo
    'source_dir_nanopore_p24',    # obs12 - Nanopore P24
]


def _get_legacy_app_source_dirs() -> list[Path]:
    """Return the filesystem directories that the legacy CMG application watches for the appearance of new datasets."""
    raw_data_reg = config['registration'].get('RAW_DATA', {})
    return [Path(raw_data_reg[k]) for k in _LEGACY_APP_SOURCE_DIR_KEYS if k in raw_data_reg]


def _compute_dataset_origin(candidate: Path) -> str | None:
    """
    Determine the application (among CMG and CMG-Bioloop) that this candidate dataset originated from (i.e.
    the application registered this dataset).

    Returns 'legacy' if:
      - legacy_application_active is True in config (i.e. the legacy
        CMG application is still active and running), AND
      - the candidate appears inside a directory that is watched by the legacy
        CMG application for the appearance of new datasets.

    Returns None otherwise.
    """
    if not config.get('legacy_application_active', False):
        return None
    legacy_dirs = _get_legacy_app_source_dirs()
    candidate_resolved = candidate.resolve()
    for legacy_dir in legacy_dirs:
        try:
            if candidate_resolved.is_relative_to(legacy_dir):
                return 'legacy'
        except ValueError:
            pass
    return None


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
        # add metadata to the dataset payload
        # - origin: 'legacy' if the dataset originates from the legacy CMG application
        # - other metadata from the watch script configuration
        payload_metadata = dict(self.metadata) if self.metadata else {}
        origin = _compute_dataset_origin(candidate)
        if origin:
            payload_metadata['origin'] = origin
        if payload_metadata:
            dataset_payload['metadata'] = payload_metadata
            logger.info(f'Including metadata: {payload_metadata}')

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
            # add metadata to the dataset payload
            # - origin: 'legacy' if the dataset originates from the legacy CMG application
            # - other metadata from the watch script configuration
            payload_metadata = dict(self.metadata) if self.metadata else {}
            origin = _compute_dataset_origin(candidate)
            if origin:
                payload_metadata['origin'] = origin
            if payload_metadata:
                dataset_payload['metadata'] = payload_metadata
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
    # Create dataset-observers for filesystem-spaces which are not watched by the legacy CMG application for the appearance of new Datasets.
    # At the moment, these filesystem-spaces are:
    # - Slate-scratch
    # - Slate-project
    
    # 1. Create dataset-observers for Slate-scratch
    obs1 = Observer(
        name='raw_data_obs---slate_scratch',
        dir_path=config['registration']['RAW_DATA']['source_dir_scratch'],
        callback=Register('RAW_DATA').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs2 = Observer(
        name='data_products_obs---slate_scratch',
        dir_path=config['registration']['DATA_PRODUCT']['source_dir_scratch'],
        callback=Register('DATA_PRODUCT').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    # 2. Create dataset-observers for Slate-project
    obs3 = Observer(
        name='raw_data_obs---slate_project',
        dir_path=config['registration']['RAW_DATA']['source_dir_project'],
        callback=Register('RAW_DATA').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs4 = Observer(
        name='data_products_obs---slate_project',
        dir_path=config['registration']['DATA_PRODUCT']['source_dir_project'],
        callback=Register('DATA_PRODUCT').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )

    # ------------------------------------------------------------------------------------------------

    # Create dataset-observers for filesystem-spaces which are watched by the legacy CMG application for the appearance of new Datasets.
    # At the moment, these filesystem-spaces are on hosts:
    # - Knight (k*) hosts
    # - Nanopore host
    # 
    # Note: Both Knight (k*) and Nanopore hosts are archive-only nodes. For Datasets placed on these nodes,
    # we use the `intake_integrated` workflow to ingest the Datasets, instead of the `integrated` workflow.
    
    # 1. Create dataset-observers for Knight (k*) hosts:
    # 1.1 Create dataset-observers for k2 (Compbio) host
    obs5 = Observer(
        name='raw_data_obs---k2--test',
        dir_path=config['registration']['RAW_DATA']['source_dir_test'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs5 = Observer(
        name='raw_data_obs---k2--nextseq',
        dir_path=config['registration']['RAW_DATA']['source_dir_nextseq'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs6 = Observer(
        name='raw_data_obs---k2--ns2000',
        dir_path=config['registration']['RAW_DATA']['source_dir_ns2000'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    # 1.2 Create dataset-observers for k3 (Compbio) host
    obs7 = Observer(
        name='raw_data_obs---k3--miseq',
        dir_path=config['registration']['RAW_DATA']['source_dir_miseq'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs8 = Observer(
        name='raw_data_obs---k3--novaseq2',
        dir_path=config['registration']['RAW_DATA']['source_dir_novaseq2'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs9 = Observer(
        name='raw_data_obs---k3--ns6000',
        dir_path=config['registration']['RAW_DATA']['source_dir_ns6000'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    # 1.3 Create dataset-observers for k4 (Compbio) host
    obs10 = Observer(
        name='raw_data_obs---k4--novaseqx1',
        dir_path=config['registration']['RAW_DATA']['source_dir_novaseqx1'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )

    # 2. Create dataset-observers for Nanopore host
    obs11 = Observer(
        name='raw_data_obs---nanopore--p2solo',
        dir_path=config['registration']['RAW_DATA']['source_dir_nanopore_p2solo'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )
    obs12 = Observer(
        name='raw_data_obs---nanopore--p24',
        dir_path=config['registration']['RAW_DATA']['source_dir_nanopore_p24'],
        callback=Register('RAW_DATA', default_wf_name='intake_integrated').register,
        interval=config['registration']['poll_interval_seconds'],
        full_scan_every_n_scans=config['registration']['full_scan_every_n_scans']
    )

    # Register all dataset-observers to the poller
    poller = Poller()
    poller.register(obs1)
    poller.register(obs2)
    poller.register(obs3)
    poller.register(obs4)
    poller.register(obs5)
    poller.register(obs5)
    poller.register(obs6)
    poller.register(obs7)
    poller.register(obs8)
    poller.register(obs9)
    poller.register(obs10)
    poller.register(obs11)
    poller.register(obs12)

    # Start the poller
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
