import datetime
import os
import urllib.parse

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.
YEAR = datetime.datetime.now().year
APP_API_TOKEN = os.environ['APP_API_TOKEN']
# print(f'APP_API_TOKEN: {APP_API_TOKEN}')

API_BASE_URL = os.environ['API_BASE_URL']
CMG_API_BASE_URL = os.environ['CMG_API_BASE_URL']
CMG_API_TOKEN = os.environ['CMG_API_TOKEN']

QUEUE_URL = os.environ['QUEUE_URL']
QUEUE_USER = os.environ['QUEUE_USER']
QUEUE_PASSWORD = os.environ['QUEUE_PASS']

MONGO_HOST = os.environ['MONGO_HOST']
MONGO_PORT = os.environ['MONGO_PORT']
MONGO_DB = os.environ['MONGO_DB']
MONGO_AUTH_SOURCE = os.environ['MONGO_AUTH_SOURCE']
MONGO_USER = os.environ['MONGO_USER']
MONGO_PASSWORD = os.environ['MONGO_PASS']

ALIAS_SALT = os.environ['ALIAS_SALT']

ONE_HOUR = 60 * 60
ONE_GIGABYTE = 1024 * 1024 * 1024
FIVE_MINUTES = 5 * 60

config = {
    'app_id': 'cmg-test.sca.iu.edu',
    # cspell: disable-next-line
    'genome_file_types': ['.cbcl', '.bcl', '.bcl.gz', '.bgzf', '.fastq.gz', '.bam', '.bam.bai', '.vcf.gz',
                          '.vcf.gz.tbi', '.vcf'],
    'trackable_extensions': ['.bam', '.bw', '.bigwig', '.vcf'],
    'api': {
        'base_url': API_BASE_URL,
        'auth_token': APP_API_TOKEN,
        'conn_timeout': 5,  # seconds
        'read_timeout': 30  # seconds
    },
    'cmg_api': {
        'base_url': CMG_API_BASE_URL,
        'auth_token': CMG_API_TOKEN,
        'conn_timeout': 5,  # seconds
        'read_timeout': 30  # seconds
    },
    'paths': {
        'scratch': '/path/to/scratch',
        'RAW_DATA': {
            'archive': f'development/{YEAR}/raw_data',
            # archive_legacy: the legacy CMG application's archive path for Raw Data (Sequencing Runs).
            'archive_legacy': 'archive_raw',
            'stage': '/path/to/staged/raw_data',
            'migration': '/path/to/migration/raw_data',
            'bundle': {
                'generate': '/path/for/raw_data/bundle/generation',
                'stage': '/path/for/raw_data/bundle/staging',
            },
            'qc': '/path/to/qc'
        },
        'DATA_PRODUCT': {
            'upload': '/opt/sca/data',
            'archive': f'development/{YEAR}/data_products',
            # archive_legacy: the legacy CMG application's archive path for Data Products.
            'archive_legacy': 'archive_products',
            'stage': '/path/to/staged/data_products',
            'migration': '/path/to/migration/data_products',
            'bundle': {
                'generate': '/path/for/data_products/bundle/generation',
                'stage': '/path/for/data_products/bundle/staging',
            },
        },
        'download_dir': '/path/to/download_dir',
        'conversion': {
          'reports': '/path/to/conversion_reports',
          'reports_access': '/path/to/access/conversion_reports',
        },
        'root': '/path/to/root'
    },
    'registration': {
        'RAW_DATA': {
            'source_dir': '/path/to/source/raw_data',
            'rejects': ['.snapshots', 'Log Files', 'OldFolders'],
        },
        'DATA_PRODUCT': {
            'source_dir': '/path/to/source/data_products',
            'rejects': ['.snapshots', 'Log Files', 'OldFolders'],
        },
        'recency_threshold_seconds': 30,  # 30 seconds for standard Illumina datasets
        'recency_threshold_seconds_nanopore': 21600,  # 6 hours for nanopore datasets
        'minimum_dataset_size': ONE_GIGABYTE,
        'wait_between_stability_checks_seconds': FIVE_MINUTES,
        'poll_interval_seconds': 10,
        'full_scan_every_n_scans': 90  # every 90th scan will be a full scan / full scan every 15 minutes
    },
    'service_user': 'bioloopuser',
    'stage': {
        'purge': {
            'days_to_live': 20,
            'max_purges': 10
        },
        'alias_salt': ALIAS_SALT
    },
    'workflow_registry': {
        'stage': {
            'steps': [
                {
                    'name': 'stage',
                    'task': 'stage_dataset'
                },
                {
                    'name': 'validate',
                    'task': 'validate_dataset'
                },
                {
                    'name': 'setup_download',
                    'task': 'setup_dataset_download'
                }
            ]
        },
        'stage_migrated': {
            'description': 'Stage and hydrate legacy CMG datasets with file and track metadata',
            'steps': [
                {
                    'name': 'begin_migration',
                    'task': 'begin_migration'
                },
                {
                    'name': 'retrieve_archive',
                    'task': 'retrieve_archive_dataset'
                },
                {
                    'name': 'inspect',
                    'task': 'inspect_dataset'
                },
                {
                    'name': 'populate_metadata',
                    'task': 'populate_metadata_dataset'
                },
                {
                    'name': 'stage',
                    'task': 'stage_dataset'
                },
                {
                    'name': 'validate',
                    'task': 'validate_dataset'
                },
                {
                    'name': 'setup_download',
                    'task': 'setup_dataset_download'
                },
                {
                    'name': 'end_migration',
                    'task': 'end_migration'
                }
            ]
        },
        'integrated': {
            'steps': [
                {
                    'name': 'await stability',
                    'task': 'await_stability'
                },
                {
                    'name': 'inspect',
                    'task': 'inspect_dataset'
                },
                {
                    'name': 'archive',
                    'task': 'archive_dataset'
                },
                {
                    'name': 'stage',
                    'task': 'stage_dataset'
                },
                {
                    'name': 'validate',
                    'task': 'validate_dataset'
                },
                {
                    'name': 'setup_download',
                    'task': 'setup_dataset_download'
                },
                # IMPORTANT:
                # The delete_source step of the Integrated workflow
                # should NOT be enabled if the legacy CMG application is also registering
                # new Datasets from the same source directory. The delete_source step
                # being run in such situations could possibly delete the source directory
                # before the legacy CMG application can finish registering the same dataset.
            ]
        },
        'process_dataset_upload': {
            'steps': [
                {
                    'name': 'Process Dataset Upload',
                    'task': 'process_dataset_upload'
                }
            ]
        },
        'cancel_dataset_upload': {
            'steps': [
                {
                    'name': 'Cancel Dataset Upload',
                    'task': 'cancel_dataset_upload'
                }
            ]
        },
        "conversion": {
          "name": "Conversion",
          "steps": [
            {
              "name": "convert",
              "task": "convert_dataset",
              "queue": "conversion.cmg-test.sca.iu.edu.q"
            },

          ]
        },
        "genomic_conversion": {
          "name": "Genomic Conversion",
          "steps": [
            {
              "name": "convert",
              "task": "convert_genomic",
              "queue": "conversion.cmg-test.sca.iu.edu.q"
            },
            {
              "name": "generate qc",
              "task": "generate_qc",
              "queue": "conversion.cmg-test.sca.iu.edu.q"
            },
            {
              "name": "copy reports",
              "task": "copy_conversion_reports",
              "queue": "conversion.cmg-test.sca.iu.edu.q"
            },
            {
              "name": "derive data products",
              "task": "derive_data_products",
              "queue": "conversion.cmg-test.sca.iu.edu.q"
            }
          ]
        },
        "file_info_population": {
          "name": "File Info Population",
          "steps": [
            {
              "name": "populate file metadata",
              "task": "populate_file_metadata"
            },
            # {
            #   "name": "archive",
            #   "task": "archive_dataset"
            # },
            # {
            #   "name": "stage",
            #   "task": "stage_dataset"
            # },
            # {
            #   "name": "validate",
            #   "task": "validate_dataset"
            # },
            # {
            #   "name": "delete_source",
            #   "task": "delete_source"
            # }
          ]
        },
        "hydrate_session": {
          "name": "Hydrate Session",
          "description": "Hydrate legacy CMG Session with Tracks",
          "steps": [
            {
              "name": "hydrate_tracks",
              "task": "hydrate_session_tracks"
            },
            {
              "name": "finish_hydration",
              "task": "finish_session_hydration"
            }
          ]
        }
    },
    'celery': {
        'queue': {
            'url': QUEUE_URL,
            'username': QUEUE_USER,
            'password': QUEUE_PASSWORD
        },
        'mongo': {
            'uri': f'mongodb://{MONGO_USER}:{urllib.parse.quote(MONGO_PASSWORD)}@{MONGO_HOST}:{MONGO_PORT}/{MONGO_DB}?authSource={MONGO_AUTH_SOURCE}',
        }
    },
    'email': {
        'from_addr': 'scauser@iu.edu',
        'sendmail_path': '/usr/sbin/sendmail'
    },
    'qc_tools': {
        'fastqc_path': 'fastqc',  # Available in PATH
        'multiqc_path': '/opt/sca/cmg/miniconda/bin/multiqc'
    },
    'workflow': {
        'purge': {
            'types': ['integrated', 'stage', 'delete', 'conversion', 'file_info_population'],
            'age_threshold_seconds': 86400,
            'max_purge_count': 10
        }
    },
    'inspect': {
        'file_metadata_batch_size': 25000
    },
    'genomic_conversion_programs': [
        'bcl2fastq', 'bcl-convert', 'cellranger-v8.0.1', 'cellranger-v6.1.2', 
        'cellranger-v4.0.0', 'cellranger-arc', 'cellranger-arc-v2', 
        'cellranger-atac', 'spaceranger-v3.0.1', 'spaceranger-v1.3.1', 
        'spaceranger-v1.1.0'
    ],
    'genomic_conversion': {
        'default_analysis_type': {
            'enabled': True,  # Set to True to override default behavior
            'value': 'fastq'      # Set to desired Analysis Type (e.g., 'FASTQ_CLEANED')
        },
        'qc': {
            'enabled': True,  # Set to False to skip QC generation (requires fastqc and multiqc)
        }
    },
    'file_info_population': {
        'batch_size': 10,
        'max_download_size_tb': 10,
        'download_dir': '/opt/sca/data/file_info_downloads',
        'state_file': '/opt/sca/data/file_info_population_state.json',
        'skip_sda_upload': True,  # Skip SDA upload in archive step
        'poll_interval_seconds': 300,  # 5 minutes between batch completion checks
        'max_retries_per_dataset': 3
    },
    'execution_platform': {
        # 'KUBERNETES': { },
        # 'AWS_BATCH': { },
        # 'CUSTOM': { },
        'SLURM': {
            'connection': {
                'type': 'ssh',
                'host': 'h1.quartz.uits.iu.edu',
                'user': 'cmguser',
                'private_key': '~/.ssh/id_rsa',
            },
            'slurm_script_dir': '/slurm_scripts',
        },
      'legacy_migration': {
        'completed': False,
      }
    },
}
