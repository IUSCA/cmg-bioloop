import datetime

YEAR = datetime.datetime.now().year

APP_ID = 'cmg-test.sca.iu.edu'
FETCH_QUEUE = f'fetch.{APP_ID}.q'
ARCHIVE_QUEUE = f'archive.{APP_ID}.q'
CONVERSION_QUEUE = f'conversion.{APP_ID}.q'

config = {
    'app_id': APP_ID,
    'service_user': 'cmguser',
    'default_queue': FETCH_QUEUE,
    'fetch_queue': FETCH_QUEUE,
    'archive_queue': ARCHIVE_QUEUE,
    'conversion_queue': CONVERSION_QUEUE,
    'api': {
        'base_url': 'https://cmg-test.sca.iu.edu/api/',  # trailing slash is required
    },
    'cmg_api': {
        'base_url': 'https://cmg.sca.iu.edu/api/',  # trailing slash is required
    },
    'paths': {
        'root': '/N/scratch/cmguser',
        'RAW_DATA': {
            'stage': '/N/scratch/cmguser/cmg-bioloop/stage/raw_data',
            'migration': '/N/scratch/cmguser/cmg-bioloop/migration/raw_data',
            # 'stage': '/test',
            'bundle': {
                'generate': '/N/scratch/cmguser/cmg-bioloop/stage/raw_data',
                'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/raw_data',
            },
            'qc': '/N/scratch/cmguser/cmg-bioloop/qc/raw_data'
        },
        'DATA_PRODUCT': {
            # 'upload': '/opt/sca/uploads/cpa/data_products',
            'stage': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
            'migration': '/N/scratch/cmguser/cmg-bioloop/migration/data_products',
            'bundle': {
                'generate': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
                'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/data_products',
            },
        },
        'download_dir': '/N/scratch/cmguser/cmg-bioloop/production/downloads',
        'conversion': {
          'reports': '/N/scratch/cmguser/cmg-bioloop/conversions/reports',
          # 'reports_access': '/opt/sca/cmg/api/public/reports', 
        },
    },
    'registration': {
        'RAW_DATA': {
            # Slate-scratch Origin paths
            'source_dir_scratch': '/N/scratch/cmguser/cmg-bioloop/origin/raw_data',
            # Slate-project Origin paths
            'source_dir_project': '/N/project/CMG-SCA/cmg-bioloop/origin/raw_data',
            
            # Test Origin paths
            # 'source_dir_cmguser_1': '/home/cmguser/cmg-bioloop-ingestion-test/origin/raw_data_1',
            # 'source_dir_cmguser_2': '/home/cmguser/cmg-bioloop-ingestion-test/origin/raw_data_2',
            
            # AGENT: don't delete these commented paths below
            # K2 (Compbio) host's Origin paths
            # 'source_dir_nextseq': '/data/nextseq',
            # 'source_dir_ns2000': '/data/ns2000/output',

            # K3 (Compbio) host's Origin paths
            # 'source_dir_miseq': '/data/miseq',
            # 'source_dir_novaseq2': '/data/novaseq2',
            # 'source_dir_ns6000': '/data/ns6000',

            # K4 (Compbio) host's Origin paths
            # 'source_dir_novaseqx1': '/zpool/novaseqx/novaseqx1',
            
            # Nanopore Origin paths
            # 'source_dir_nanopore_1': '/data/p2solo',
            # 'source_dir_nanopore_2': '/zpool/p24',
        },
        'DATA_PRODUCT': {
            'source_dir_scratch': '/N/scratch/cmguser/cmg-bioloop/origin/data_products',
        },
        'recency_threshold_seconds': 300,  # 1 minute for standard Illumina datasets
        'recency_threshold_seconds_nanopore': 21600,  # 6 hours for nanopore datasets
    },
    'stage': {
        'purge': {
            'max_purges': 25
        },
    },
}
