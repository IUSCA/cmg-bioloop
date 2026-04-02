import datetime

YEAR = datetime.datetime.now().year

config = {
    'mode': 'production',
    'app_id': 'cmg-test.sca.iu.edu',
    'default_queue': 'cmg-bioloop-fetch.cmg-test.sca.iu.edu.q',
    'fetch_queue': 'cmg-bioloop-fetch.cmg-test.sca.iu.edu.q',
    'archive_queue': 'cmg-bioloop-archive.cmg-test.sca.iu.edu.q',
    'conversion_queue': 'cmg-bioloop-conversion.cmg-test.sca.iu.edu.q',
    'api': {
        'base_url': 'https://cmg-test.sca.iu.edu/api/',  # trailing slash is required
    },
    'cmg_api': {
        'base_url': 'https://cmg.sca.iu.edu/api/',  # trailing slash is required
    },
    'paths': {
        'root': '/N/scratch/cmguser',
        'RAW_DATA': {
            'archive': f'production/{YEAR}/raw_data',
            'stage': '/N/scratch/cmguser/cmg-bioloop/stage/raw_data',
            'migration': '/N/scratch/cmguser/cmg-bioloop/migration/raw_data',
            # 'stage': '/test',
            'bundle': {
                'generate': '/N/scratch/cmguser/cmg-bioloop/stage/raw_data',
                # generate_archive_node: used by archive nodes (k2/k3/k4) that don't have /N/scratch access
                'generate_archive_node': '/tmp/cmg-bioloop/bundles/raw_data',
                'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/raw_data',
            },
            'qc': '/N/scratch/cmguser/cmg-bioloop/qc/raw_data'
        },
        'DATA_PRODUCT': {
            'archive': f'production/{YEAR}/data_products',
            # 'upload': '/opt/sca/uploads/cpa/data_products',
            'stage': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
            'migration': '/N/scratch/cmguser/cmg-bioloop/migration/data_products',
            'bundle': {
                'generate': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
                # generate_archive_node: used by archive nodes (k2/k3/k4) that don't have /N/scratch access
                'generate_archive_node': '/tmp/cmg-bioloop/bundles/data_products',
                'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/data_products',
            },
            'qc': '/N/scratch/cmguser/cmg-bioloop/qc/data_products',
        },
        'download_dir': '/N/scratch/cmguser/cmg-bioloop/production/downloads',
        'conversion': {
          'reports': '/N/scratch/cmguser/cmg-bioloop/conversions/reports',
          'qc_reports': '/N/scratch/cmguser/cmg-bioloop/conversions/qc_reports',
          # 'reports_access': '/opt/sca/cmg/api/public/reports', 
        },
    },
    'registration': {
        'RAW_DATA': {
            'source_dir_scratch': '/N/scratch/cmguser/cmg-bioloop/origin/raw_data',
            'source_dir_project': '/N/project/CMG-SCA/cmg-bioloop/origin/raw_data',
            
            # AGENT: don't delete these commented paths below
            # K2 (Compbio) host's Origin paths
            'source_dir_test': '/home/cmguser/cmg-bioloop-ingestion-test/origin/raw_data_1',
            'source_dir_nextseq': '/data/nextseq',
            'source_dir_ns2000': '/data/ns2000/output',

            # K3 (Compbio) host's Origin paths
            'source_dir_miseq': '/data/miseq',
            'source_dir_novaseq2': '/data/novaseq2',
            'source_dir_ns6000': '/data/ns6000',

            # K4 (Compbio) host's Origin paths
            'source_dir_novaseqx1': '/zpool/novaseqx/novaseqx1',
            
            # Nanopore Origin paths
            # NOTE: Bioloop workers use the below dict's keys to detect whether or not a Dataset should be
            # considered a 'nanopore' dataset. If these keys are changed, the behavior of method
            # ``is_nanopore_dataset()`` in ``dataset.py`` will need to be updated accordingly.
            'source_dir_nanopore_p2solo': '/data/p2solo',
            'source_dir_nanopore_p24': '/zpool/p24/data',
        },
        'DATA_PRODUCT': {
            'source_dir_scratch': '/N/scratch/cmguser/cmg-bioloop/origin/data_products',
            'source_dir_project': '/N/project/CMG-SCA/cmg-bioloop/origin/data_products',

            # K2 (Compbio) host's Origin paths
            # 'source_dir_test': '/home/cmguser/cmg-bioloop-ingestion-test/origin/data_products',
        },
        'recency_threshold_seconds': 0,  # 1 minute for standard Illumina datasets
        'recency_threshold_seconds_nanopore': 21600,  # 6 hours for nanopore datasets
    },
    'stage': {
        'purge': {
            'max_purges': 25
        },
    },
    'genomic_conversion': {
        'qc': {
            'use_conversion_dirs': True,
        }
    },
}
