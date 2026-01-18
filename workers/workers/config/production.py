import datetime

YEAR = datetime.datetime.now().year

config = {
    'app_id': 'cmg-test.sca.iu.edu',
    'api': {
        'base_url': 'https://cmg-test.sca.iu.edu/api/',  # trailing slash is required
    },
    'paths': {
        'root': '/N/scratch/cmguser',
        'RAW_DATA': {
            'archive': f'archive_raw',
            'stage': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data',
            # 'stage': '/test',
            'bundle': {
                'generate': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/stage/raw_data',
                'stage': '/N/scratch/cmguser/CMG-SCA/cmg-bioloop/bundles/raw_data',
            },
            'qc': '/N/scratch/cmguser/cmg-bioloop/qc/raw_data'
        },
        'DATA_PRODUCT': {
            # 'upload': '/opt/sca/uploads/cpa/data_products',
            'archive': f'archive_products',
            'stage': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
            'bundle': {
                'generate': '/N/scratch/cmguser/cmg-bioloop/stage/data_products',
                'stage': '/N/scratch/cmguser/cmg-bioloop/bundles/data_products',
            },
        },
        'download_dir': '/N/scratch/cmguser/cmg-bioloop/production/downloads',
        'conversion': {
          # /N/project/CMG-SCA/production/conversion is the path used in CMG-Production.
          # 'reports': '/N/project/CMG-SCA/production/conversion', 
          # '/opt/sca/cmg/api/public/reports' is the path used in CMG-Production.
          # 'reports_access': '/opt/sca/cmg/api/public/reports', 
        },
    },
    'registration': {
        'RAW_DATA': {
            # k4: /zpool/novaseqx/novaseqx1
            # k3: '/data/miseq', '/data/novaseq2', '/data/ns6000'
            # k2: '/data/nextseq', '/data/ns2000/output'
            'source_dir': '/N/scratch/cmguser/cmg-bioloop/origin/raw_data',
        },
        'DATA_PRODUCT': {
            'source_dir': '/N/scratch/cmguser/cmg-bioloop/origin/data_products',
        },
        'recency_threshold_seconds': 0, # 0 minutes
    },
    'stage': {
        'purge': {
            'max_purges': 25
        },
    },
}
