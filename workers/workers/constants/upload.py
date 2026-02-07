UPLOAD_RETRY_THRESHOLD_HOURS = 72
MAX_RETRY_COUNT = 3

UPLOAD_STATUS = {
    'UPLOADING': 'UPLOADING',
    'UPLOAD_FAILED': 'UPLOAD_FAILED',
    'UPLOADED': 'UPLOADED',
    'VERIFYING': 'VERIFYING',  # Integrity verification in progress (async Celery task)
    'VERIFIED': 'VERIFIED',  # Integrity verified, ready to trigger workflow
    'VERIFICATION_FAILED': 'VERIFICATION_FAILED',  # Integrity check failed before workflow
    'PROCESSING': 'PROCESSING',
    'PROCESSING_FAILED': 'PROCESSING_FAILED',
    'COMPLETE': 'COMPLETE',
    'PERMANENTLY_FAILED': 'PERMANENTLY_FAILED',
}
