"""
Upload Verification Task

Standalone Celery task for async upload integrity verification.
Not a WorkflowTask - fire-and-forget with status tracking via dataset_upload_log.

Handles BLAKE3 manifest verification for large files with:
- Streaming hash (16MB chunks, Lustre-optimized)
- 24-hour timeout
- Auto-retry (max 3 attempts)
- Detailed failure logging for each failure mode
"""

import logging
from datetime import datetime

from celery.exceptions import SoftTimeLimitExceeded

from workers import api
from workers.constants.upload import UPLOAD_STATUS
from workers.upload import verify_upload_integrity

logger = logging.getLogger(__name__)


def verify_upload_integrity(celery_task, dataset_id):
    """
    Verify upload integrity and update status.
    
    Args:
        celery_task: Celery task instance (self)
        dataset_id: Dataset ID to verify
        
    Returns:
        dict: Verification result with status
        
    Raises:
        Various exceptions that Celery will auto-retry
    """
    task_id = celery_task.request.id
    retry_count = celery_task.request.retries
    worker_process_id = None
    
    logger.info("="*80)
    logger.info(f"UPLOAD VERIFICATION TASK STARTED")
    logger.info(f"Dataset ID: {dataset_id}")
    logger.info(f"Task ID: {task_id}")
    logger.info(f"Retry attempt: {retry_count + 1}/{celery_task.max_retries + 1}")
    logger.info("="*80)
    
    try:
        # Fetch dataset and upload log
        logger.info(f"Fetching dataset {dataset_id}...")
        dataset = api.get_dataset(dataset_id=dataset_id, workflows=True)
        upload_log = api.get_dataset_upload_log(dataset_id)
        
        # Create or retrieve worker_process record for log tracking
        # Always persist logs, not just on first attempt
        try:
            # Check if worker_process already exists (from metadata)
            existing_process_id = (upload_log.get('metadata') or {}).get('worker_process_id')
            
            if existing_process_id:
                logger.info(f"Using existing worker_process: {existing_process_id}")
                worker_process_id = existing_process_id
                # Log retry attempt
                api.post_worker_logs(worker_process_id, [{
                    'message': f'Retry attempt {retry_count + 1}/{celery_task.max_retries + 1} started',
                    'level': 'info',
                    'timestamp': datetime.utcnow().isoformat(),
                }])
            else:
                logger.info("Creating worker_process record for log tracking...")
                worker_process = api.register_process({
                    'task_id': task_id,
                    'task_name': 'verify_upload_integrity',
                    'dataset_id': dataset_id,
                    'status': 'STARTED',
                    'started_at': datetime.utcnow().isoformat(),
                    'metadata': {
                        'upload_log_id': upload_log.get('id'),
                        'verification_type': 'upload_integrity',
                    }
                })
                worker_process_id = worker_process.get('id')
                logger.info(f"✓ Worker process created: {worker_process_id}")
                
                # Update upload log with worker_process_id
                api.update_dataset_upload_log(
                    dataset_id=dataset_id,
                    log_data={
                        'metadata': {
                            **(upload_log.get('metadata') or {}),
                            'worker_process_id': worker_process_id,
                        }
                    }
                )
        except Exception as e:
            logger.warning(f"Failed to create/retrieve worker_process record: {e}")
            # Non-critical - continue without it
        
        dataset_name = dataset.get('name')
        origin_path = dataset.get('origin_path')
        
        logger.info(f"Dataset name: {dataset_name}")
        logger.info(f"Origin path: {origin_path}")
        logger.info(f"Current status: {upload_log.get('status')}")
        
        # Verify integrity (idempotent - safe to run multiple times)
        logger.info("Starting integrity verification...")
        logger.info("This may take a while for large datasets (up to 24 hours for very large files)")
        
        verify_upload_integrity(dataset, upload_log)
        
        logger.info("✓ Integrity verification PASSED")
        logger.info(f"Updating status to {UPLOAD_STATUS['VERIFIED']}...")
        
        # Update status to VERIFIED
        api.update_dataset_upload_log(
            dataset_id=dataset_id,
            log_data={'status': UPLOAD_STATUS['VERIFIED']}
        )
        
        # Log completion to worker_process
        if worker_process_id:
            try:
                api.post_worker_logs(worker_process_id, [{
                    'message': f'✓ Verification completed successfully for dataset {dataset_id}',
                    'level': 'info',
                    'timestamp': datetime.utcnow().isoformat(),
                }])
            except Exception as e:
                logger.warning(f"Failed to post completion log: {e}")
        
        logger.info("="*80)
        logger.info("UPLOAD VERIFICATION TASK COMPLETED SUCCESSFULLY")
        logger.info(f"Dataset ID: {dataset_id}")
        logger.info(f"Dataset name: {dataset_name}")
        logger.info(f"Worker process ID: {worker_process_id}")
        logger.info(f"Expected resolution: manage_upload_workflows.py will pick this up on next run")
        logger.info(f"                     and trigger integrated workflow")
        logger.info("="*80)
        
        return {
            'status': 'success',
            'dataset_id': dataset_id,
            'dataset_name': dataset_name,
            'worker_process_id': worker_process_id,
        }
        
    except SoftTimeLimitExceeded:
        # Task exceeded 23h 55m soft limit
        logger.error("="*80)
        logger.error("FAILURE MODE: SOFT TIME LIMIT EXCEEDED")
        logger.error(f"Dataset ID: {dataset_id}")
        logger.error(f"Task ID: {task_id}")
        logger.error(f"Worker process ID: {worker_process_id}")
        logger.error("The verification task exceeded 23 hours 55 minutes")
        logger.error("This typically indicates:")
        logger.error("  - Extremely large files (>200GB)")
        logger.error("  - Slow disk I/O on the filesystem")
        logger.error("  - Too many files to process")
        logger.error(f"Expected resolution: Status set to {UPLOAD_STATUS['VERIFICATION_FAILED']}")
        logger.error("                     Admin will be notified to investigate")
        logger.error("                     May need to disable checksum verification for this upload")
        logger.error("="*80)
        
        # Mark upload as failed
        api.update_dataset_upload_log(
            dataset_id=dataset_id,
            log_data={
                'status': UPLOAD_STATUS['VERIFICATION_FAILED'],
                'metadata': {
                    'failure_reason': f'Verification timeout (>23h 55m). Task ID: {task_id}',
                    'task_id': task_id,
                    'worker_process_id': worker_process_id,
                    'failed_at': datetime.utcnow().isoformat(),
                }
            }
        )
        
        # Log timeout failure to worker_process
        if worker_process_id:
            try:
                api.post_worker_logs(worker_process_id, [{
                    'message': f'✗ Verification timeout (>23h 55m) for dataset {dataset_id}',
                    'level': 'error',
                    'timestamp': datetime.utcnow().isoformat(),
                }])
            except Exception as e:
                logger.warning(f"Failed to post timeout log: {e}")
        
        raise  # Re-raise for Celery to log
        
    except Exception as e:
        # Catchable exception - Celery will auto-retry
        is_final_retry = retry_count >= celery_task.max_retries
        
        logger.error("="*80)
        if is_final_retry:
            logger.error("FAILURE MODE: VERIFICATION EXCEPTION (FINAL RETRY)")
        else:
            logger.error("FAILURE MODE: VERIFICATION EXCEPTION (WILL RETRY)")
        logger.error(f"Dataset ID: {dataset_id}")
        logger.error(f"Task ID: {task_id}")
        logger.error(f"Retry attempt: {retry_count + 1}/{celery_task.max_retries + 1}")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Error type: {type(e).__name__}")
        
        if is_final_retry:
            logger.error("This was the final retry attempt")
            logger.error(f"Expected resolution: Status set to {UPLOAD_STATUS['VERIFICATION_FAILED']}")
            logger.error("                     Admin will be notified")
            logger.error("                     Admin should investigate:")
            logger.error("                       - Check if files exist at origin_path")
            logger.error("                       - Check if filesystem is accessible")
            logger.error("                       - Check if BLAKE3 library is installed")
            logger.error("                       - Consider disabling checksum verification")
            logger.error("="*80)
            
            # Final failure - mark as VERIFICATION_FAILED
            api.update_dataset_upload_log(
                dataset_id=dataset_id,
                log_data={
                    'status': UPLOAD_STATUS['VERIFICATION_FAILED'],
                    'metadata': {
                        'failure_reason': f'{type(e).__name__}: {str(e)}',
                        'task_id': task_id,
                        'worker_process_id': worker_process_id,
                        'failed_at': datetime.utcnow().isoformat(),
                        'retries_exhausted': True,
                    }
                }
            )
            
            # Log failure to worker_process
            if worker_process_id:
                try:
                    api.post_worker_logs(worker_process_id, [{
                        'message': f'✗ Verification failed: {type(e).__name__}: {str(e)}',
                        'level': 'error',
                        'timestamp': datetime.utcnow().isoformat(),
                    }])
                except Exception as proc_error:
                    logger.warning(f"Failed to post failure log: {proc_error}")
            
            # Send admin notification
            try:
                api.create_notification({
                    'title': f'Upload Verification Failed: {dataset.get("name", dataset_id)}',
                    'message': (
                        f'Upload verification permanently failed after {retry_count + 1} attempts.\n\n'
                        f'Dataset ID: {dataset_id}\n'
                        f'Dataset Name: {dataset.get("name")}\n'
                        f'Task ID: {task_id}\n'
                        f'Worker Process ID: {worker_process_id}\n'
                        f'Error: {str(e)}\n\n'
                        f'Manual intervention required.\n'
                        f'View logs at /uploads/{dataset_id}'
                    ),
                    'type': 'error',
                    'metadata': {
                        'dataset_id': dataset_id,
                        'task_id': task_id,
                        'worker_process_id': worker_process_id,
                        'error': str(e),
                        'timestamp': datetime.utcnow().isoformat(),
                    },
                })
                logger.info("✓ Admin notification sent")
            except Exception as notify_error:
                logger.error(f"✗ Failed to send admin notification: {notify_error}")
        else:
            logger.error(f"Expected resolution: Celery will retry in 60 seconds")
            logger.error(f"                     Retry attempt {retry_count + 2}/{celery_task.max_retries + 1} will start soon")
            logger.error("="*80)
        
        raise  # Re-raise for Celery to handle retry
