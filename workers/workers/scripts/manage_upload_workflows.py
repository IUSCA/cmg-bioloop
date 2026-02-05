"""
Manage Upload Workflows - TUS Upload Retry Job

This script manages TUS upload workflows by:
1. Retrying stalled uploads (UPLOADED but workflow not started)
2. Retrying failed processing workflows (up to 3 times)
3. Marking permanently failed uploads after 3 failures
4. Sending admin notifications for permanent failures

Designed to run every 15 minutes via PM2 or cron.

Usage:
    # Dry run (default)
    python -m workers.scripts.manage_upload_workflows
    
    # Actually retry workflows
    python -m workers.scripts.manage_upload_workflows --dry-run=False
    
    # Custom retry threshold
    python -m workers.scripts.manage_upload_workflows --dry-run=False --max-retries=3
"""

import logging
from datetime import datetime

import fire
from celery import Celery
from sca_rhythm import Workflow

import workers.config.celeryconfig as celeryconfig
import workers.workflow_utils as wf_utils
from workers import api
from workers.constants.upload import MAX_RETRY_COUNT, UPLOAD_STATUS
from workers.constants.workflow import WORKFLOWS
from workers.upload import verify_upload_integrity

# Initialize Celery app for workflow creation
celery_app = Celery("tasks")
celery_app.config_from_object(celeryconfig)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def manage_upload_workflows(dry_run=True, max_retries=MAX_RETRY_COUNT):
    """
    Manage upload workflows by retrying stalled and failed uploads.
    
    Args:
        dry_run (bool): If True, simulates the process without making actual changes
        max_retries (int): Maximum number of retry attempts before permanent failure
    
    Returns:
        dict: Summary of operations performed
    """
    logger.info("="*60)
    logger.info("Starting upload workflow management")
    logger.info(f"Dry run: {dry_run}")
    logger.info(f"Max retries: {max_retries}")
    logger.info("="*60)
    
    summary = {
        'stalled_retried': 0,
        'verification_failed': 0,
        'failed_retried': 0,
        'permanently_failed': 0,
        'errors': 0,
    }
    
    # Process stalled uploads (UPLOADED but no workflow started)
    try:
        stalled_summary = process_stalled_uploads(dry_run)
        summary['stalled_retried'] = stalled_summary['verified']
        summary['verification_failed'] = stalled_summary.get('verification_failed', 0)
        summary['errors'] += stalled_summary['errors']
    except Exception as e:
        logger.error(f"Error processing stalled uploads: {e}", exc_info=True)
        summary['errors'] += 1
    
    # Process failed uploads (PROCESSING_FAILED, retryable)
    try:
        failed_summary = process_failed_uploads(dry_run, max_retries)
        summary['failed_retried'] = failed_summary['retried']
        summary['permanently_failed'] = failed_summary['permanently_failed']
        summary['errors'] += failed_summary['errors']
    except Exception as e:
        logger.error(f"Error processing failed uploads: {e}", exc_info=True)
        summary['errors'] += 1
    
    # Print summary
    logger.info("="*60)
    logger.info("Upload workflow management complete")
    logger.info(f"Stalled uploads retried: {summary['stalled_retried']}")
    logger.info(f"Failed uploads retried: {summary['failed_retried']}")
    logger.info(f"Uploads marked permanently failed: {summary['permanently_failed']}")
    logger.info(f"Errors: {summary['errors']}")
    logger.info("="*60)
    
    return summary


def process_stalled_uploads(dry_run=True):
    """
    Process uploads that are UPLOADED but workflow hasn't started.
    
    These are uploads where files were successfully uploaded via TUS,
    but the integrated workflow was never triggered.
    
    Flow:
    1. Get stalled uploads (UPLOADED status)
    2. Verify integrity (checksum match or file existence)
    3. If verified -> trigger integrated workflow directly, update status to COMPLETE
    4. If verification fails -> mark as VERIFICATION_FAILED
    
    Args:
        dry_run (bool): If True, simulates without making changes
    
    Returns:
        dict: Summary of stalled uploads processed
    """
    logger.info("\n--- Processing Stalled Uploads ---")
    
    summary = {'verified': 0, 'verification_failed': 0, 'errors': 0}
    
    try:
        response = api.get_stalled_uploads()
        stalled_uploads = response.get('uploads', [])
        
        logger.info(f"Found {len(stalled_uploads)} stalled uploads (UPLOADED status)")
        
        for upload in stalled_uploads:
            dataset_id = upload['dataset_id']
            dataset_name = upload['dataset_name']
            uploaded_at = upload['uploaded_at']
            
            logger.info(f"\nStalled upload:")
            logger.info(f"  Dataset ID: {dataset_id}")
            logger.info(f"  Dataset Name: {dataset_name}")
            logger.info(f"  Uploaded At: {uploaded_at}")
            
            try:
                # Get full dataset and upload log for verification
                dataset = api.get_dataset(dataset_id=dataset_id, workflows=True)
                upload_log = api.get_dataset_upload_log(dataset_id)
                
                if dry_run:
                    logger.info(f"  [DRY RUN] Would verify integrity for dataset {dataset_id}")
                    continue
                
                # Step 1: Verify upload integrity (checksum or file existence)
                logger.info(f"  Verifying upload integrity...")
                try:
                    verify_upload_integrity(dataset, upload_log)
                    logger.info(f"  Integrity verified")
                except Exception as verify_error:
                    # Verification failed - mark dataset and don't trigger workflow
                    logger.error(f"  Integrity verification failed: {verify_error}")
                    api.update_dataset_upload_log(
                        dataset_id=dataset_id,
                        log_data={
                            'status': UPLOAD_STATUS['VERIFICATION_FAILED'],
                            'metadata': {
                                'failure_reason': str(verify_error)
                            }
                        }
                    )
                    summary['verification_failed'] += 1
                    continue
                
                # Step 2: Check for existing integrated workflows
                active_integrated_wfs = [wf for wf in dataset.get('workflows', []) 
                                        if wf['name'] == WORKFLOWS['INTEGRATED']]
                if active_integrated_wfs:
                    logger.info(f"  Integrated workflow already exists for dataset {dataset_id}, skipping")
                    summary['verified'] += 1
                    continue
                
                # Step 3: Create and start integrated workflow directly
                logger.info(f"  Starting {WORKFLOWS['INTEGRATED']} workflow...")
                integrated_wf_body = wf_utils.get_wf_body(wf_name=WORKFLOWS['INTEGRATED'])
                int_wf = Workflow(celery_app=celery_app, **integrated_wf_body)
                int_wf_id = int_wf.workflow['_id']
                api.add_workflow_to_dataset(dataset_id=dataset_id, workflow_id=int_wf_id)
                int_wf.start(dataset_id)
                logger.info(f"  Workflow started: {int_wf_id}")
                
                # Step 3: Update upload status to COMPLETE
                logger.info(f"  Updating upload status to COMPLETE...")
                api.update_dataset_upload_log(
                    dataset_id=dataset_id,
                    log_data={'status': UPLOAD_STATUS['COMPLETE']}
                )
                
                summary['verified'] += 1
                
            except Exception as e:
                logger.error(f"  Failed to process dataset {dataset_id}: {e}")
                summary['errors'] += 1
    
    except Exception as e:
        logger.error(f"Failed to fetch stalled uploads: {e}", exc_info=True)
        summary['errors'] += 1
    
    logger.info(f"\nStalled uploads processed: {summary['verified']} verified & triggered, "
                f"{summary['verification_failed']} verification failed, {summary['errors']} errors")
    return summary


def process_failed_uploads(dry_run=True, max_retries=MAX_RETRY_COUNT):
    """
    Process uploads that are PROCESSING_FAILED and eligible for retry.
    
    Retries uploads that haven't exceeded max retry count.
    Marks uploads as PERMANENTLY_FAILED after max retries and sends admin notification.
    
    Args:
        dry_run (bool): If True, simulates without making changes
        max_retries (int): Maximum retry attempts before permanent failure
    
    Returns:
        dict: Summary of failed uploads processed
    """
    logger.info("\n--- Processing Failed Uploads ---")
    
    summary = {'retried': 0, 'permanently_failed': 0, 'errors': 0}
    
    try:
        response = api.get_failed_uploads(max_retry_count=max_retries - 1)
        failed_uploads = response.get('uploads', [])
        
        logger.info(f"Found {len(failed_uploads)} failed uploads eligible for processing")
        
        for upload in failed_uploads:
            dataset_id = upload['dataset_id']
            dataset_name = upload['dataset_name']
            retry_count = upload.get('retry_count', 0)
            last_error = upload.get('last_error', 'Unknown error')
            
            logger.info(f"\nFailed upload:")
            logger.info(f"  Dataset ID: {dataset_id}")
            logger.info(f"  Dataset Name: {dataset_name}")
            logger.info(f"  Retry Count: {retry_count}/{max_retries}")
            logger.info(f"  Last Error: {last_error}")
            
            # Check if we should retry or mark as permanently failed
            if retry_count < max_retries:
                # Retry the workflow
                new_retry_count = retry_count + 1
                logger.info(f"  Retry attempt {new_retry_count}/{max_retries}")
                
                try:
                    if dry_run:
                        logger.info(f"  [DRY RUN] Would retry workflow for dataset {dataset_id}")
                        logger.info(f"  [DRY RUN] Would update retry_count to {new_retry_count}")
                    else:
                        # Get dataset for workflow check
                        dataset = api.get_dataset(dataset_id=dataset_id, workflows=True)
                        
                        # Check for existing integrated workflows
                        active_integrated_wfs = [wf for wf in dataset.get('workflows', []) 
                                                if wf['name'] == WORKFLOWS['INTEGRATED']]
                        if active_integrated_wfs:
                            logger.info(f"  Integrated workflow already exists, skipping")
                            summary['retried'] += 1
                            continue
                        
                        # Update status
                        logger.info(f"  Updating status...")
                        api.update_dataset_upload(
                            uploaded_dataset_id=dataset_id,
                            log_data={
                                'status': UPLOAD_STATUS['PROCESSING'],
                            }
                        )
                        
                        # Trigger integrated workflow directly
                        logger.info(f"  Starting {WORKFLOWS['INTEGRATED']} workflow...")
                        integrated_wf_body = wf_utils.get_wf_body(wf_name=WORKFLOWS['INTEGRATED'])
                        int_wf = Workflow(celery_app=celery_app, **integrated_wf_body)
                        int_wf_id = int_wf.workflow['_id']
                        api.add_workflow_to_dataset(dataset_id=dataset_id, workflow_id=int_wf_id)
                        int_wf.start(dataset_id)
                        
                        logger.info(f"  Workflow restarted: {int_wf_id}")
                        summary['retried'] += 1
                        
                except Exception as e:
                    logger.error(f"  ✗ Failed to retry workflow for dataset {dataset_id}: {e}")
                    summary['errors'] += 1
            else:
                # Max retries exceeded - mark as permanently failed
                logger.info(f"  Max retries ({max_retries}) exceeded - marking as PERMANENTLY_FAILED")
                
                try:
                    if dry_run:
                        logger.info(f"  [DRY RUN] Would mark dataset {dataset_id} as PERMANENTLY_FAILED")
                        logger.info(f"  [DRY RUN] Would send admin notification")
                    else:
                        # Mark as permanently failed
                        api.update_dataset_upload(
                            uploaded_dataset_id=dataset_id,
                            log_data={
                                'status': UPLOAD_STATUS['PERMANENTLY_FAILED'],
                                'metadata': {
                                    'failure_reason': f"Failed after {max_retries} retry attempts. Last error: {last_error}",
                                },
                            }
                        )
                        
                        # Send admin notification
                        send_permanent_failure_notification(
                            dataset_id=dataset_id,
                            dataset_name=dataset_name,
                            retry_count=retry_count,
                            last_error=last_error,
                        )
                        
                        logger.info(f"  ✓ Marked as permanently failed and notified admins")
                        summary['permanently_failed'] += 1
                        
                except Exception as e:
                    logger.error(f"  ✗ Failed to mark dataset {dataset_id} as permanently failed: {e}")
                    summary['errors'] += 1
    
    except Exception as e:
        logger.error(f"Failed to fetch failed uploads: {e}", exc_info=True)
        summary['errors'] += 1
    
    logger.info(f"\nFailed uploads processed: {summary['retried']} retried, "
                f"{summary['permanently_failed']} permanently failed, {summary['errors']} errors")
    return summary


def send_permanent_failure_notification(dataset_id, dataset_name, retry_count, last_error):
    """
    Send admin notification for permanently failed upload.
    
    Args:
        dataset_id (int): Dataset ID
        dataset_name (str): Dataset name
        retry_count (int): Number of retry attempts made
        last_error (str): Last error message
    """
    try:
        notification_payload = {
            'title': f'Upload Permanently Failed: {dataset_name}',
            'message': (
                f'Dataset upload has permanently failed after {retry_count} retry attempts.\n\n'
                f'Dataset ID: {dataset_id}\n'
                f'Dataset Name: {dataset_name}\n'
                f'Retry Count: {retry_count}\n'
                f'Last Error: {last_error}\n\n'
                f'Manual intervention required.'
            ),
            'type': 'error',
            'metadata': {
                'dataset_id': dataset_id,
                'dataset_name': dataset_name,
                'retry_count': retry_count,
                'error': last_error,
                'timestamp': datetime.utcnow().isoformat(),
            },
        }
        
        api.create_notification(notification_payload)
        logger.info(f"  ✓ Admin notification sent for dataset {dataset_id}")
        
    except Exception as e:
        logger.error(f"  ✗ Failed to send notification for dataset {dataset_id}: {e}")


if __name__ == "__main__":
    fire.Fire(manage_upload_workflows)
