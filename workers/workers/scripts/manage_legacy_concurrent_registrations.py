"""
Manage legacy concurrent-registration workflows.

Purpose:
- Inspect legacy-origin datasets (CMG/Xenium) that have integrated ingestion workflows.
- Resume only workflows where the archive step is explicitly FAILED.
"""

import logging
from datetime import datetime
from typing import Any

import fire

import workers.api as api

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

TARGET_WORKFLOW_NAMES = ('integrated', 'intake_integrated')
LEGACY_ORIGINS = {'legacy', 'legacy_xenium'}
RESUMABLE_WORKFLOW_STATUSES = {'FAILED', 'PAUSED'}


def _parse_created_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith('Z'):
            text = text.replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return datetime.min
    return datetime.min


def _fetch_workflows_for_name(
    workflow_name: str,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    skip = 0
    while True:
        response = api.list_workflows(
            workflow_name=workflow_name,
            limit=page_size,
            skip=skip,
            last_task_runs=True,
            prev_task_runs=True,
        )
        page = response.get('results', [])
        if not page:
            break
        results.extend(page)
        if len(page) < page_size:
            break
        skip += page_size
    return results


def _latest_workflow_per_dataset_name(
    workflows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    latest: dict[tuple[int, str], dict[str, Any]] = {}
    for wf in workflows:
        dataset_id = wf.get('dataset_id')
        wf_name = wf.get('name')
        if not dataset_id or not wf_name:
            continue
        key = (dataset_id, wf_name)
        current = latest.get(key)
        if current is None:
            latest[key] = wf
            continue

        if _parse_created_at(wf.get('created_at')) > _parse_created_at(current.get('created_at')):
            latest[key] = wf
    return list(latest.values())


def _archive_step_failed(workflow: dict[str, Any]) -> bool:
    for step in workflow.get('steps', []) or []:
        if step.get('name') != 'archive':
            continue
        status = str(step.get('status', '')).upper()
        return status == 'FAILED'
    return False


def manage_legacy_concurrent_registrations(
    dry_run: bool = False,
    page_size: int = 200,
) -> dict[str, int]:
    """
    Resume legacy workflows only when archive step failed.

    Args:
        dry_run: If True, log intended resumes without executing them.
        page_size: Workflow page size for API pagination.
    """
    logger.info('=' * 80)
    logger.info('Managing legacy concurrent-registration workflows')
    logger.info('dry_run=%s page_size=%s', dry_run, page_size)
    logger.info('=' * 80)

    summary = {
        'workflows_scanned': 0,
        'legacy_workflows_scoped': 0,
        'archive_failed': 0,
        'resumed': 0,
        'resume_errors': 0,
        'skipped_non_resumable_status': 0,
        'datasets_lookup_errors': 0,
    }

    workflows: list[dict[str, Any]] = []
    for name in TARGET_WORKFLOW_NAMES:
        wf_rows = _fetch_workflows_for_name(workflow_name=name, page_size=page_size)
        logger.info('Fetched %s workflows for name=%s', len(wf_rows), name)
        workflows.extend(wf_rows)

    scoped_workflows = _latest_workflow_per_dataset_name(workflows)
    summary['workflows_scanned'] = len(scoped_workflows)
    logger.info('Scoped to latest workflow per dataset+name: %s', len(scoped_workflows))

    dataset_origin_by_id: dict[int, str | None] = {}
    for wf in scoped_workflows:
        dataset_id = wf.get('dataset_id')
        if not dataset_id or dataset_id in dataset_origin_by_id:
            continue
        try:
            dataset = api.get_dataset(dataset_id=dataset_id)
            dataset_origin_by_id[dataset_id] = (dataset.get('metadata') or {}).get('origin')
        except Exception as error:
            summary['datasets_lookup_errors'] += 1
            dataset_origin_by_id[dataset_id] = None
            logger.error('Failed to load dataset id=%s: %s', dataset_id, error)

    for wf in scoped_workflows:
        workflow_id = wf.get('id')
        workflow_name = wf.get('name')
        dataset_id = wf.get('dataset_id')
        workflow_status = str(wf.get('status', '')).upper()
        if not workflow_id or not dataset_id:
            continue

        dataset_origin = dataset_origin_by_id.get(dataset_id)
        if dataset_origin not in LEGACY_ORIGINS:
            continue
        summary['legacy_workflows_scoped'] += 1

        if not _archive_step_failed(wf):
            continue
        summary['archive_failed'] += 1

        if workflow_status not in RESUMABLE_WORKFLOW_STATUSES:
            summary['skipped_non_resumable_status'] += 1
            logger.info(
                'Skip resume: workflow=%s dataset_id=%s name=%s status=%s archive_step=FAILED',
                workflow_id,
                dataset_id,
                workflow_name,
                workflow_status,
            )
            continue

        if dry_run:
            logger.info(
                '[DRY RUN] Would resume workflow=%s dataset_id=%s name=%s status=%s origin=%s',
                workflow_id,
                dataset_id,
                workflow_name,
                workflow_status,
                dataset_origin,
            )
            summary['resumed'] += 1
            continue

        try:
            api.resume_workflow(workflow_id)
            logger.info(
                'Resumed workflow=%s dataset_id=%s name=%s status=%s origin=%s',
                workflow_id,
                dataset_id,
                workflow_name,
                workflow_status,
                dataset_origin,
            )
            summary['resumed'] += 1
        except Exception as error:
            summary['resume_errors'] += 1
            logger.error(
                'Failed to resume workflow=%s dataset_id=%s name=%s status=%s origin=%s error=%s',
                workflow_id,
                dataset_id,
                workflow_name,
                workflow_status,
                dataset_origin,
                error,
            )

    logger.info('=' * 80)
    logger.info('Summary: %s', summary)
    logger.info('=' * 80)
    return summary


if __name__ == '__main__':
    fire.Fire(manage_legacy_concurrent_registrations)
