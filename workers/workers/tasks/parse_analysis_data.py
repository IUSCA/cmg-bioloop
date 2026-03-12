import json
import logging
from pathlib import Path

from workers.legacy_migration.xenium import is_legacy_xenium_dataset

logger = logging.getLogger(__name__)


def _extract_first_balanced_json_object(script_text: str):
    start_idx = script_text.find('{')
    if start_idx < 0:
        return None

    depth = 0
    in_string = False
    escaped = False

    for idx in range(start_idx, len(script_text)):
        ch = script_text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif ch == '\\':
                escaped = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
        elif ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                return script_text[start_idx:idx + 1]

    return None


def _parse_analysis_from_html(html_text: str):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_text, 'html.parser')
    script_nodes = soup.find_all('script')
    preferred_keys = {'metrics_summary', 'region_info', 'run_info', 'analysis'}

    for script in script_nodes:
        script_text = script.string or script.get_text() or ''
        if not script_text.strip():
            continue

        parsed_obj = None

        if script.get('type') == 'application/json':
            try:
                parsed_obj = json.loads(script_text)
            except Exception:
                parsed_obj = None

        if parsed_obj is None:
            candidate = _extract_first_balanced_json_object(script_text)
            if candidate:
                try:
                    parsed_obj = json.loads(candidate)
                except Exception:
                    parsed_obj = None

        if isinstance(parsed_obj, dict):
            if preferred_keys.intersection(parsed_obj.keys()):
                return parsed_obj
            if isinstance(parsed_obj.get('analysis'), dict):
                return parsed_obj['analysis']

    return None


def parse_analysis_data(celery_task, dataset_id: int, **kwargs):
    """
    Parse the Xenium analysis_summary.html from origin_path and update dataset metadata.

    Args:
        dataset_id: The Bioloop dataset ID
    """
    import workers.api as api

    dataset = api.get_dataset(dataset_id=dataset_id)
    dataset_name = dataset.get('name', str(dataset_id))

    if not is_legacy_xenium_dataset(dataset):
        # Non-Xenium datasets: skip gracefully. This task is a no-op for them.
        logger.info(f'{dataset_name} - not a xenium dataset, skipping parse_analysis_data')
        return

    origin_path = dataset.get('origin_path')
    if not origin_path:
        logger.info(f'{dataset_name} - missing origin_path, skipping parse_analysis_data')
        return

    analysis_html = Path(origin_path) / 'analysis_summary.html'
    if not analysis_html.exists():
        logger.info(f'{dataset_name} - analysis_summary.html not found at {analysis_html}, skipping')
        return

    try:
        html_text = analysis_html.read_text(encoding='utf-8', errors='ignore')
    except Exception as error:
        logger.warning(f'{dataset_name} - failed reading {analysis_html}: {error}')
        return

    analysis_data = _parse_analysis_from_html(html_text)
    if not isinstance(analysis_data, dict):
        logger.info(f'{dataset_name} - no parseable analysis JSON found in {analysis_html}, skipping')
        return

    metadata = dict(dataset.get('metadata') or {})
    metadata['analysis'] = analysis_data
    api.update_dataset(dataset_id=dataset_id, update_data={'metadata': metadata})
    logger.info(f'{dataset_name} - parsed analysis_summary.html and stored metadata.analysis')
