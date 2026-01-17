# Worker Development Conventions

## Configuration Pattern

Workers use Python config files:

```python
# workers/config/common.py

CONFIG = {
    'genomic_conversion': {
        'default_analysis_type': {
            'enabled': True,
            'value': 'fastq'
        }
    },
    'celery': {
        'broker_url': os.getenv('CELERY_BROKER_URL'),
        'result_backend': os.getenv('CELERY_RESULT_BACKEND'),
    }
}
```

---

## Task Definition Pattern

```python
from celery import shared_task
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3)
def process_conversion(self, conversion_id):
    """
    Process a genomic data conversion.
    
    Args:
        conversion_id: ID of the conversion to process
    
    Returns:
        dict: Conversion result with output dataset IDs
    """
    try:
        # Task logic
        logger.info(f"Processing conversion {conversion_id}")
        # ...
        return {'status': 'success', 'datasets': [1, 2, 3]}
    except Exception as exc:
        logger.error(f"Conversion failed: {exc}")
        raise self.retry(exc=exc, countdown=60)
```

---

## Logging Pattern

```python
import logging

logger = logging.getLogger(__name__)

# Use structured logging
logger.info(f"[CONVERSION] Starting conversion {conversion_id}")
logger.warning(f"[CONVERSION] Missing parameter, using default: {default_value}")
logger.error(f"[CONVERSION] Failed to process file: {error}", exc_info=True)
```

---

**Last Updated:** 2026-01-16

