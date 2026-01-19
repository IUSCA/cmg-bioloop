# Bioloop Workflow Architecture: Complete Guide
**Date:** 2026-01-03  
**Purpose:** Document how Bioloop uses the sca_rhythm workflow system for dataset operations

---

## Table of Contents

1. [Overview](#overview)
2. [Rhythm Components](#rhythm-components)
3. [Bioloop Workflow Integration](#bioloop-workflow-integration)
4. [Workflow Types](#workflow-types)
5. [Workflow Lifecycle](#workflow-lifecycle)
6. [Data Storage](#data-storage)
7. [Workflow Creation Process](#workflow-creation-process)
8. [Task Execution](#task-execution)
9. [Progress Tracking](#progress-tracking)
10. [Relevance to CMG Sync](#relevance-to-cmg-sync)

---

## Overview

Bioloop uses the **sca_rhythm** workflow management system to orchestrate long-running dataset operations like:
- **Registration** - Initial dataset ingestion
- **Inspection** - File scanning and metadata extraction
- **Archival** - Moving datasets to tape storage
- **Staging** - Extracting archived datasets to fast storage
- **Validation** - Verifying staged dataset integrity
- **Conversion** - Running genomic pipelines (bcl2fastq, cellranger, etc.)

### Key Characteristics

- **Asynchronous**: Operations run in background Celery workers
- **Multi-Step**: Each workflow consists of ordered steps/tasks
- **Traceable**: Full history of task runs with timestamps and status
- **Resumable**: Workflows can be paused/resumed
- **Progress-Aware**: Real-time progress updates for long-running operations

---

## Rhythm Components

### 1. sca_rhythm Python Package

**Source:** https://pypi.org/project/sca_rhythm/  
**Version in Bioloop:** `^0.6.15` (from `workers/pyproject.toml`)

**Key Classes:**

```python
from sca_rhythm import Workflow, WorkflowTask
from sca_rhythm.progress import Progress
```

- **`Workflow`**: Represents a multi-step workflow
- **`WorkflowTask`**: Base class for Celery tasks that integrate with workflows
- **`Progress`**: Enables real-time progress reporting

### 2. Rhythm API (workflow_server)

**Purpose:** REST API for workflow management

**Configuration:**
```json
{
  "workflow_server": {
    "base_url": "http://127.0.0.1:5001",
    "auth_token": ""
  }
}
```

**Endpoints Used by Bioloop:**
- `GET /workflows` - List workflows (with filters for status, app_id)
- `GET /workflows/:id` - Get single workflow details
- `POST /workflows` - Create new workflow
- `POST /workflows/:id/pause` - Pause workflow
- `POST /workflows/:id/resume` - Resume workflow
- `DELETE /workflows/:id` - Delete workflow
- `GET /workflows/counts_by_status` - Get workflow statistics

**Client:** `api/src/services/workflow.js` (Axios-based)

### 3. Rhythm MongoDB

**Purpose:** Persistent storage for workflow metadata and task results

**Collections:**

#### `workflow_meta`
Stores workflow definitions and execution history

```javascript
{
  "_id": ObjectId("..."),                    // Workflow ID
  "_status": "SUCCESS",                      // Current status
  "app_id": "cmg-test.sca.iu.edu",          // Application identifier
  "name": "stage",                           // Workflow type
  "description": "Stage dataset",
  "created_at": ISODate("..."),
  "updated_at": ISODate("..."),
  "steps": [
    {
      "name": "stage",                       // Step name
      "task": "stage_dataset",               // Celery task name
      "queue": "cmg-test.sca.iu.edu.q",     // Celery queue
      "kwargs": null,                        // Additional task arguments
      "task_runs": [
        {
          "date_start": ISODate("..."),
          "task_id": "abc123..."             // Celery task ID
        }
      ]
    },
    {
      "name": "validate",
      "task": "validate_dataset",
      "queue": "cmg-test.sca.iu.edu.q",
      "kwargs": null,
      "task_runs": [
        {
          "date_start": ISODate("..."),
          "task_id": "def456..."
        }
      ]
    }
  ]
}
```

**Key Fields:**
- `_status`: Overall workflow status (PENDING, IN_PROGRESS, SUCCESS, FAILURE, PAUSED)
- `steps`: Array of workflow steps with their task run history
- `task_runs`: Each step can have multiple runs (retries)

#### `celery_taskmeta`
Stores individual Celery task results (Celery's default result backend)

```javascript
{
  "_id": "abc123...",                        // Task ID
  "status": "SUCCESS",                       // Task status
  "result": "[42]",                          // Return value (JSON string)
  "traceback": null,                         // Error traceback if failed
  "children": [],                            // Child task IDs
  "date_done": ISODate("..."),              // Completion timestamp
  "name": "stage_dataset",                   // Task name
  "args": [42],                              // Task arguments (dataset_id)
  "kwargs": {
    "workflow_id": "...",                    // Parent workflow ID
    "step": "stage",                         // Step name
    "app_id": "cmg-test.sca.iu.edu"
  },
  "worker": "celery-worker@hostname",        // Worker that executed task
  "retries": 0,                              // Retry count
  "queue": "cmg-test.sca.iu.edu.q",         // Queue name
  "parent_id": null                          // Previous task ID in workflow
}
```

**Key Fields:**
- `parent_id`: Links tasks in a workflow chain
- `result`: Task return value (often dataset_id or list of dataset_ids)
- `kwargs.workflow_id`: Links task to parent workflow

---

## Bioloop Workflow Integration

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     Bioloop UI (Vue.js)                     │
│  - Initiates workflows via API                              │
│  - Displays workflow status & progress                      │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP API calls
┌───────────────────────────▼─────────────────────────────────┐
│                Bioloop API (Express.js + Prisma)            │
│  - Workflow service (wrapper for rhythm API)                │
│  - Dataset service (orchestrates operations)                │
│  - Stores workflow association in PostgreSQL                │
└───────────────────────────┬─────────────────────────────────┘
                            │ HTTP to Rhythm API
┌───────────────────────────▼─────────────────────────────────┐
│                      Rhythm API (Flask)                      │
│  - Manages workflow lifecycle                               │
│  - Stores metadata in MongoDB                               │
│  - Dispatches Celery tasks                                  │
└───────────────────────────┬─────────────────────────────────┘
                            │ Celery task dispatch
┌───────────────────────────▼─────────────────────────────────┐
│                  Bioloop Workers (Python)                    │
│  - Celery workers with WorkflowTask tasks                   │
│  - Execute actual operations (stage, archive, etc.)         │
│  - Report progress back to Rhythm MongoDB                   │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **User Action** → Bioloop UI
2. **API Call** → Bioloop API (e.g., `POST /api/datasets/:id/stage`)
3. **Create Workflow** → Bioloop API calls `wfService.create(wf_body)`
4. **Store Association** → Bioloop API inserts into `workflow` table (PostgreSQL)
5. **Dispatch Tasks** → Rhythm API dispatches first Celery task
6. **Execute Task** → Worker picks up task, executes step logic
7. **Update Progress** → Worker updates `celery_taskmeta` in Rhythm MongoDB
8. **Next Task** → Upon success, Rhythm dispatches next step
9. **Complete** → Workflow status updated to SUCCESS/FAILURE
10. **Poll Status** → UI polls Bioloop API for workflow status updates

---

## Workflow Types

Bioloop defines workflows in config files (`api/config/default.json`, `workers/workers/config/common.py`)

### 1. **integrated** Workflow

**Purpose:** End-to-end dataset processing from raw origin to staged

**Steps:**
1. `await_stability` - Wait for dataset to stabilize (no file changes)
2. `inspect` - Scan files and extract metadata
3. `archive` - Move to tape storage
4. `stage` - Extract from tape to fast storage
5. `validate` - Verify staged files match archive
6. `setup_download` - Configure secure download access

**Use Case:** Automated pipeline for new raw datasets

### 2. **stage** Workflow

**Purpose:** Extract archived dataset to fast storage for access

**Steps:**
1. `stage` - Extract from archive
2. `validate` - Verify integrity
3. `setup_download` - Enable download access

**Use Case:** User requests access to archived dataset

### 3. **conversion** Workflow

**Purpose:** Run genomic pipeline (bcl2fastq, cellranger, etc.)

**Steps:**
1. `convert` - Execute conversion pipeline

**Use Case:** User initiates genomic data conversion

### 4. **genomic_conversion** Workflow

**Purpose:** Advanced genomic conversion with options

**Steps:**
1. `convert_genomic` - Execute genomic conversion with configuration

**Use Case:** Complex genomic conversions with custom parameters

### 5. **delete** Workflow

**Purpose:** Delete dataset and its archive

**Steps:**
1. `delete` - Remove files and mark dataset as deleted

**Use Case:** User deletes a dataset

### 6. **process_dataset_upload** Workflow

**Purpose:** Handle user-uploaded datasets

**Steps:**
1. `process_dataset_upload` - Validate and ingest uploaded data

**Use Case:** User uploads dataset via web interface

### 7. **cancel_dataset_upload** Workflow

**Purpose:** Cancel an in-progress upload

**Steps:**
1. `cancel_dataset_upload` - Clean up partial upload

**Use Case:** User cancels upload or upload fails

---

## Workflow Lifecycle

### Status Progression

```
PENDING → IN_PROGRESS → SUCCESS
                      ↓
                   FAILURE
                      ↓
                   PAUSED (manual intervention)
                      ↓
                   IN_PROGRESS (resumed)
```

### Status Definitions

- **PENDING**: Workflow created but not started
- **IN_PROGRESS**: At least one task is executing
- **SUCCESS**: All tasks completed successfully
- **FAILURE**: A task failed (workflow halted)
- **PAUSED**: Workflow manually paused by user/admin
- **CANCELLED**: Workflow was cancelled before completion

---

## Data Storage

### Dual Storage Model

Bioloop stores workflow data in **two separate databases**:

#### PostgreSQL (Bioloop DB)

**Table: `workflow`**

```sql
CREATE TABLE workflow (
  id VARCHAR(36) PRIMARY KEY,     -- Workflow UUID (from Rhythm)
  dataset_id INT NOT NULL,        -- Associated dataset
  initiator_id INT,               -- User who initiated
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  FOREIGN KEY (dataset_id) REFERENCES dataset(id),
  FOREIGN KEY (initiator_id) REFERENCES user(id)
);
```

**Purpose:** Links workflows to datasets in relational context

**Key Points:**
- `id` is the Rhythm workflow UUID (not auto-increment)
- One dataset can have multiple workflows
- `initiator_id` tracks who started the workflow

#### Rhythm MongoDB

**Collections:** `workflow_meta`, `celery_taskmeta` (see earlier section)

**Purpose:** Stores detailed workflow execution history

**Key Points:**
- Managed by Rhythm API, not Bioloop directly
- Contains step-by-step task results
- Supports progress tracking and retries

### Why Dual Storage?

1. **PostgreSQL**: For relational queries (e.g., "all workflows for this dataset")
2. **MongoDB**: For flexible workflow metadata and task history
3. **Separation of Concerns**: Bioloop manages business logic; Rhythm manages execution

---

## Workflow Creation Process

### From API (Express.js)

**File:** `api/src/services/dataset.js`

```javascript
async function create_workflow(dataset, wf_name, initiator_id) {
  // 1. Get workflow definition from config
  const wf_body = get_wf_body(wf_name);
  
  // 2. Check if another workflow with same name is already running
  const active_wfs_with_same_name = dataset.workflows
    .filter(_wf => _wf.name === wf_body.name)
    .filter(_wf => !DONE_STATUSES.includes(_wf.status));
  
  assert(active_wfs_with_same_name.length === 0, 
    'A workflow with the same name is either pending / running');
  
  // 3. Call Rhythm API to create workflow
  const wf = (await wfService.create({
    ...wf_body,
    args: [dataset.id],  // Pass dataset_id as first argument
  })).data;
  
  // 4. Store association in PostgreSQL
  await prisma.workflow.create({
    data: {
      id: wf.workflow_id,           // UUID from Rhythm
      dataset_id: dataset.id,
      ...(initiator_id && { initiator_id }),
    },
  });
  
  return wf;
}
```

**Workflow Body Structure:**

```javascript
function get_wf_body(wf_name) {
  const wf_body = { ...config.workflow_registry[wf_name] };
  wf_body.name = wf_name;
  wf_body.app_id = config.app_id;  // e.g., "cmg-test.sca.iu.edu"
  wf_body.steps = wf_body.steps.map(step => ({
    ...step,
    queue: step.queue || `${config.app_id}.q`,
  }));
  return wf_body;
}
```

**Example `wf_body` for "stage" workflow:**

```javascript
{
  "name": "stage",
  "app_id": "cmg-test.sca.iu.edu",
  "args": [42],  // dataset_id
  "steps": [
    {
      "name": "stage",
      "task": "stage_dataset",
      "queue": "cmg-test.sca.iu.edu.q"
    },
    {
      "name": "validate",
      "task": "validate_dataset",
      "queue": "cmg-test.sca.iu.edu.q"
    },
    {
      "name": "setup_download",
      "task": "setup_dataset_download",
      "queue": "cmg-test.sca.iu.edu.q"
    }
  ]
}
```

### From Workers (Python)

**File:** `workers/workers/scripts/register_ondemand.py`

```python
from sca_rhythm import Workflow
import workers.workflow_utils as wf_utils
from workers.celery_app import app as celery_app

# 1. Get workflow definition
wf_body = wf_utils.get_wf_body(wf_name='integrated')

# 2. Create Workflow instance
wf = Workflow(celery_app=celery_app, **wf_body)

# 3. Create dataset via API with workflow_id
dataset_payload = {
    'name': dataset_name,
    'type': dataset_type,
    'workflow_id': wf.workflow['_id'],  # Link workflow to dataset
    'origin_path': dataset_path,
}
created_dataset = api.create_dataset(dataset_payload)

# 4. Start workflow
wf.start(created_dataset['id'])
```

**Key Difference:**
- **API approach**: Bioloop API controls workflow creation
- **Worker approach**: Workers create workflows directly (e.g., for automated registration)

---

## Task Execution

### Task Declaration

**File:** `workers/workers/tasks/declarations.py`

```python
from celery import Celery
from sca_rhythm import WorkflowTask
from workers import exceptions as exc

app = Celery("tasks")
app.config_from_object(celeryconfig)

@app.task(base=WorkflowTask, bind=True, name='stage_dataset')
def stage_dataset(celery_task, dataset_id, **kwargs):
    from workers.tasks.stage import stage as task_body
    try:
        return task_body(celery_task, dataset_id, **kwargs)
    except exc.RetryableException:
        # Celery will retry task
        raise
    except Exception as e:
        # Permanent failure
        raise exc.StagingException(e)
```

**Key Points:**
- `base=WorkflowTask`: Integrates with Rhythm progress tracking
- `bind=True`: Passes `celery_task` (self) as first argument
- `name='stage_dataset'`: Task name must match workflow definition
- `**kwargs`: Receives `workflow_id`, `step`, `app_id` from Rhythm

### Task Implementation

**File:** `workers/workers/tasks/stage.py`

```python
from sca_rhythm import WorkflowTask
from workers import api, utils, workflow_utils as wf_utils

def stage(celery_task: WorkflowTask, dataset: dict) -> (str, str):
    """
    Stage a dataset by extracting its archive to fast storage.
    
    Args:
        celery_task: WorkflowTask instance for progress reporting
        dataset: Dataset object from Bioloop API
    
    Returns:
        (stage_path, alias): Paths to staged dataset
    """
    # 1. Calculate staging path
    staging_dir, alias = compute_staging_path(dataset)
    
    # 2. Copy archive bundle to staging
    bundle_download_path = Path(get_bundle_staged_path(dataset))
    shutil.copy2(dataset['archive_path'], bundle_download_path)
    
    # 3. Verify checksum
    evaluated_checksum = utils.checksum(bundle_download_path)
    if evaluated_checksum != dataset['bundle']['md5']:
        raise exc.ValidationFailed('Checksum mismatch')
    
    # 4. Extract archive (with progress tracking)
    extract_tarfile(tar_path=bundle_download_path, target_dir=staging_dir)
    
    # 5. Update dataset via API
    api.update_dataset(dataset['id'], {
        'is_staged': True,
        'staged_path': str(staging_dir)
    })
    
    return str(staging_dir), alias
```

**Key Operations:**
1. Compute paths
2. Copy/download files
3. Verify integrity
4. Extract archives
5. Update dataset metadata via API

### Task Return Values

**Important:** Rhythm passes task return values to subsequent tasks

```python
# Task 1: stage_dataset
return [dataset_id]

# Task 2: validate_dataset (receives dataset_id from task 1)
def validate_dataset(celery_task, dataset_id, **kwargs):
    dataset = api.get_dataset(dataset_id)
    # ... validation logic ...
    return [dataset_id, validation_errors]

# Task 3: setup_dataset_download (receives dataset_id and errors)
def setup_dataset_download(celery_task, dataset_id, validation_errors, **kwargs):
    # ... setup logic ...
    return [dataset_id]
```

---

## Progress Tracking

### Real-Time Progress Updates

**File:** `workers/workers/workflow_utils.py`

```python
from sca_rhythm import WorkflowTask
from sca_rhythm.progress import Progress

@contextmanager
def track_progress_parallel(celery_task: WorkflowTask,
                            name: str,
                            progress_fn,
                            total: int = None,
                            units: str = None,
                            loop_delay=5):
    """
    Track progress in a background process while main task executes.
    
    Args:
        celery_task: WorkflowTask instance
        name: Progress identifier (e.g., "sda put")
        progress_fn: Function that returns current progress value
        total: Total expected value (e.g., file size in bytes)
        units: Unit of measurement (e.g., "bytes")
        loop_delay: Seconds between progress checks
    """
    def progress_loop():
        prog = Progress(celery_task=celery_task, name=name, 
                       total=total, units=units)
        while True:
            time.sleep(loop_delay)
            try:
                done = progress_fn()
                prog.update(done)
            except Exception as e:
                logger.warning(f'exception in progress loop: {e}')
    
    # Start subprocess to track progress
    p = multiprocessing.Process(target=progress_loop)
    p.start()
    
    try:
        yield p  # Execute main task
    finally:
        # Terminate progress tracker
        p.terminate()
```

**Usage Example:**

```python
def upload_file_to_sda(local_file_path, sda_file_path, 
                       celery_task=None, **kwargs):
    if celery_task is not None:
        local_file_size = local_file_path.stat().st_size
        cm = track_progress_parallel(
            celery_task=celery_task,
            name='sda put',
            progress_fn=lambda: sda.get_size(sda_file_path),  # Current size
            total=local_file_size,
            units='bytes'
        )
    else:
        cm = utils.empty_context_manager()
    
    with cm:
        # Actual upload happens here
        sda.put(local_file=str(local_file_path), 
               sda_file=sda_file_path,
               verify_checksum=True)
```

**Key Points:**
- Progress tracking runs in **separate process** (billiard/multiprocessing)
- Doesn't block main task execution
- Updates Rhythm MongoDB in real-time
- UI can poll for progress updates

---

## Relevance to CMG Sync

### Why This Matters for Database Synchronization

#### 1. **Workflow-Dataset Associations**

**Challenge:** CMG dataproducts have `events` arrays indicating staging/conversion completion

**Bioloop Equivalent:** Workflows in `workflow` table + Rhythm MongoDB

**Sync Implication:**
- **Don't migrate** CMG events as Bioloop workflows initially
- Bioloop workflows are **future-only** (created for new operations)
- CMG historical events → Bioloop `dataset_audit` table (append-only)

**Rationale:**
- Recreating historical workflows is time-consuming (commented out in current migration)
- Not necessary for Bioloop functionality
- Users care about *current state*, not *historical workflow details*

---

#### 2. **Event Detection**

**CMG Model:**
```javascript
{
  events: [
    { stamp: ISODate("..."), description: "stage - start" },
    { stamp: ISODate("..."), description: "stage - finish" },
    { stamp: ISODate("..."), description: "validate - finish" }
  ]
}
```

**When Bioloop Needs to Know:**
- Dataset staged → Update `dataset.is_staged = true`
- Conversion completed → Create new `dataset` (DATA_PRODUCT)

**Sync Strategy:**
1. **Poll CMG MongoDB** for datasets with new events
2. **Compare event timestamps** with last sync
3. **Update Bioloop dataset fields** (is_staged, staged_path, etc.)
4. **Create audit log entries** for new events
5. **Don't create Bioloop workflows** for past events

**Why Not Create Workflows?**
- Workflows are for **orchestrating future operations**, not recording history
- Creating workflows requires complex Rhythm MongoDB setup
- Audit logs provide sufficient historical record

---

#### 3. **Conversion Tracking**

**CMG Conversion Completion:**
- When CMG finishes a conversion, it creates a new `dataproduct`
- The `dataproduct.conversion` field references the parent `conversion`
- This dataproduct gets added to the original project

**Bioloop Sync Process:**
1. Detect new CMG `dataproduct` (via sync process)
2. Create Bioloop `dataset` with type='DATA_PRODUCT'
3. Create `conversion_derived_dataset` association
4. Create `project_dataset` association
5. **Don't create conversion workflow** (conversion already happened in CMG)

**Key Insight:**
- Bioloop workflows track **live operations**
- CMG sync is about **replicating state**, not replaying operations

---

#### 4. **Workflow Table Population**

**Current Migration Script:** `create_workflows_for_past_stagings()` is **commented out**

**Reason:** Not necessary for Bioloop functionality

**For Incremental Sync:**
- **Skip workflow table** entirely for now
- Only populate it if/when Bioloop initiates workflows for CMG datasets
- Example: User re-stages an already-archived CMG dataset in Bioloop UI

**Future Consideration:**
- If users want to see "last staging time" in Bioloop UI
- Could populate workflows **on-demand** when dataset is first accessed
- Or rely on `dataset_audit` table for historical context

---

#### 5. **Rhythm MongoDB Dependencies**

**Synchronization Note:** 
- Bioloop PostgreSQL ← sync from → CMG MongoDB
- Rhythm MongoDB is **separate** and **not synced**

**Rhythm MongoDB Purpose:**
- Track **Bioloop-initiated** operations only
- **Not relevant** to CMG sync process

**Separation:**
```
CMG MongoDB        → Sync →    Bioloop PostgreSQL (datasets, projects, users)
                              
Rhythm MongoDB     ←       →   Bioloop Workflows (future operations only)
```

---

### Summary: Workflow Considerations for Sync

| Aspect | CMG | Bioloop | Sync Strategy |
|--------|-----|---------|---------------|
| **Historical Events** | `events` array in documents | `dataset_audit` table | Sync to audit logs |
| **Workflow Tracking** | N/A (no workflow system) | `workflow` table + Rhythm MongoDB | Don't populate for past operations |
| **Operation State** | Embedded flags (`staged`, `archived`, etc.) | Dataset fields + audit logs | Sync flags, create audit entries |
| **Conversion Results** | New `dataproduct` with `conversion` reference | New `dataset` + `conversion_derived_dataset` | Create dataset and link |
| **Future Operations** | CMG workflow system (Python polling) | Bioloop workflow system (Rhythm + Celery) | Independent systems |

**Key Takeaway:** Bioloop workflows are for **future operations initiated by Bioloop**, not for recording CMG's historical operations. Focus sync effort on **dataset state** and **audit logs**, not workflows.

---

## Workflow System Resources

### Official Documentation

- **sca_rhythm PyPI**: https://pypi.org/project/sca_rhythm/
- **rhythm_api GitHub**: https://github.com/IUSCA/rhythm_api

### Bioloop Implementation Files

**API (Express.js):**
- `api/src/services/workflow.js` - Workflow API client
- `api/src/services/dataset.js` - Workflow creation logic
- `api/config/default.json` - Workflow definitions

**Workers (Python):**
- `workers/workers/workflow_utils.py` - Workflow utilities
- `workers/workers/tasks/declarations.py` - Task declarations
- `workers/workers/tasks/*.py` - Task implementations
- `workers/workers/config/common.py` - Workflow definitions

---

## Conclusion

The Bioloop workflow architecture provides a robust, scalable system for orchestrating long-running dataset operations. Understanding this system is crucial for:

1. **Distinguishing** between historical CMG events and future Bioloop operations
2. **Deciding** what to sync (dataset state) vs. what not to sync (workflows)
3. **Designing** the incremental sync process to focus on state replication
4. **Avoiding** unnecessary complexity of recreating historical workflows

**For the CMG sync project:** Focus on syncing **dataset state** and **audit logs**, not workflows. Bioloop will create its own workflows for future operations as needed.

---

**Previous:** See `CURRENT_MIGRATION_PROCESS_ANALYSIS.md` for understanding the existing Python migration process  
**Next:** Use these insights to design the JavaScript-based incremental sync process

