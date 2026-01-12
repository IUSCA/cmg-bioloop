# Genomic Data Conversion Pipelines Architecture
**Documentation Date:** 2026-01-11  
**For:** AI Agents & Developers

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Database Schema](#database-schema)
3. [Core Entities & Relationships](#core-entities--relationships)
4. [Conversion Definitions](#conversion-definitions)
5. [Command-Line Programs & Arguments](#command-line-programs--arguments)
6. [Conversion Execution](#conversion-execution)
7. [API Endpoints](#api-endpoints)
8. [Worker Implementation](#worker-implementation)
9. [UI Components](#ui-components)
10. [CMG Migration Strategy](#cmg-migration-strategy)
11. [Key Code Locations](#key-code-locations)

---

## System Overview

Bioloop's conversion system provides a **flexible, extensible framework** for executing bioinformatics pipelines that transform genomic datasets from one format to another.

### Key Capabilities
1. **Generic Conversion Registry**: Define new conversion types without code changes
2. **Pipeline Execution**: Execute command-line tools with validated parameters
3. **Dataset Transformation**: Convert RAW_DATA → DATA_PRODUCT (e.g., FASTQ → BAM → VCF)
4. **Workflow Integration**: Track conversion history and derived datasets
5. **CMG Compatibility**: Migrate existing CMG conversion pipelines

### Architecture Components
- **API**: Express.js backend manages conversion definitions and instances
- **Database**: PostgreSQL with Prisma ORM stores conversion metadata
- **Workers**: Python Celery workers execute actual conversions
- **UI**: Vue 3 frontend for pipeline configuration and monitoring

---

## Database Schema

### Core Models (Prisma)

#### `conversion_definition`
Registry of available conversion pipelines.

```prisma
model conversion_definition {
  id               Int              @id @default(autoincrement())
  name             String           @unique      // e.g., "FASTQ_to_BAM", "BAM_to_VCF"
  description      String?
  enabled          Boolean          @default(false)
  author_id        Int
  dataset_types    String[]         // Allowed input dataset types
  tags             String[]         // For searching/filtering
  program_id       Int              // Command-line program to execute
  output_directory String?          // Where outputs are stored
  logs_directory   String?          // Where logs are stored
  capture_logs     Boolean          @default(false)
  created_at       DateTime         @default(now())
  updated_at       DateTime         @updatedAt
  
  program          cmd_line_program @relation(fields: [program_id], references: [id])
  author           user             @relation(fields: [author_id], references: [id])
  conversions      conversion[]     // Instances of this conversion
}
```

**Key Fields:**
- `dataset_types`: Whitelist of dataset types this conversion accepts (e.g., `["raw_data", "fastq"]`)
- `enabled`: Only enabled definitions can be initiated
- `tags`: Used for UI filtering and search

---

#### `cmd_line_program`
Executable programs used by conversion definitions.

```prisma
model cmd_line_program {
  id                    Int                     @id @default(autoincrement())
  name                  String                  @unique
  executable_path       String                  // e.g., "/usr/bin/bwa"
  executable_directory  String?                 // Working directory
  allow_additional_args Boolean                 @default(false)
  created_at            DateTime                @default(now())
  updated_at            DateTime                @updatedAt
  
  arguments             argument[]
  conversion_definitions conversion_definition[]
}
```

**Characteristics:**
- Represents a single executable (bwa, samtools, gatk, etc.)
- Can be reused across multiple conversion definitions
- Defines allowable arguments

---

#### `argument`
Parameter definition for command-line programs.

```prisma
model argument {
  id                    Int                   @id @default(autoincrement())
  name                  String?               // Argument name (e.g., "threads")
  value_type            argument_value_type   // STRING, NUMBER, BOOLEAN
  allowed_values        String[]              // Enum-like restrictions
  is_required           Boolean               @default(false)
  default_value         String?
  is_flag               Boolean               @default(false)  // True for --flag-style args
  description           String?
  min_value             Float?                // For NUMBER type
  max_value             Float?
  min_length            Int?                  // For STRING type
  max_length            Int?
  position              Int?                  // Positional argument order
  dynamic_variable_name String?               // Reference to runtime variables
  program_id            Int
  
  program               cmd_line_program      @relation(fields: [program_id], references: [id])
  dynamic_variable      dynamic_variable?     @relation(fields: [dynamic_variable_name], references: [name])
  argument_values       argument_value[]
}

enum argument_value_type {
  STRING
  NUMBER
  BOOLEAN
}
```

**Key Features:**
- **Type Validation**: Enforce STRING, NUMBER, or BOOLEAN types
- **Value Constraints**: Min/max for numbers, allowed_values for enums
- **Dynamic Variables**: Reference runtime values (e.g., `{OUTPUT_DIR}`)
- **Positional vs Named**: Support both positional and flag-based arguments

---

#### `dynamic_variable`
Runtime variables that can be referenced in arguments.

```prisma
model dynamic_variable {
  name        String     @id           // e.g., "OUTPUT_DIR", "REFERENCE_GENOME"
  description String?
  created_at  DateTime   @default(now())
  updated_at  DateTime   @updatedAt
  
  arguments   argument[]
}
```

**Example Variables:**
- `OUTPUT_DIR`: Runtime output directory path
- `INPUT_FILE`: Path to input dataset file
- `REFERENCE_GENOME`: Path to reference genome file
- `NUM_THREADS`: Number of threads to use

---

#### `conversion`
Instance of a conversion execution.

```prisma
model conversion {
  id               Int                          @id @default(autoincrement())
  cmg_id           String?                      // Legacy CMG conversion ID
  initiated_at     DateTime                     @default(now())
  definition_id    Int
  workflow_id      String?                      // Celery workflow ID
  dataset_id       Int?                         // Input dataset
  initiator_id     Int?                         // User who initiated
  additional_args  Json?                        // Extra arguments not in definition
  
  definition       conversion_definition        @relation(fields: [definition_id], references: [id])
  dataset          dataset?                     @relation(fields: [dataset_id], references: [id], onDelete: Cascade)
  initiator        user?                        @relation(fields: [initiator_id], references: [id], onDelete: SetNull)
  argument_values  argument_value[]
  derived_datasets conversion_derived_dataset[]
}
```

**Key Points:**
- Represents one execution of a conversion pipeline
- Links input dataset to output datasets
- Tracks workflow execution via `workflow_id` (Celery task ID)

---

#### `argument_value`
Runtime values for conversion arguments.

```prisma
model argument_value {
  id            Int        @id @default(autoincrement())
  argument_id   Int
  conversion_id Int
  value         String?    // Actual value provided at runtime
  
  argument      argument   @relation(fields: [argument_id], references: [id])
  conversion    conversion @relation(fields: [conversion_id], references: [id])
}
```

---

#### `conversion_derived_dataset`
Links conversions to their output datasets.

```prisma
model conversion_derived_dataset {
  conversion_id Int
  dataset_id    Int
  created_at    DateTime   @default(now())
  updated_at    DateTime   @updatedAt
  metadata      Json?      // Additional metadata about derivation
  
  conversion    conversion @relation(fields: [conversion_id], references: [id], onDelete: Cascade)
  dataset       dataset    @relation(fields: [dataset_id], references: [id], onDelete: Cascade)
  
  @@id([conversion_id, dataset_id])
}
```

**Purpose**: Many-to-many relationship allowing one conversion to produce multiple output datasets.

---

## Core Entities & Relationships

### Relationship Diagram (Text)
```
User
  └── conversion (initiator) (1:N)
        ├── conversion_definition (N:1)
        │     ├── cmd_line_program (N:1)
        │     │     └── argument (1:N)
        │     │           ├── dynamic_variable (N:1, optional)
        │     │           └── argument_value (1:N)
        │     └── user (author) (N:1)
        ├── dataset (input) (N:1)
        ├── argument_value (1:N)
        └── conversion_derived_dataset (1:N)
              └── dataset (output) (N:1)
```

### Key Relationships
1. **User → Conversions**: A user initiates multiple conversions
2. **Conversion → Definition**: Each conversion instance references a definition
3. **Definition → Program**: Each definition uses one command-line program
4. **Program → Arguments**: Programs have multiple argument definitions
5. **Conversion → Datasets**: Input dataset (N:1) and output datasets (1:N)

---

## Conversion Definitions

### What is a Conversion Definition?

A **conversion definition** is a **template** for a bioinformatics pipeline. It defines:
- What command-line tool to run
- What arguments it accepts
- What types of datasets it can process
- Where outputs and logs are stored

### Example: FASTQ to BAM Conversion

```javascript
{
  "name": "FASTQ_to_BAM_bwa_samtools",
  "description": "Align FASTQ files to BAM using BWA-MEM and samtools",
  "enabled": true,
  "dataset_types": ["raw_data", "fastq"],
  "tags": ["alignment", "bwa", "samtools"],
  "program_id": 1,  // References BWA-MEM program
  "output_directory": "/opt/sca/data/conversions/bam",
  "logs_directory": "/opt/sca/logs/conversions",
  "capture_logs": true
}
```

### Creating New Definitions

Conversion definitions are typically seeded via `api/prisma/seed_data/conversion_definitions.js`:

```javascript
const conversionDefinitions = [
  {
    name: 'FASTQ_to_BAM_bwa_samtools',
    description: 'Align FASTQ files to reference genome using BWA-MEM',
    enabled: true,
    dataset_types: ['raw_data', 'fastq'],
    tags: ['alignment', 'bwa', 'samtools'],
    program: {
      create: {
        name: 'bwa_mem',
        executable_path: '/usr/bin/bwa',
        executable_directory: '/opt/sca/bin',
        allow_additional_args: true,
        arguments: {
          create: [
            {
              name: 'threads',
              value_type: 'NUMBER',
              is_required: true,
              default_value: '8',
              min_value: 1,
              max_value: 32,
              description: 'Number of threads',
            },
            {
              name: 'reference',
              value_type: 'STRING',
              is_required: true,
              description: 'Path to reference genome',
              dynamic_variable_name: 'REFERENCE_GENOME',
            },
          ],
        },
      },
    },
  },
];
```

---

## Command-Line Programs & Arguments

### Program Structure

Each `cmd_line_program` represents an executable tool with defined parameters.

**Example: BWA-MEM**

```javascript
{
  "name": "bwa_mem",
  "executable_path": "/usr/bin/bwa",
  "executable_directory": "/opt/sca/bin",
  "allow_additional_args": true,
  "arguments": [
    {
      "name": "threads",
      "value_type": "NUMBER",
      "is_required": true,
      "default_value": "8",
      "is_flag": true,
      "description": "Number of threads (-t flag)"
    },
    {
      "name": "reference",
      "value_type": "STRING",
      "is_required": true,
      "position": 1,
      "dynamic_variable_name": "REFERENCE_GENOME"
    },
    {
      "name": "input_r1",
      "value_type": "STRING",
      "is_required": true,
      "position": 2,
      "dynamic_variable_name": "INPUT_FILE_R1"
    },
    {
      "name": "input_r2",
      "value_type": "STRING",
      "is_required": false,
      "position": 3,
      "dynamic_variable_name": "INPUT_FILE_R2"
    }
  ]
}
```

**Command Construction:**
```bash
/usr/bin/bwa mem \
  -t 8 \
  /references/hg38.fa \
  /data/sample_R1.fastq.gz \
  /data/sample_R2.fastq.gz
```

### Argument Types

#### **Positional Arguments**
Arguments with `position` field are passed in order:
```javascript
{
  "name": "input_file",
  "position": 1,
  "value_type": "STRING"
}
```
→ `/usr/bin/tool <value>`

#### **Flag Arguments**
Arguments with `is_flag: true` are passed with flag notation:
```javascript
{
  "name": "threads",
  "is_flag": true,
  "value_type": "NUMBER"
}
```
→ `/usr/bin/tool -t 8` or `/usr/bin/tool --threads 8`

#### **Boolean Flags**
Boolean arguments are included only when true:
```javascript
{
  "name": "verbose",
  "is_flag": true,
  "value_type": "BOOLEAN"
}
```
→ `/usr/bin/tool --verbose` (if true) or `` (if false)

### Dynamic Variables

Dynamic variables are resolved at runtime:

```javascript
{
  "name": "output_dir",
  "value_type": "STRING",
  "dynamic_variable_name": "OUTPUT_DIR"
}
```

**Runtime Resolution:**
- `OUTPUT_DIR` → `/opt/sca/data/conversions/output_123/`
- `REFERENCE_GENOME` → `/references/hg38.fa`
- `INPUT_FILE` → `/opt/sca/data/datasets/42/sample.fastq.gz`

---

## Conversion Execution

### Lifecycle

1. **User Initiates Conversion**
   - Selects input dataset
   - Chooses conversion definition
   - Provides argument values (optional, uses defaults)

2. **API Validates Request**
   - Checks dataset type is allowed
   - Validates all required arguments
   - Checks value constraints (min/max, allowed_values)

3. **API Creates Conversion Record**
   - Creates `conversion` record
   - Creates `argument_value` records for provided values
   - Returns conversion ID

4. **Worker Picks Up Task**
   - Celery worker fetches conversion details
   - Resolves dynamic variables
   - Constructs command line
   - Executes program

5. **Worker Creates Output Datasets**
   - Creates new `dataset` records for outputs
   - Links via `conversion_derived_dataset`
   - Stages output files

6. **Completion**
   - Conversion status updated
   - User notified
   - Output datasets available for downstream use (e.g., tracks, further conversions)

---

## API Endpoints

### Conversion Definitions

#### `GET /api/conversion-definitions`
List all available conversion definitions.

**Query Parameters:**
- `enabled`: Filter by enabled status
- `dataset_type`: Filter by compatible dataset types
- `tags`: Filter by tags

**Response:**
```json
{
  "definitions": [
    {
      "id": 1,
      "name": "FASTQ_to_BAM_bwa_samtools",
      "description": "Align FASTQ files to BAM using BWA-MEM and samtools",
      "enabled": true,
      "dataset_types": ["raw_data", "fastq"],
      "tags": ["alignment", "bwa"],
      "program": {
        "name": "bwa_mem",
        "arguments": [...]
      }
    }
  ]
}
```

#### `GET /api/conversion-definitions/:id`
Get details of a specific conversion definition.

**Response:**
```json
{
  "definition": {
    "id": 1,
    "name": "FASTQ_to_BAM_bwa_samtools",
    "description": "...",
    "program": {
      "name": "bwa_mem",
      "executable_path": "/usr/bin/bwa",
      "arguments": [
        {
          "id": 1,
          "name": "threads",
          "value_type": "NUMBER",
          "is_required": true,
          "default_value": "8",
          "min_value": 1,
          "max_value": 32
        }
      ]
    }
  }
}
```

### Conversions

#### `POST /api/conversions`
Initiate a new conversion.

**Request Body:**
```json
{
  "definition_id": 1,
  "dataset_id": 42,
  "argument_values": [
    {
      "argument_id": 1,
      "value": "16"
    },
    {
      "argument_id": 2,
      "value": "/references/hg38.fa"
    }
  ],
  "additional_args": {
    "--no-lane-splitting": "true"
  }
}
```

**Response:**
```json
{
  "conversion": {
    "id": 123,
    "definition_id": 1,
    "dataset_id": 42,
    "initiated_at": "2026-01-11T10:00:00Z",
    "workflow_id": "celery-task-uuid",
    "status": "queued"
  }
}
```

#### `GET /api/conversions`
List conversions with filtering.

**Query Parameters:**
- `definition_id`: Filter by conversion definition
- `dataset_id`: Filter by input dataset
- `initiator_id`: Filter by user
- `status`: Filter by status (queued, running, completed, failed)

#### `GET /api/conversions/:id`
Get details of a specific conversion.

**Response:**
```json
{
  "conversion": {
    "id": 123,
    "definition": {
      "name": "FASTQ_to_BAM_bwa_samtools"
    },
    "dataset": {
      "name": "Sample_001_FASTQ"
    },
    "initiator": {
      "name": "John Doe"
    },
    "initiated_at": "2026-01-11T10:00:00Z",
    "workflow_id": "celery-task-uuid",
    "argument_values": [...],
    "derived_datasets": [
      {
        "dataset": {
          "id": 101,
          "name": "Sample_001_BAM",
          "type": "data_product"
        }
      }
    ]
  }
}
```

---

## Worker Implementation

### Python Celery Tasks

**File:** `workers/workers/tasks/convert.py`

```python
from celery import shared_task
import subprocess
import logging

logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3)
def execute_conversion(self, conversion_id):
    """
    Execute a genomic data conversion.
    
    Args:
        conversion_id: ID of the conversion to process
    
    Returns:
        dict: Conversion result with output dataset IDs
    """
    try:
        # 1. Fetch conversion details from database
        conversion = get_conversion(conversion_id)
        definition = conversion.definition
        program = definition.program
        
        # 2. Resolve dynamic variables
        variables = resolve_dynamic_variables(conversion)
        
        # 3. Build command line
        cmd = build_command(program, conversion.argument_values, variables)
        
        logger.info(f"[Conversion {conversion_id}] Executing: {' '.join(cmd)}")
        
        # 4. Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=program.executable_directory
        )
        
        if result.returncode != 0:
            logger.error(f"[Conversion {conversion_id}] Failed: {result.stderr}")
            raise Exception(f"Conversion failed: {result.stderr}")
        
        # 5. Create output datasets
        output_datasets = create_output_datasets(conversion, variables['OUTPUT_DIR'])
        
        # 6. Link to conversion
        link_derived_datasets(conversion_id, output_datasets)
        
        logger.info(f"[Conversion {conversion_id}] Completed successfully")
        return {
            'status': 'success',
            'dataset_ids': [ds.id for ds in output_datasets]
        }
        
    except Exception as exc:
        logger.error(f"[Conversion {conversion_id}] Error: {exc}")
        raise self.retry(exc=exc, countdown=60)


def build_command(program, argument_values, variables):
    """
    Construct command line from program definition and runtime values.
    """
    cmd = [program.executable_path]
    
    # Add positional arguments
    positional_args = sorted(
        [(arg.position, resolve_value(arg_val, variables)) 
         for arg, arg_val in argument_values if arg.position is not None],
        key=lambda x: x[0]
    )
    cmd.extend([val for _, val in positional_args])
    
    # Add flag arguments
    for arg, arg_val in argument_values:
        if arg.is_flag and arg.position is None:
            flag_name = f"--{arg.name}" if len(arg.name) > 1 else f"-{arg.name}"
            
            if arg.value_type == 'BOOLEAN':
                if arg_val.value.lower() == 'true':
                    cmd.append(flag_name)
            else:
                cmd.extend([flag_name, resolve_value(arg_val, variables)])
    
    return cmd


def resolve_dynamic_variables(conversion):
    """
    Resolve runtime variables for this conversion.
    """
    dataset = conversion.dataset
    definition = conversion.definition
    
    return {
        'OUTPUT_DIR': f"{definition.output_directory}/{conversion.id}/",
        'INPUT_FILE': f"{dataset.staged_path}/{dataset.files[0].path}",
        'REFERENCE_GENOME': get_reference_genome(dataset.genomic_details),
        'NUM_THREADS': get_available_threads(),
    }
```

### Worker Configuration

**File:** `workers/config/common.py`

```python
CONVERSION_CONFIG = {
    'default_threads': 8,
    'max_threads': 32,
    'timeout': 86400,  # 24 hours
    'retry_attempts': 3,
    'references': {
        'hg38': '/references/hg38.fa',
        'hg19': '/references/hg19.fa',
        'mm10': '/references/mm10.fa',
    },
}
```

---

## UI Components

### Conversion Definition Browser

**Component:** `ui/src/pages/conversions/definitions/index.vue`

Lists available conversion definitions with filtering:
- Filter by dataset type compatibility
- Search by name/tags
- View argument requirements

### Conversion Initiation Form

**Component:** `ui/src/pages/conversions/new.vue`

```vue
<template>
  <div>
    <h2>Initiate Conversion</h2>
    
    <!-- Step 1: Select Dataset -->
    <va-select
      v-model="form.dataset_id"
      :options="compatibleDatasets"
      label="Input Dataset"
    />
    
    <!-- Step 2: Select Conversion Definition -->
    <va-select
      v-model="form.definition_id"
      :options="availableDefinitions"
      label="Conversion Type"
      @update:model-value="loadArguments"
    />
    
    <!-- Step 3: Configure Arguments -->
    <div v-for="arg in arguments" :key="arg.id">
      <va-input
        v-if="arg.value_type === 'STRING'"
        v-model="form.argument_values[arg.id]"
        :label="arg.name"
        :placeholder="arg.default_value"
        :rules="getValidationRules(arg)"
      />
      
      <va-input
        v-if="arg.value_type === 'NUMBER'"
        v-model.number="form.argument_values[arg.id]"
        type="number"
        :label="arg.name"
        :min="arg.min_value"
        :max="arg.max_value"
        :placeholder="arg.default_value"
      />
      
      <va-checkbox
        v-if="arg.value_type === 'BOOLEAN'"
        v-model="form.argument_values[arg.id]"
        :label="arg.name"
      />
    </div>
    
    <!-- Submit -->
    <va-button @click="submitConversion">
      Start Conversion
    </va-button>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import conversionService from '@/services/conversion';

const form = ref({
  dataset_id: null,
  definition_id: null,
  argument_values: {},
});

const arguments = ref([]);

const loadArguments = async () => {
  if (!form.value.definition_id) return;
  
  const response = await conversionService.getDefinition(form.value.definition_id);
  arguments.value = response.data.definition.program.arguments;
};

const submitConversion = async () => {
  const payload = {
    definition_id: form.value.definition_id,
    dataset_id: form.value.dataset_id,
    argument_values: Object.entries(form.value.argument_values).map(([argId, value]) => ({
      argument_id: parseInt(argId),
      value: String(value),
    })),
  };
  
  await conversionService.create(payload);
  toast.success('Conversion initiated');
};
</script>
```

### Conversion Monitoring

**Component:** `ui/src/pages/conversions/[id].vue`

Displays:
- Conversion status (queued, running, completed, failed)
- Input dataset details
- Argument values used
- Output datasets (when completed)
- Logs (if captured)

---

## CMG Migration Strategy

### CMG Legacy System

CMG used a simpler conversion model:
- **MongoDB-based** with basic conversion tracking
- **String-based pipeline definitions** (e.g., `"fastq_to_bam"`)
- **Minimal argument validation**
- **Direct pipeline processing**

### Migration Approach

#### 1. Map CMG Pipelines to Conversion Definitions

**CMG Pipeline:**
```javascript
{
  "_id": "...",
  "user": "user_id",
  "dataset": "dataset_id",
  "pipeline": "10_RHS24_MERGED_ANALYSIS",
  "options": {
    "reference": "hg38",
    "threads": 16
  },
  "status": "completed",
  "output_path": "/data/outputs/..."
}
```

**Bioloop Conversion Definition:**
```javascript
{
  "name": "10_RHS24_MERGED_ANALYSIS",
  "description": "CMG legacy pipeline 10 (RHS24 merged analysis)",
  "enabled": true,
  "dataset_types": ["raw_data", "fastq"],
  "tags": ["cmg", "legacy", "rhs24"],
  "program": {
    "name": "rhs24_pipeline",
    "executable_path": "/opt/cmg/pipelines/rhs24_merged.sh",
    "arguments": [
      {
        "name": "reference",
        "value_type": "STRING",
        "allowed_values": ["hg38", "hg19"],
        "default_value": "hg38"
      },
      {
        "name": "threads",
        "value_type": "NUMBER",
        "default_value": "8",
        "min_value": 1,
        "max_value": 32
      }
    ]
  }
}
```

#### 2. Migrate Existing Conversions

**Script:** `db_conversion/src/convert/scripts/migrate_conversions.py`

```python
def migrate_cmg_conversions():
    """
    Migrate CMG conversions to Bioloop schema.
    """
    # Fetch CMG conversions from MongoDB
    cmg_conversions = mongo_db.conversions.find({})
    
    for cmg_conv in cmg_conversions:
        # Find or create matching definition
        definition = find_or_create_definition(cmg_conv['pipeline'])
        
        # Find migrated dataset
        dataset = find_bioloop_dataset_by_cmg_id(cmg_conv['dataset'])
        
        # Create conversion record
        conversion = prisma.conversion.create({
            'data': {
                'cmg_id': str(cmg_conv['_id']),
                'definition_id': definition.id,
                'dataset_id': dataset.id if dataset else None,
                'initiated_at': cmg_conv.get('created_at'),
                'workflow_id': cmg_conv.get('worker_id'),
            }
        })
        
        # Migrate options to argument_values
        for key, value in cmg_conv.get('options', {}).items():
            arg = find_argument_by_name(definition.program.id, key)
            if arg:
                prisma.argument_value.create({
                    'data': {
                        'argument_id': arg.id,
                        'conversion_id': conversion.id,
                        'value': str(value),
                    }
                })
```

#### 3. Preserve CMG Behavior

- Maintain CMG pipeline names for continuity
- Store `cmg_id` in conversion records for traceability
- Support legacy option names via argument name mapping
- Preserve output directory structure

---

## Key Code Locations

### API (Express.js)
```
api/src/
├── routes/
│   └── conversions.js          # Conversion CRUD & definition endpoints
├── services/
│   └── conversion.js           # Conversion business logic
└── prisma/
    ├── schema.prisma           # Database schema
    └── seed_data/
        └── conversion_definitions.js  # Seeded conversion definitions
```

### Workers (Python)
```
workers/workers/
├── tasks/
│   └── convert.py              # Generic conversion execution engine
└── config/
    └── common.py               # Worker configuration
```

### UI (Vue 3)
```
ui/src/
├── pages/
│   └── conversions/
│       ├── index.vue           # Conversion list
│       ├── new.vue             # Initiate conversion
│       ├── [id].vue            # Conversion details
│       └── definitions/
│           └── index.vue       # Browse conversion definitions
└── services/
    └── conversion.js           # Conversion API client
```

### Database Migrations
```
api/prisma/migrations/
└── 20250909172706_conversions/
    └── migration.sql           # Initial conversion schema
```

---

## Summary

Bioloop's conversion system provides a **powerful, extensible framework** for managing bioinformatics pipelines:

### Key Strengths
1. **Generic Architecture**: Add new pipelines without code changes
2. **Type Safety**: Strong validation of arguments and values
3. **Traceability**: Complete audit trail from input → conversion → outputs
4. **CMG Compatible**: Migrate existing CMG pipelines seamlessly
5. **Flexible Execution**: Support for any command-line tool

### Integration Points
1. **Datasets** → Conversions → **Derived Datasets** (data flow)
2. **Conversions** → **Tracks** (BAM/VCF outputs become tracks)
3. **Conversions** → **Sessions** (view converted data in genome browsers)

### Next Steps for Developers
1. **Review** `api/prisma/seed_data/conversion_definitions.js` for examples
2. **Add** new conversion definitions via database seeds
3. **Test** conversions with sample datasets
4. **Monitor** worker logs for execution details

For detailed implementation of Sessions and Tracks that use conversion outputs, see:
- `genome-browser-sessions-tracks-conversions-2026-01-03.md`
- `genome-browser-igv-washu-implementation-2026-01-03.md`

---

**Last Updated**: 2026-01-11  
**Status**: ✅ Fully Documented


