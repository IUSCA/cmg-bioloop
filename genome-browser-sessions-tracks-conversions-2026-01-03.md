# Genome Browser, Sessions, Tracks & Conversions Architecture
**Documentation Date:** 2026-01-03  
**For:** AI Agents & Developers

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Database Schema](#database-schema)
3. [Core Entities & Relationships](#core-entities--relationships)
4. [Genome Browser Sessions](#genome-browser-sessions)
5. [Tracks](#tracks)
6. [Conversions](#conversions)
7. [API Endpoints](#api-endpoints)
8. [UI Components](#ui-components)
9. [File Serving & Authentication](#file-serving--authentication)
10. [Key Code Locations](#key-code-locations)

---

## System Overview

Bioloop is a microservice architecture for genomic data management with three main components:
- **API**: Express.js backend with Prisma ORM (PostgreSQL)
- **UI**: Vue 3 frontend with Vuestic UI
- **Workers**: Python-based processing workers (Celery)

### Key Features
1. **Dataset Management**: Upload, stage, and manage genomic datasets
2. **Genome Browser Sessions**: Create sessions to visualize tracks in IGV or WashU browsers
3. **Track Management**: Configure genomic tracks from dataset files
4. **Data Conversions**: Execute bioinformatics pipeline conversions (e.g., FASTQ → BAM)

---

## Database Schema

### Core Models (Prisma)

#### `dataset`
Primary entity for genomic data bundles.

```prisma
model dataset {
  id                       Int                          @id @default(autoincrement())
  name                     String
  type                     String                       // e.g., "raw_data", "data_product"
  is_staged                Boolean                      @default(false)
  metadata                 Json?                        // {stage_alias: "staged/data_products/hash/..."}
  files                    dataset_file[]
  genomic_details          dataset_genomic_attributes?
  conversions              conversion[]
  // ... other fields
}
```

**Key Fields:**
- `metadata.stage_alias`: Path prefix for staged files (e.g., `"staged/data_products/c10895aea636715eeec70b3a129d4b17"`)
- `is_staged`: Whether files are available for genome browser access
- `type`: Dataset type (raw_data, data_product, etc.)

#### `dataset_genomic_attributes`
Genomic metadata for datasets.

```prisma
model dataset_genomic_attributes {
  dataset_id   Int     @id
  dataset      dataset @relation(fields: [dataset_id], references: [id], onDelete: Cascade)
  genome_type  String? // e.g., "human", "mouse"
  genome_value String? // e.g., "hg38", "mm10"
}
```

#### `dataset_file`
Individual files within a dataset.

```prisma
model dataset_file {
  id         Int      @id @default(autoincrement())
  path       String   // Relative path within dataset (e.g., "bigwig_dp/file.bw")
  name       String?
  size       BigInt?
  filetype   String?  // e.g., "BAM", "BIGWIG", "VCF_GZ"
  dataset_id Int
  dataset    dataset  @relation(fields: [dataset_id], references: [id], onDelete: Cascade)
  track      track?
}
```

**Key Relationships:**
- One dataset file can have one track
- Files have parent-child relationships (e.g., BAM + BAI index)

#### `track`
Genome browser track configuration.

```prisma
model track {
  id              Int             @id @default(autoincrement())
  name            String
  dataset_file_id Int             @unique
  dataset_file    dataset_file    @relation(fields: [dataset_file_id], references: [id], onDelete: Cascade)
  color           String?
  session_tracks  session_track[]
}
```

**Characteristics:**
- One-to-one with `dataset_file`
- Can be reused across multiple sessions

#### `genome_browser_session`
User-created genome browser sessions.

```prisma
model genome_browser_session {
  id                   Int             @id @default(autoincrement())
  title                String?
  genome               String?         // Assembly (e.g., "hg38")
  genome_type          String?         // Type (e.g., "human")
  user_id              Int?
  is_public            Boolean         @default(false)
  staging_requested    Json?
  staging_completed    Boolean         @default(false)
  session_tracks       session_track[]
  user                 user?           @relation(fields: [user_id], references: [id])
}
```

**Key Points:**
- Sessions define which tracks to display together
- `genome` and `genome_type` are optional (can be empty if session has no specific genome)
- Tracks can come from different datasets

#### `session_track`
Join table linking sessions and tracks.

```prisma
model session_track {
  id         Int                    @id @default(autoincrement())
  session_id Int
  track_id   Int
  color      String?                // Override track color
  title      String?                // Override track name
  order      Int                    @default(0)
  session    genome_browser_session @relation(fields: [session_id], references: [id])
  track      track                  @relation(fields: [track_id], references: [id])
  
  @@unique([session_id, track_id])
}
```

---

## Core Entities & Relationships

### Relationship Diagram (Text)
```
User
  └── genome_browser_session (1:N)
        └── session_track (1:N)
              └── track (N:1)
                    └── dataset_file (1:1)
                          └── dataset (N:1)
                                └── dataset_genomic_attributes (1:1)
                                └── conversion (1:N)
```

### Key Relationships
1. **User → Sessions**: A user owns multiple sessions
2. **Session → Tracks**: A session contains multiple tracks (via `session_track`)
3. **Track → Dataset File**: Each track references exactly one primary file
4. **Dataset File → Dataset**: Files belong to a dataset
5. **Dataset → Genomic Attributes**: Optional genome metadata
6. **Dataset → Conversions**: Datasets can be inputs to conversions

---

## Genome Browser Sessions

### Session Lifecycle

#### 1. Creation
**UI:** `ui/src/pages/sessions/new.vue`

```vue
<script setup>
const form = ref({
  session_name: '',
  genome_type: '',  // Selected from constants.GENOME_TYPES
  genome: '',       // Filtered by genome_type
  track_ids: [],    // Selected track IDs
  is_public: false,
});
</script>
```

**API:** `POST /sessions`

```javascript
// api/src/routes/sessions.js
router.post(
  '/',
  isPermittedTo('create'),
  [
    body('session_name').isString().trim().notEmpty(),
    body('genome_type').optional().isString(),
    body('genome').optional().isString(),
    body('track_ids').isArray(),
    body('is_public').optional().isBoolean(),
  ],
  asyncHandler(async (req, res) => {
    const { session_name, genome_type, genome, track_ids, is_public } = req.body;
    
    // Validate tracks exist and are accessible
    const tracks = await validateTracksForSession(track_ids);
    
    // Create session
    const session = await prisma.genome_browser_session.create({
      data: {
        title: session_name,
        genome_type,
        genome,
        is_public,
        user_id: req.user.id,
        session_tracks: {
          create: track_ids.map((trackId, index) => ({
            track_id: trackId,
            order: index,
          })),
        },
      },
      include: { session_tracks: { include: { track: true } } },
    });
    
    res.status(201).json({ session });
  })
);
```

#### 2. Viewing
**UI:** `ui/src/pages/sessions/[id].vue`

Displays session details:
- Session metadata (title, genome, created date)
- List of associated tracks
- "View in Genome Browser" button

#### 3. Opening in Browser
User flow:
1. Click "View in Genome Browser"
2. Modal opens with browser selection (IGV or WashU)
3. Select browser → Fetch datahub config → Render browser

**API:** `GET /sessions/:id/datahub?browser=igv|washu`

Returns browser-specific track configuration (see [Genome Browser Implementation](#genome-browser-igv-washu-implementation-2026-01-03md) doc).

---

## Tracks

### Track Creation
Tracks are created from dataset files.

**UI:** `ui/src/pages/tracks/new.vue`

```vue
<script setup>
const form = ref({
  name: '',
  dataset_file_id: null, // Selected via async autocomplete
  color: '#2669a3',
});
</script>
```

**API:** `POST /tracks`

```javascript
router.post(
  '/',
  isPermittedTo('create'),
  [
    body('name').isString().trim().notEmpty(),
    body('dataset_file_id').isInt(),
    body('color').optional().isString(),
  ],
  asyncHandler(async (req, res) => {
    const { name, dataset_file_id, color } = req.body;
    
    // Verify file exists
    const datasetFile = await prisma.dataset_file.findUnique({
      where: { id: dataset_file_id },
      include: { dataset: true },
    });
    
    if (!datasetFile) throw createError(404, 'Dataset file not found');
    
    // Create track
    const track = await prisma.track.create({
      data: { name, dataset_file_id, color },
      include: { dataset_file: { include: { dataset: true } } },
    });
    
    res.status(201).json({ track });
  })
);
```

### Track File Types
Supported browser-compatible file types:

```javascript
// ui/src/constants.js
genomeBrowser: {
  browserCompatibleFormats: [
    'BAM',
    'CRAM',
    'VCF_GZ',
    'BIGWIG',
    'BIGBED',
    'FRAGMENTS_TSV_GZ',
    'BED_GZ',
  ],
  indexFormats: ['BAI', 'CRAI', 'TBI', 'CSI'],
}
```

### Track Selection in Sessions
Tracks are selected via async autocomplete:

**Component:** `ui/src/components/tracks/TracksAsyncAutoComplete.vue`

```vue
<va-async-auto-complete
  v-model="selectedTracks"
  :fetch-data="fetchTracks"
  :multiple="true"
  placeholder="Search tracks..."
/>
```

**Service:** `ui/src/services/track.js`

```javascript
async search(params) {
  return api.get('/tracks', { params });
}
```

---

## Conversions

Conversions are bioinformatics pipeline executions that transform datasets.

### Conversion Architecture

```
conversion_definition (pipeline template)
  ├── cmd_line_program (executable + arguments)
  │     └── argument (parameter definitions)
  └── conversion (execution instance)
        ├── argument_value (runtime values)
        └── conversion_derived_dataset (output datasets)
```

### Database Models

#### `conversion_definition`
Pipeline template defining a conversion type.

```prisma
model conversion_definition {
  id               Int              @id @default(autoincrement())
  name             String           @unique
  description      String?
  enabled          Boolean          @default(false)
  dataset_types    String[]         // Allowed input types
  program_id       Int
  program          cmd_line_program @relation(fields: [program_id], references: [id])
  conversions      conversion[]
}
```

**Example:** "FASTQ to BAM" conversion definition

#### `cmd_line_program`
Executable program with defined arguments.

```prisma
model cmd_line_program {
  id                    Int       @id @default(autoincrement())
  name                  String    @unique
  executable_path       String    // e.g., "/usr/local/bin/bwa"
  arguments             argument[]
  allow_additional_args Boolean   @default(false)
}
```

#### `argument`
Parameter definition for a program.

```prisma
model argument {
  id                   Int                @id @default(autoincrement())
  flag                 String?            // e.g., "-r", "--reference"
  data_type            String             // "INTEGER", "STRING", "FILE"
  is_required          Boolean            @default(false)
  default_value        String?
  dynamic_variable_key String?            // Reference to dynamic_variable
  program_id           Int
  program              cmd_line_program   @relation(fields: [program_id], references: [id])
}
```

#### `conversion`
Execution instance of a conversion.

```prisma
model conversion {
  id               Int                          @id @default(autoincrement())
  cmg_id           String?
  initiated_at     DateTime                     @default(now())
  definition_id    Int
  workflow_id      String?                      // Celery workflow ID
  dataset_id       Int?
  definition       conversion_definition        @relation(fields: [definition_id], references: [id])
  dataset          dataset?                     @relation(fields: [dataset_id], references: [id])
  argument_values  argument_value[]
  derived_datasets conversion_derived_dataset[]
  additional_args  Json?
}
```

#### `conversion_derived_dataset`
Output datasets from a conversion.

```prisma
model conversion_derived_dataset {
  conversion_id Int
  dataset_id    Int
  metadata      Json?
  conversion    conversion @relation(fields: [conversion_id], references: [id])
  dataset       dataset    @relation(fields: [dataset_id], references: [id])
  
  @@id([conversion_id, dataset_id])
}
```

### Conversion Workflow

1. **User Initiates Conversion:**
   - UI: Select dataset, conversion definition, provide argument values
   - API: `POST /conversions` creates `conversion` record

2. **Worker Processes Conversion:**
   - Celery worker picks up task
   - Executes command-line program
   - Creates output dataset(s)
   - Links via `conversion_derived_dataset`

3. **Result:**
   - New dataset(s) created
   - Can be used to create tracks for genome browser sessions

### Conversion Script (Data Sync)

**Location:** `data_sync/src/sync/bigbang/sync_conversions.js`

```python
class MongoToPostgresConversionManager:
    """
    Handles data migration from MongoDB (CMG legacy) to PostgreSQL (Bioloop).
    Includes conversion of conversion pipelines and their metadata.
    """
    
    def convert_conversions(self):
        """Convert conversion definitions and instances from MongoDB to PostgreSQL."""
        # Fetch conversion definitions from MongoDB
        # Map to PostgreSQL schema
        # Create conversion_definition, cmd_line_program, argument records
```

---

## API Endpoints

### Sessions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/sessions` | List all sessions |
| GET | `/sessions/:id` | Get session details |
| POST | `/sessions` | Create new session |
| PATCH | `/sessions/:id` | Update session |
| DELETE | `/sessions/:id` | Delete session |
| GET | `/sessions/:id/datahub` | Get browser-specific track config |
| POST | `/sessions/:id/set-file-cookie` | Set auth cookie for file access |
| GET | `/sessions/:id/files/expose/*` | Serve dataset files for browsers |

### Tracks

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tracks` | Search/list tracks |
| GET | `/tracks/:id` | Get track details |
| POST | `/tracks` | Create new track |
| PATCH | `/tracks/:id` | Update track |
| DELETE | `/tracks/:id` | Delete track |

### Conversions

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/conversions` | List conversions |
| GET | `/conversions/:id` | Get conversion details |
| POST | `/conversions` | Initiate new conversion |
| GET | `/conversion-definitions` | List available pipelines |

---

## UI Components

### Session Management

#### `ui/src/pages/sessions/index.vue`
- Lists all sessions for the user
- Filterable by genome type, genome assembly
- Search by title

#### `ui/src/pages/sessions/new.vue`
- Create new session form
- Genome type/assembly dropdowns (populated from `constants.GENOME_TYPES`)
- Track selection via `TracksAsyncAutoComplete`

```vue
<va-select
  v-model="form.genome_type"
  :options="genomeTypeOptions"
  text-by="text"
  value-by="value"
  label="Genome Type"
/>

<va-select
  v-model="form.genome"
  :options="availableAssemblies"
  text-by="text"
  value-by="value"
  label="Genome Assembly"
/>
```

**Auto-population logic:**
- When tracks are selected, if `genome_type` and `genome` are empty, auto-populate from track dataset genomic details
- Preserves manual selections

#### `ui/src/pages/sessions/[id].vue`
- Session detail view
- Track list
- "View in Genome Browser" button
- Browser selection modal → Browser rendering modal

```vue
<va-button @click="showBrowserSelectionModal = true">
  View in Genome Browser
</va-button>

<BrowserSelectionModal
  v-model="showBrowserSelectionModal"
  @browser-selected="handleBrowserSelection"
/>
```

### Track Management

#### `ui/src/pages/tracks/index.vue`
- List all tracks
- Search by name, genome type, genome value

#### `ui/src/pages/tracks/new.vue`
- Create new track form
- Dataset file selection via async autocomplete

#### `ui/src/components/tracks/TracksAsyncAutoComplete.vue`
- Async searchable dropdown for track selection
- Supports multiple selection
- Displays track name, genome info, dataset name

```vue
<script setup>
const fetchTracks = async (inputValue) => {
  const response = await trackService.search({
    search: inputValue,
    limit: 20,
  });
  return response.data.tracks;
};
</script>
```

### Conversion Management

#### `ui/src/pages/conversions/index.vue`
- List all conversions
- Filter by status, definition

#### `ui/src/pages/conversions/new.vue`
- Initiate new conversion
- Select conversion definition
- Provide argument values
- Choose input dataset

---

## File Serving & Authentication

### File Exposure for Genome Browsers

Genome browsers (IGV, WashU) need to fetch genomic files (BAM, BigWig, etc.) via HTTP.

#### Authentication Flow

1. **Set Cookie:**
   - UI calls `POST /sessions/:id/set-file-cookie`
   - API generates JWT with session scope
   - Sets `HttpOnly`, `Secure`, `SameSite=Lax` cookie

```javascript
// api/src/routes/sessions.js
router.post(
  '/:id/set-file-cookie',
  isPermittedTo('read'),
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    
    // Generate JWT with session_id in payload
    const token = jwt.sign(
      { user_id: req.user.id, session_id: sessionId },
      JWT_SECRET,
      { expiresIn: '1h' }
    );
    
    res.cookie('bioloop_session_token', token, {
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
      maxAge: 3600000, // 1 hour
    });
    
    res.json({ success: true });
  })
);
```

2. **File Access:**
   - Browser requests `GET /sessions/:id/files/expose/path/to/file.bam`
   - Middleware validates cookie, checks session permissions
   - File streamed with range support

```javascript
// api/src/routes/sessions.js
fileExposureRouter.get(
  '/:id/files/expose/*',
  authenticateWithCookie,
  asyncHandler(async (req, res) => {
    const sessionId = parseInt(req.params.id);
    const requestedPath = req.params[0];
    
    // Verify user has access to this session
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      include: { session_tracks: { include: { track: { include: { dataset_file: true } } } } },
    });
    
    if (!session) throw createError(404, 'Session not found');
    
    // Check if requested file is in this session's tracks
    const fileInSession = session.session_tracks.some((st) => {
      const relativePath = getRelativeFilePath({
        dataset: st.track.dataset_file.dataset,
        datasetFile: st.track.dataset_file,
      });
      return requestedPath === relativePath;
    });
    
    if (!fileInSession) throw createError(403, 'File not in session');
    
    // Construct absolute file path
    const absolutePath = path.join(DATA_ROOT, requestedPath);
    
    // Disable compression for binary files
    res.locals.compress = false;
    res.set('Content-Encoding', 'identity');
    
    // Stream file with range support
    const stat = await fs.promises.stat(absolutePath);
    res.set('Content-Length', stat.size);
    res.set('Accept-Ranges', 'bytes');
    
    const readStream = fs.createReadStream(absolutePath);
    readStream.pipe(res);
  })
);
```

#### Key Points
- **Cookie-based auth** avoids CORS preflight issues
- **Session-scoped tokens** limit access to specific session files
- **Range request support** for efficient binary file streaming
- **Compression disabled** for binary files (BAM, BigWig)

---

## Key Code Locations

### API (Express.js)
```
api/src/
├── routes/
│   ├── sessions.js          # Session & file exposure endpoints
│   ├── tracks.js            # Track CRUD endpoints
│   └── conversions.js       # Conversion endpoints
├── services/
│   └── dataset.js           # Dataset utilities
├── utils/
│   └── genomeBrowserUtils.js # Track serialization helpers
└── middleware/
    └── auth.js              # Cookie authentication
```

### UI (Vue 3)
```
ui/src/
├── pages/
│   ├── sessions/
│   │   ├── index.vue        # Session list
│   │   ├── new.vue          # Create session
│   │   └── [id].vue         # Session detail + browser rendering
│   ├── tracks/
│   │   ├── index.vue        # Track list
│   │   └── new.vue          # Create track
│   └── conversions/
│       ├── index.vue        # Conversion list
│       └── new.vue          # Initiate conversion
├── components/
│   ├── genomeBrowser/
│   │   ├── BrowserSelectionModal.vue
│   │   └── WashUBrowser.vue
│   └── tracks/
│       └── TracksAsyncAutoComplete.vue
├── services/
│   ├── session.js           # Session API client
│   ├── track.js             # Track API client
│   └── conversion.js        # Conversion API client
└── constants.js             # GENOME_TYPES, browser constants
```

### Database
```
api/prisma/
├── schema.prisma            # Prisma schema definition
└── migrations/              # Database migrations
```

### Data Sync (Node.js)
```
data_sync/src/sync/
├── bigbang/
│   └── sync_conversions.js  # MongoDB → PostgreSQL migration
├── pollers/                 # Incremental sync
└── utils/                   # Helper functions
```

---

## Quick Reference: Entity Relationships

### How to find tracks for a session?
```javascript
const session = await prisma.genome_browser_session.findUnique({
  where: { id: sessionId },
  include: {
    session_tracks: {
      include: {
        track: {
          include: {
            dataset_file: {
              include: {
                dataset: {
                  include: { genomic_details: true },
                },
              },
            },
          },
        },
      },
    },
  },
});

// Access:
session.session_tracks[0].track.dataset_file.dataset.genomic_details.genome_type
```

### How to find datasets from a conversion?
```javascript
const conversion = await prisma.conversion.findUnique({
  where: { id: conversionId },
  include: {
    dataset: true,                    // Input dataset
    derived_datasets: {
      include: { dataset: true },    // Output datasets
    },
  },
});
```

### How to check if a file can be used as a track?
```javascript
const BROWSER_COMPATIBLE = ['BAM', 'BIGWIG', 'VCF_GZ', 'BIGBED', 'BED_GZ'];
const file = await prisma.dataset_file.findUnique({ where: { id: fileId } });
const canBeTrack = BROWSER_COMPATIBLE.includes(file.filetype);
```

---

## Summary

**Genome Browser Sessions** are collections of **Tracks** that reference **Dataset Files** from **Datasets**. Datasets can be created manually or via **Conversions** (bioinformatics pipelines). Sessions are rendered in **IGV** or **WashU** browsers via cookie-authenticated file serving.

**Key Integration Points:**
1. Datasets → Tracks → Sessions (data flow)
2. Conversions → Derived Datasets → Tracks (pipeline outputs)
3. Sessions → Browsers → File Serving (visualization)

For browser-specific implementation details (IGV/WashU), see: `genome-browser-igv-washu-implementation-2026-01-03.md`

