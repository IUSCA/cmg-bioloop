# IGV & WashU Genome Browser Implementation Guide
**Documentation Date:** 2026-01-03  
**For:** AI Agents & Developers

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Browser Constants](#browser-constants)
3. [Common Infrastructure](#common-infrastructure)
4. [IGV Browser Implementation](#igv-browser-implementation)
5. [WashU Browser Implementation](#washu-browser-implementation)
6. [Track Serialization](#track-serialization)
7. [Authentication & File Serving](#authentication--file-serving)
8. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

Bioloop supports two embedded genome browsers:
- **IGV.js**: Pure JavaScript browser, rendered in a `<div>`
- **WashU Epigenome Browser (wuepgg)**: React-based browser, wrapped in Vue component

### Key Design Principles
1. **Generic Terminology**: Use "Genome Browser" in variable/method names, not browser-specific names
2. **Unified UI**: Single "View in Genome Browser" button → Modal to choose IGV or WashU
3. **Browser-Specific Serialization**: API endpoint returns different JSON based on `?browser=igv|washu`
4. **Cookie-Based Auth**: Avoids CORS preflight issues for file access
5. **No Duplication**: Shared utilities for file path resolution, track validation

---

## Browser Constants

### UI Constants (`ui/src/constants.js`)

```javascript
export default {
  genomeBrowser: {
    // Browser type identifiers
    browserTypes: {
      IGV: 'igv',
      WASHU: 'washu',
    },
    
    // Display labels for UI
    browserLabels: {
      igv: 'IGV Browser',
      washu: 'WashU Epigenome Browser',
    },
    
    // Modal titles
    browserTitles: {
      igv: 'IGV Genome Browser',
      washu: 'WashU Epigenome Browser',
      default: 'Genome Browser',
    },
    
    // Default browser selection
    defaultBrowser: 'igv',
    
    // Supported file formats
    browserCompatibleFormats: [
      'BAM', 'CRAM', 'VCF_GZ', 'BIGWIG', 'BIGBED', 
      'FRAGMENTS_TSV_GZ', 'BED_GZ',
    ],
    indexFormats: ['BAI', 'CRAI', 'TBI', 'CSI'],
  },
};
```

### API Constants (`api/src/routes/sessions.js`)

```javascript
const BROWSER_TYPES = {
  IGV: 'igv',
  WASHU: 'washu',
};
```

**Usage:**
```javascript
// Instead of:
if (browserType === 'igv') { ... }

// Use:
if (browserType === BROWSER_TYPES.IGV) { ... }
```

---

## Common Infrastructure

### Browser Selection Modal

**Component:** `ui/src/components/genomeBrowser/BrowserSelectionModal.vue`

```vue
<template>
  <va-modal
    v-model="showModal"
    title="Choose Genome Browser"
    size="small"
    ok-text="Open Browser"
    @ok="openBrowser"
    @cancel="closeModal"
  >
    <div class="space-y-4">
      <p class="text-sm">Select which genome browser to view this session:</p>

      <div class="flex flex-col gap-3">
        <va-radio
          v-model="selectedBrowser"
          :option="BROWSER_TYPES.IGV"
          :label="BROWSER_LABELS[BROWSER_TYPES.IGV]"
        />
        <va-radio
          v-model="selectedBrowser"
          :option="BROWSER_TYPES.WASHU"
          :label="BROWSER_LABELS[BROWSER_TYPES.WASHU]"
        />
      </div>
    </div>
  </va-modal>
</template>

<script setup>
import { ref, watch } from 'vue';
import constants from '@/constants';

const {
  browserTypes: BROWSER_TYPES,
  browserLabels: BROWSER_LABELS,
  defaultBrowser: DEFAULT_BROWSER,
} = constants.genomeBrowser;

const emit = defineEmits(['browser-selected', 'close']);

const showModal = defineModel({ type: Boolean, default: false });
const selectedBrowser = ref(DEFAULT_BROWSER);

const openBrowser = () => {
  emit('browser-selected', selectedBrowser.value);
  showModal.value = false;
};

const closeModal = () => {
  showModal.value = false;
  emit('close');
};

// Reset selection to default when modal is closed
watch(showModal, (isOpen) => {
  if (!isOpen) {
    selectedBrowser.value = DEFAULT_BROWSER;
  }
});
</script>
```

### Browser Selection Flow

**Parent Component:** `ui/src/pages/sessions/[id].vue`

```vue
<template>
  <!-- Trigger Button -->
  <va-button @click="showBrowserSelectionModal = true">
    View in Genome Browser
  </va-button>

  <!-- Selection Modal -->
  <BrowserSelectionModal
    v-model="showBrowserSelectionModal"
    @browser-selected="handleBrowserSelection"
  />

  <!-- Browser Rendering Modal -->
  <va-modal
    v-model="showGenomeBrowserModal"
    :title="genomeBrowserTitle"
    fullscreen
    hide-default-actions
    no-padding
    no-outside-dismiss
    :disable-attachment="true"
    @close="closeGenomeBrowser"
  >
    <div class="h-full flex flex-col" @click.stop>
      <!-- IGV Container -->
      <div
        v-if="selectedBrowserType === BROWSER_TYPES.IGV"
        id="igv-container"
        class="flex-1"
        style="min-height: 600px"
      ></div>

      <!-- WashU Container -->
      <WashUBrowser
        v-if="showGenomeBrowserModal && selectedBrowserType === BROWSER_TYPES.WASHU"
        :key="washuMountKey"
        :genome-name="genomeBrowserGenome"
        :data-hub="genomeBrowserTracks"
        :view-region="genomeBrowserRegion"
        class="h-full flex-1"
      />
    </div>
  </va-modal>
</template>

<script setup>
import constants from '@/constants';

const { browserTypes: BROWSER_TYPES, browserTitles: BROWSER_TITLES } = 
  constants.genomeBrowser;

const showBrowserSelectionModal = ref(false);
const showGenomeBrowserModal = ref(false);
const selectedBrowserType = ref(null);
const genomeBrowserGenome = ref('');
const genomeBrowserTracks = ref([]);
const genomeBrowserRegion = ref('');
const washuMountKey = ref(0);

const genomeBrowserTitle = computed(() => {
  if (selectedBrowserType.value === BROWSER_TYPES.IGV) return BROWSER_TITLES.igv;
  if (selectedBrowserType.value === BROWSER_TYPES.WASHU) return BROWSER_TITLES.washu;
  return BROWSER_TITLES.default;
});

const handleBrowserSelection = async (browserType) => {
  selectedBrowserType.value = browserType;

  if (browserType === BROWSER_TYPES.IGV) {
    await initializeIGV();
  } else if (browserType === BROWSER_TYPES.WASHU) {
    await initializeWashU();
  }
};
</script>
```

---

## IGV Browser Implementation

### Dependencies

**Package:** `igv` (npm)

```json
{
  "dependencies": {
    "igv": "^3.0.0"
  }
}
```

### Initialization Flow

**Function:** `initializeIGV()` in `ui/src/pages/sessions/[id].vue`

```javascript
const initializeIGV = async () => {
  if (!session.value) return;

  genomeBrowserLoading.value = true;

  try {
    // 1. Set authentication cookie
    await sessionService.setFileCookie(session.value.id);

    // 2. Fetch IGV-specific datahub configuration
    const datahubResponse = await sessionService.getDatahub(
      session.value.id, 
      BROWSER_TYPES.IGV
    );
    const datahubConfig = datahubResponse?.data;

    const tracks = datahubConfig?.tracks || [];
    const genome = datahubConfig?.genome;

    if (!tracks || tracks.length === 0) {
      toast.error('No tracks available for this session');
      genomeBrowserLoading.value = false;
      return;
    }

    // 3. Store for modal display
    genomeBrowserGenome.value = genome;
    genomeBrowserTracks.value = tracks;

    // 4. Show genome browser modal
    showGenomeBrowserModal.value = true;

    // 5. Wait for DOM to update
    await nextTick();

    // 6. Dynamically import IGV
    const igvModule = await import('igv');
    const igv = igvModule.default;

    // 7. Configure IGV options
    const igvOptions = {
      genome,
      tracks,
    };

    // 8. Create IGV browser instance
    const container = document.getElementById('igv-container');
    if (container) {
      igvBrowser = await igv.createBrowser(container, igvOptions);
      console.log('[IGV] Browser loaded successfully');
    } else {
      throw new Error('IGV container not found');
    }
  } catch (error) {
    console.error('[IGV] Failed to initialize:', error);
    toast.error('Failed to load IGV browser');
    showGenomeBrowserModal.value = false;
  } finally {
    genomeBrowserLoading.value = false;
  }
};
```

### IGV Datahub Format

**API Response:** `GET /sessions/:id/datahub?browser=igv`

```json
{
  "genome": "hg38",
  "locus": "chr1:155000000-155050000",
  "tracks": [
    {
      "type": "wig",
      "format": "bigwig",
      "name": "Sample Track",
      "url": "/api/sessions/9/files/expose/staged/data_products/hash/file.bw",
      "indexURL": "/api/sessions/9/files/expose/staged/data_products/hash/file.bw.bai",
      "color": "#2669a3",
      "height": 100
    },
    {
      "type": "alignment",
      "format": "bam",
      "name": "Alignment Track",
      "url": "/api/sessions/9/files/expose/staged/data_products/hash/file.bam",
      "indexURL": "/api/sessions/9/files/expose/staged/data_products/hash/file.bam.bai",
      "color": "#2669a3",
      "height": 100
    }
  ]
}
```

### IGV Track Serialization

**Function:** `serializeTrackForIGV()` in `api/src/routes/sessions.js`

```javascript
function serializeTrackForIGV(sessionTrack, sessionId, filesByDataset) {
  const { track } = sessionTrack;
  const { dataset_file: datasetFile } = track;
  const { dataset } = datasetFile;
  const filePath = datasetFile?.path || datasetFile?.name || '';

  // Get file type configuration (type + format)
  const fileConfig = getGenomeBrowserFileConfig(filePath);
  if (!fileConfig) {
    logger.warn(`[IGV] Unsupported file type: ${filePath}`);
    return null;
  }

  // Build file exposure URL
  const relativePath = getRelativeFilePath({ dataset, datasetFile });
  const url = buildFileExposureUrl(sessionId, relativePath);
  
  // Track name (prioritize session override)
  const trackName = sessionTrack.title || track.name || datasetFile.name || 'Unnamed Track';

  // Build IGV track config
  const trackConfig = {
    type: fileConfig.igv.type,       // e.g., "wig", "alignment"
    format: fileConfig.igv.format,   // e.g., "bigwig", "bam"
    name: trackName,
    url,
    color: sessionTrack.color || '#2669a3',
    height: 100,
  };

  // Find and add index file if present
  const datasetFilesForThisDataset = filesByDataset[dataset.id] || [];
  const indexFile = findIndexFileForPrimary(datasetFilesForThisDataset, datasetFile);

  if (indexFile) {
    const indexRelativePath = getRelativeFilePath({ dataset, datasetFile: indexFile });
    const indexUrl = buildFileExposureUrl(sessionId, indexRelativePath);
    trackConfig.indexURL = indexUrl;
  }

  return trackConfig;
}
```

### File Type Configuration

**Function:** `getGenomeBrowserFileConfig()` in `api/src/routes/sessions.js`

```javascript
const getGenomeBrowserFileConfig = (filePath) => {
  if (!filePath) return null;
  const lowerPath = filePath.toLowerCase();

  if (lowerPath.endsWith('.bam')) {
    return {
      baseType: 'alignment',
      extension: 'bam',
      igv: { type: 'alignment', format: 'bam' },
      washu: { type: 'bam' },
    };
  }
  
  if (lowerPath.endsWith('.bw') || lowerPath.endsWith('.bigwig')) {
    return {
      baseType: 'signal',
      extension: 'bigwig',
      igv: { type: 'wig', format: 'bigwig' },
      washu: { type: 'bigwig' },
    };
  }
  
  if (lowerPath.endsWith('.vcf') || lowerPath.endsWith('.vcf.gz')) {
    return {
      baseType: 'variant',
      extension: 'vcf',
      igv: { type: 'variant', format: 'vcf' },
      washu: { type: 'vcf' },
    };
  }
  
  // ... other file types
  
  return null;
};
```

### IGV Cleanup

**Function:** `closeGenomeBrowser()` in `ui/src/pages/sessions/[id].vue`

```javascript
const closeGenomeBrowser = () => {
  // Clean up IGV browser instance if it exists
  if (igvBrowser) {
    igvBrowser.dispose();
    igvBrowser = null;
  }

  // Reset state
  showGenomeBrowserModal.value = false;
  selectedBrowserType.value = null;
  genomeBrowserGenome.value = '';
  genomeBrowserTracks.value = [];
  genomeBrowserRegion.value = '';

  console.log('[Genome Browser] Closed and cleaned up');
};
```

---

## WashU Browser Implementation

### Dependencies

**Packages:**
- `wuepgg`: WashU Epigenome Browser (React-based)
- `react`, `react-dom`: Required for rendering React components
- `json-stable-stringify`: For generating stable React keys

```json
{
  "dependencies": {
    "wuepgg": "^3.0.4",
    "react": "^18",
    "react-dom": "^18",
    "json-stable-stringify": "^1.3.0"
  }
}
```

### Architecture: React-in-Vue Wrapper

WashU is a React component, but Bioloop is a Vue 3 app. We use a **manual mounting approach** (no adapter libraries):

1. Create a Vue wrapper component with a `<div>` container
2. Use `react-dom/client`'s `createRoot()` to mount React component inside `onMounted()`
3. Unmount React component in `onBeforeUnmount()`

### WashU Wrapper Component

**Component:** `ui/src/components/genomeBrowser/WashUBrowser.vue`

```vue
<template>
  <div ref="washuRoot" class="h-full w-full"></div>
</template>

<script setup>
import { createRoot } from 'react-dom/client';
import { createElement } from 'react';
import { onMounted, onBeforeUnmount, ref } from 'vue';
import stableStringify from 'json-stable-stringify';

const props = defineProps({
  genomeName: { type: String, required: true },   // e.g., "hg38"
  dataHub: { type: Array, required: true },       // Track array
  viewRegion: { type: String, default: 'chr1:155000000-155050000' },
});

const washuRoot = ref(null);
let reactRoot = null;
let clearAllStoreCaches = null;

// Helper: Convert relative URLs to absolute (needed for Web Workers)
const toAbsoluteUrl = (url) => {
  if (!url) return url;
  if (url.startsWith('http://') || url.startsWith('https://')) return url;
  const origin = window.location.origin;
  return `${origin}${url.startsWith('/') ? '' : '/'}${url}`;
};

onMounted(async () => {
  // 🧹 NUCLEAR: Purge old WashU persisted state from localStorage
  try {
    const persistKeys = Object.keys(localStorage).filter((key) => key.startsWith('persist:'));
    if (persistKeys.length > 0) {
      console.log('[WashU] Clearing old persisted state:', persistKeys);
      persistKeys.forEach((key) => localStorage.removeItem(key));
    }
  } catch (e) {
    console.warn('[WashU] Failed to clear persisted state:', e);
  }

  // Import WashU components
  const mod = await import('wuepgg');
  console.log('wuepgg keys:', Object.keys(mod));

  const GenomeHub = mod.default ?? mod.GenomeHub; // Defensive import
  clearAllStoreCaches = mod.clearAllStoreCaches ?? null;

  if (!GenomeHub) {
    console.error('[WashU] GenomeHub component not found in wuepgg module');
    return;
  }

  // Prepare tracks with absolute URLs
  const dataHubPlain = props.dataHub.map((t) => ({
    ...t,
    url: toAbsoluteUrl(t.url), // WashU uses Web Workers which require absolute URLs
  }));

  // Generate unique store ID to prevent state rehydration
  const uniqueStoreId = `bioloop-washu-${Date.now()}`;

  // Construct WashU props
  const washuProps = {
    genomeName: props.genomeName,
    tracks: dataHubPlain,
    viewRegion: props.viewRegion,
    showToolBar: true,
    showNavBar: true,
    showGenomeNavigator: false,
    storeConfig: {
      storeId: uniqueStoreId,         // Unique ID prevents old state
      enablePersistence: false,       // Disable localStorage writes
    },
  };

  // Generate stable React key (forces remount if tracks change)
  const key = `washu:${props.genomeName}:${stableStringify(washuProps.tracks)}`;
  console.log('[WashU] Using React key:', key);

  // Create React root and render
  reactRoot = createRoot(washuRoot.value);
  const genomeViewerElement = createElement(GenomeHub, {
    ...washuProps,
    key,
  });

  reactRoot.render(genomeViewerElement);
  console.log('[WashU] Browser initialized successfully');
});

onBeforeUnmount(() => {
  // CLEANUP: Clear WashU global store caches
  try {
    if (clearAllStoreCaches) {
      clearAllStoreCaches();
      console.log('[WashU] Store caches cleared');
    }
  } catch (e) {
    console.warn('[WashU] clearAllStoreCaches failed:', e);
  }

  // Unmount React component
  if (reactRoot) {
    reactRoot.unmount();
    reactRoot = null;
  }
  console.log('[WashU] Browser unmounted');
});
</script>
```

### Key WashU Implementation Details

#### 1. Absolute URLs Requirement
WashU uses **Web Workers** for data fetching. Web Workers cannot resolve relative URLs, so all track URLs must be absolute:

```javascript
const toAbsoluteUrl = (url) => {
  if (url.startsWith('http://') || url.startsWith('https://')) return url;
  const origin = window.location.origin; // e.g., "https://localhost"
  return `${origin}${url.startsWith('/') ? '' : '/'}${url}`;
};
```

#### 2. Redux Persistence Disabled
WashU internally uses `redux-persist` which saves state to `localStorage`. This causes "zombie tracks" (old tracks persist across sessions). **Solution:**

```javascript
storeConfig: {
  storeId: `bioloop-washu-${Date.now()}`, // Unique ID each mount
  enablePersistence: false,                // Disable localStorage
}
```

Additionally, **one-time purge** of old `persist:*` keys on mount:

```javascript
const persistKeys = Object.keys(localStorage).filter((key) => key.startsWith('persist:'));
persistKeys.forEach((key) => localStorage.removeItem(key));
```

#### 3. React-Vue Event Isolation
React and Vue event systems can conflict, causing modals to close unexpectedly. **Solution:**

```vue
<!-- Parent modal -->
<va-modal :disable-attachment="true" no-outside-dismiss>
  <div @click.stop @mousedown.stop>
    <WashUBrowser ... />
  </div>
</va-modal>
```

- `:disable-attachment="true"`: Render modal as normal child (not portal)
- `@click.stop`, `@mousedown.stop`: Prevent React events from bubbling to Vue

#### 4. Force Remount on Track Changes
Use dynamic `:key` to force Vue to destroy and recreate the component when tracks change:

```vue
<WashUBrowser
  :key="washuMountKey"
  :genome-name="genomeBrowserGenome"
  :data-hub="genomeBrowserTracks"
/>

<script setup>
const washuMountKey = ref(0);

const initializeWashU = async () => {
  // ... fetch tracks ...
  washuMountKey.value++; // Increment to force remount
};
</script>
```

Inside `WashUBrowser.vue`, also use a React key:

```javascript
const key = `washu:${props.genomeName}:${stableStringify(washuProps.tracks)}`;

const genomeViewerElement = createElement(GenomeHub, {
  ...washuProps,
  key, // Forces React remount if key changes
});
```

### WashU Initialization Flow

**Function:** `initializeWashU()` in `ui/src/pages/sessions/[id].vue`

```javascript
const initializeWashU = async () => {
  if (!session.value) return;

  genomeBrowserLoading.value = true;

  try {
    // 1. Set authentication cookie
    await sessionService.setFileCookie(session.value.id);

    // 2. Fetch WashU-specific datahub configuration
    const datahubResponse = await sessionService.getDatahub(
      session.value.id,
      BROWSER_TYPES.WASHU
    );
    const datahubConfig = datahubResponse.data;

    const tracks = datahubConfig.tracks || [];
    const genome = datahubConfig.genome;

    if (!tracks || tracks.length === 0) {
      toast.error('No tracks available for this session');
      genomeBrowserLoading.value = false;
      return;
    }

    // 3. Store for modal display
    genomeBrowserGenome.value = genome;
    genomeBrowserTracks.value = tracks;
    genomeBrowserRegion.value = datahubConfig.locus || 'chr1:155000000-155050000';

    // 4. Force fresh WashU mount by incrementing key
    washuMountKey.value++;

    // 5. Show genome browser modal (WashU component will mount automatically)
    showGenomeBrowserModal.value = true;

    console.log('[WashU] Browser will initialize via component with mount key:', washuMountKey.value);
  } catch (error) {
    console.error('[WashU] Failed to initialize:', error);
    toast.error('Failed to load WashU browser');
    showGenomeBrowserModal.value = false;
  } finally {
    genomeBrowserLoading.value = false;
  }
};
```

### WashU Datahub Format

**API Response:** `GET /sessions/:id/datahub?browser=washu`

```json
{
  "genome": "hg38",
  "locus": "chr1:155000000-155050000",
  "tracks": [
    {
      "type": "bigwig",
      "name": "Sample Track",
      "url": "/api/sessions/9/files/expose/staged/data_products/hash/file.bw",
      "showOnHubLoad": true,
      "options": {
        "color": "#2669a3",
        "height": 100
      }
    },
    {
      "type": "bam",
      "name": "Alignment Track",
      "url": "/api/sessions/9/files/expose/staged/data_products/hash/file.bam",
      "indexURL": "/api/sessions/9/files/expose/staged/data_products/hash/file.bam.bai",
      "showOnHubLoad": true,
      "options": {
        "color": "#2669a3",
        "height": 100
      }
    }
  ]
}
```

**Key Differences from IGV:**
- `type` is lowercased (e.g., `"bigwig"`, not `"wig"`)
- No `format` field
- Track options (color, height) are nested in `options` object
- `showOnHubLoad` controls default visibility

### WashU Track Serialization

**Function:** `serializeTrackForWashU()` in `api/src/routes/sessions.js`

```javascript
function serializeTrackForWashU(sessionTrack, sessionId, filesByDataset) {
  const { track } = sessionTrack;
  const { dataset_file: datasetFile } = track;
  const { dataset } = datasetFile;
  const filePath = datasetFile?.path || datasetFile?.name || '';

  // Get file type configuration
  const fileConfig = getGenomeBrowserFileConfig(filePath);
  if (!fileConfig) {
    logger.warn(`[WashU] Unsupported file type: ${filePath}`);
    return null;
  }

  // Build file exposure URL
  const relativePath = getRelativeFilePath({ dataset, datasetFile });
  const url = buildFileExposureUrl(sessionId, relativePath);
  
  // Track name (prioritize session override)
  const trackName = sessionTrack.title || track.name || datasetFile.name || 'Unnamed Track';

  // Build WashU track config
  const trackConfig = {
    type: fileConfig.washu.type,    // e.g., "bigwig", "bam"
    name: trackName,
    url,
    showOnHubLoad: true,
    options: {
      color: sessionTrack.color || '#2669a3',
      height: 100,
    },
  };

  // Find and add index file if present
  const datasetFilesForThisDataset = filesByDataset[dataset.id] || [];
  const indexFile = findIndexFileForPrimary(datasetFilesForThisDataset, datasetFile);

  if (indexFile) {
    const indexRelativePath = getRelativeFilePath({ dataset, datasetFile: indexFile });
    const indexUrl = buildFileExposureUrl(sessionId, indexRelativePath);
    trackConfig.indexURL = indexUrl;
  }

  return trackConfig;
}
```

---

## Track Serialization

### API Datahub Endpoint

**Endpoint:** `GET /sessions/:id/datahub`

**Query Parameter:** `browser` (values: `igv`, `washu`)

```javascript
router.get(
  '/:id/datahub',
  isPermittedTo('read'),
  [
    param('id').isInt().toInt(),
    query('browser')
      .optional()
      .isIn(Object.values(BROWSER_TYPES))
      .default(BROWSER_TYPES.IGV),
  ],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;
    const browserType = req.query.browser || BROWSER_TYPES.IGV;

    // Fetch session with all tracks
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
                    parents: {
                      include: { parent: true },
                    },
                  },
                },
              },
            },
          },
          orderBy: { order: 'asc' },
        },
      },
    });

    if (!session) throw createError(404, 'Session not found');

    // Build filesByDataset map for index file lookup
    const datasetIds = [
      ...new Set(
        session.session_tracks.map((st) => st.track.dataset_file.dataset.id)
      ),
    ];

    const filesByDataset = {};
    for (const datasetId of datasetIds) {
      const files = await prisma.dataset_file.findMany({
        where: { dataset_id: datasetId },
        include: { parents: { include: { parent: true } } },
      });
      filesByDataset[datasetId] = files;
    }

    // Get genome from session
    const genome = session.genome;

    // Serialize tracks based on browser type
    if (browserType === BROWSER_TYPES.WASHU) {
      // WashU format: { genome, locus, tracks: [...] }
      const tracks = session.session_tracks
        .map((st) => serializeTrackForWashU(st, sessionId, filesByDataset))
        .filter(Boolean);

      const datahub = {
        genome,
        locus: 'chr1:155000000-155050000', // Default locus
        tracks,
      };

      res.json(datahub);
    } else {
      // IGV format: { genome, locus, tracks: [...] }
      const tracks = session.session_tracks
        .map((st) => serializeTrackForIGV(st, sessionId, filesByDataset))
        .filter(Boolean);

      const datahub = {
        genome,
        locus: 'chr1:155000000-155050000',
        tracks,
      };

      res.json(datahub);
    }
  })
);
```

### File Path Resolution

**Function:** `getRelativeFilePath()` in `api/src/routes/sessions.js`

```javascript
function getRelativeFilePath({ dataset, datasetFile }) {
  const stageAlias = dataset.metadata?.stage_alias || '';
  const filePath = datasetFile.path || '';

  const cleanedStageAlias = stageAlias.replace(/^\/+/, '').replace(/\/+$/, '');
  const cleanedFilePath = filePath.replace(/^\/+/, '');

  return cleanedStageAlias ? `${cleanedStageAlias}/${cleanedFilePath}` : cleanedFilePath;
}
```

**Example:**
- `dataset.metadata.stage_alias`: `"staged/data_products/c10895aea636715eeec70b3a129d4b17"`
- `datasetFile.path`: `"bigwig_dp/file.bw"`
- **Result:** `"staged/data_products/c10895aea636715eeec70b3a129d4b17/bigwig_dp/file.bw"`

**Function:** `buildFileExposureUrl()` in `api/src/routes/sessions.js`

```javascript
function buildFileExposureUrl(sessionId, relativePath) {
  const cleanedPath = relativePath.replace(/^\/+/, '');
  return `/api/sessions/${sessionId}/files/expose/${cleanedPath}`;
}
```

**Example:**
- `sessionId`: `9`
- `relativePath`: `"staged/data_products/hash/file.bw"`
- **Result:** `"/api/sessions/9/files/expose/staged/data_products/hash/file.bw"`

### Index File Detection

**Function:** `findIndexFileForPrimary()` in `api/src/utils/genomeBrowserUtils.js`

```javascript
function findIndexFileForPrimary(datasetFiles, primaryFile) {
  // Check if primary file has a parent (index) file
  const indexFileRelation = primaryFile.parents?.find(
    (rel) => rel.parent && isIndexFile(rel.parent)
  );

  if (indexFileRelation) return indexFileRelation.parent;

  // Fallback: Look for conventional index file (e.g., file.bam.bai)
  const primaryPath = primaryFile.path || '';
  const possibleIndexPaths = [
    `${primaryPath}.bai`,
    `${primaryPath}.tbi`,
    `${primaryPath}.csi`,
    `${primaryPath}.crai`,
  ];

  return datasetFiles.find((file) =>
    possibleIndexPaths.some((indexPath) => file.path === indexPath)
  );
}

function isIndexFile(file) {
  const indexExtensions = ['.bai', '.tbi', '.csi', '.crai'];
  return indexExtensions.some((ext) => file.path?.toLowerCase().endsWith(ext));
}
```

---

## Authentication & File Serving

### Cookie-Based Authentication

#### Why Cookies?
1. **No CORS Preflight**: `GET` requests with cookies don't trigger CORS preflight
2. **Browser Compatibility**: IGV and WashU automatically send cookies with file requests
3. **Session-Scoped**: Token limits access to files in a specific session

### Authentication Flow

#### Step 1: Set Cookie

**UI Call:**
```javascript
await sessionService.setFileCookie(session.value.id);
```

**API Endpoint:** `POST /sessions/:id/set-file-cookie`

```javascript
router.post(
  '/:id/set-file-cookie',
  isPermittedTo('read'),
  [param('id').isInt().toInt()],
  asyncHandler(async (req, res) => {
    const sessionId = req.params.id;

    // Generate JWT with session_id in payload
    const token = jwt.sign(
      {
        user_id: req.user.id,
        session_id: sessionId,
      },
      JWT_SECRET,
      { expiresIn: '1h' }
    );

    // Set HttpOnly cookie
    res.cookie('bioloop_session_token', token, {
      httpOnly: true,
      secure: process.env.NODE_ENV === 'production',
      sameSite: 'lax',
      maxAge: 3600000, // 1 hour
    });

    res.json({ success: true });
  })
);
```

#### Step 2: File Request with Cookie

**Browser Request:**
```
GET /api/sessions/9/files/expose/staged/data_products/hash/file.bw
Cookie: bioloop_session_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

**API Middleware:** `authenticateWithCookie` in `api/src/middleware/auth.js`

```javascript
const authenticateWithCookie = asyncHandler(async (req, res, next) => {
  const token = req.cookies.bioloop_session_token;

  if (!token) {
    throw createError(401, 'Authentication failed. Token not found.');
  }

  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    
    // Verify session access
    const sessionId = parseInt(req.params.id);
    if (decoded.session_id !== sessionId) {
      throw createError(403, 'Token does not match session');
    }

    // Attach user to request
    const user = await prisma.user.findUnique({
      where: { id: decoded.user_id },
    });

    if (!user) throw createError(401, 'User not found');

    req.user = user;
    next();
  } catch (error) {
    throw createError(401, 'Invalid or expired token');
  }
});
```

#### Step 3: Serve File

**API Endpoint:** `GET /sessions/:id/files/expose/*`

```javascript
fileExposureRouter.get(
  '/:id/files/expose/*',
  authenticateWithCookie,
  asyncHandler(async (req, res) => {
    const sessionId = parseInt(req.params.id);
    const requestedPath = req.params[0]; // Wildcard captured path

    logger.info('[FILE EXPOSE] Request received');

    // Verify session exists and user has access
    const session = await prisma.genome_browser_session.findUnique({
      where: { id: sessionId },
      include: {
        session_tracks: {
          include: {
            track: {
              include: {
                dataset_file: {
                  include: { dataset: true, parents: { include: { parent: true } } },
                },
              },
            },
          },
        },
      },
    });

    if (!session) throw createError(404, 'Session not found');

    logger.info('[FILE EXPOSE] Session found');

    // Check if requested file is in this session's tracks
    const fileInSession = session.session_tracks.some((st) => {
      const primaryRelativePath = getRelativeFilePath({
        dataset: st.track.dataset_file.dataset,
        datasetFile: st.track.dataset_file,
      });

      // Also check index file
      const indexFile = findIndexFileForPrimary(
        [st.track.dataset_file],
        st.track.dataset_file
      );
      const indexRelativePath = indexFile
        ? getRelativeFilePath({
            dataset: st.track.dataset_file.dataset,
            datasetFile: indexFile,
          })
        : null;

      return requestedPath === primaryRelativePath || requestedPath === indexRelativePath;
    });

    if (!fileInSession) {
      throw createError(403, 'File not in session');
    }

    logger.info('[FILE EXPOSE] File matched in session');

    // Construct absolute file path
    const absolutePath = path.join(DATA_ROOT, requestedPath);

    // Check file exists
    await fs.promises.access(absolutePath, fs.constants.R_OK);

    logger.info('[FILE EXPOSE] File path constructed');

    // Disable compression for binary files
    res.locals.compress = false;
    res.set('Content-Encoding', 'identity');
    res.removeHeader('Vary');

    // Get file stats for Content-Length
    const stat = await fs.promises.stat(absolutePath);
    res.set('Content-Length', stat.size);
    res.set('Accept-Ranges', 'bytes');

    // Handle range requests (for BAM, BigWig, etc.)
    const range = req.headers.range;
    if (range) {
      const parts = range.replace(/bytes=/, '').split('-');
      const start = parseInt(parts[0], 10);
      const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
      const chunkSize = end - start + 1;

      res.status(206); // Partial Content
      res.set('Content-Range', `bytes ${start}-${end}/${stat.size}`);
      res.set('Content-Length', chunkSize);

      const readStream = fs.createReadStream(absolutePath, { start, end });
      readStream.pipe(res);
    } else {
      // Full file
      const readStream = fs.createReadStream(absolutePath);
      readStream.pipe(res);
    }
  })
);
```

### Key File Serving Considerations

#### 1. Disable Compression
Binary genomic files (BAM, BigWig) must NOT be compressed by Express middleware:

```javascript
// In app.js
app.use(compression({
  filter: (req, res) => {
    // Exclude file exposure endpoints
    if (req.path && req.path.includes('/files/expose')) {
      return false;
    }
    return compression.filter(req, res);
  },
}));

// In file exposure route
res.locals.compress = false;
res.set('Content-Encoding', 'identity');
```

#### 2. Range Request Support
IGV and WashU use HTTP range requests to fetch specific byte ranges (for efficient streaming):

```javascript
const range = req.headers.range; // e.g., "bytes=0-1023"
if (range) {
  const parts = range.replace(/bytes=/, '').split('-');
  const start = parseInt(parts[0], 10);
  const end = parts[1] ? parseInt(parts[1], 10) : stat.size - 1;
  
  res.status(206); // Partial Content
  res.set('Content-Range', `bytes ${start}-${end}/${stat.size}`);
  
  const readStream = fs.createReadStream(absolutePath, { start, end });
  readStream.pipe(res);
}
```

#### 3. CORS Considerations
File exposure router is mounted **before** global authentication middleware to avoid CORS preflight:

```javascript
// api/src/routes/index.js
const fileExposureRouter = require('./sessions').fileExposureRouter;

// Mount file exposure router BEFORE global auth middleware
app.use('/sessions', fileExposureRouter);

// Global auth middleware
app.use(authenticate);

// Other routes
app.use('/sessions', sessionsRouter);
app.use('/tracks', tracksRouter);
```

---

## Troubleshooting

### Common Issues

#### Issue 1: "Authentication failed. Token not found."
**Cause:** Cookie not set before browser requests files  
**Solution:** Ensure `setFileCookie()` is called before initializing browser

```javascript
await sessionService.setFileCookie(session.value.id);
const datahubResponse = await sessionService.getDatahub(session.value.id, 'igv');
```

#### Issue 2: IGV error "Could not determine track type for file"
**Cause:** Wrong `type` or `format` values in IGV track config  
**Solution:** For BigWig files, use `type: "wig"`, `format: "bigwig"`

```javascript
// CORRECT
{
  type: "wig",
  format: "bigwig",
  url: "/api/sessions/9/files/expose/.../file.bw"
}

// WRONG
{
  type: "bigwig",
  format: "bigwig",
  url: "/api/sessions/9/files/expose/.../file.bw"
}
```

#### Issue 3: WashU error "Error detecting chromosome naming: Error: no stats"
**Causes:**
1. **Compression enabled**: Binary file compressed with Brotli/gzip
2. **Wrong component imported**: Using `GenomeViewer` instead of `GenomeHub`

**Solutions:**
1. Disable compression for `/files/expose` endpoints
2. Import `GenomeHub` from `wuepgg`:

```javascript
const mod = await import('wuepgg');
const GenomeHub = mod.default ?? mod.GenomeHub;
```

#### Issue 4: WashU modal closes on click
**Cause:** React-Vue event bubbling conflict  
**Solution:** Stop event propagation and disable modal attachment

```vue
<va-modal :disable-attachment="true" no-outside-dismiss>
  <div @click.stop @mousedown.stop>
    <WashUBrowser ... />
  </div>
</va-modal>
```

#### Issue 5: WashU "zombie tracks" (old tracks persist)
**Cause:** Redux persist rehydrating old state from localStorage  
**Solutions:**
1. Disable persistence:
```javascript
storeConfig: {
  storeId: `bioloop-washu-${Date.now()}`,
  enablePersistence: false,
}
```

2. Purge old keys:
```javascript
Object.keys(localStorage)
  .filter((key) => key.startsWith('persist:'))
  .forEach((key) => localStorage.removeItem(key));
```

3. Force remount with dynamic key:
```vue
<WashUBrowser :key="washuMountKey" />
```

#### Issue 6: WashU error "WorkerGlobalScope.fetch: /api/... is not a valid URL"
**Cause:** Relative URLs don't work in Web Workers  
**Solution:** Convert to absolute URLs

```javascript
const toAbsoluteUrl = (url) => {
  if (url.startsWith('http://') || url.startsWith('https://')) return url;
  return `${window.location.origin}${url}`;
};

const tracks = props.dataHub.map((t) => ({
  ...t,
  url: toAbsoluteUrl(t.url),
}));
```

#### Issue 7: File not found (ENOENT) for staged files
**Cause:** Incorrect path construction (missing `stage_alias`)  
**Solution:** Use `getRelativeFilePath()` utility

```javascript
const relativePath = getRelativeFilePath({
  dataset: st.track.dataset_file.dataset,
  datasetFile: st.track.dataset_file,
});
// Result: "staged/data_products/hash/file.bw"
```

---

## Summary

### IGV vs WashU Comparison

| Feature | IGV | WashU |
|---------|-----|-------|
| **Framework** | Pure JavaScript | React (wrapped in Vue) |
| **Installation** | `npm install igv` | `npm install wuepgg react react-dom` |
| **Mounting** | `igv.createBrowser(div, options)` | `createRoot(div).render(createElement(GenomeHub, props))` |
| **Track Type Field** | `type` + `format` | `type` only |
| **BigWig Type** | `type: "wig", format: "bigwig"` | `type: "bigwig"` |
| **Track Options** | Flat structure | Nested in `options` object |
| **URL Requirements** | Relative OK | Must be absolute |
| **State Persistence** | None | Redux persist (must disable) |
| **Cleanup** | `browser.dispose()` | `reactRoot.unmount()` + `clearAllStoreCaches()` |
| **Event Isolation** | N/A | Requires `@click.stop` |

### Best Practices

1. **Use Constants**: Always use `BROWSER_TYPES` constants, never hardcoded strings
2. **Generic Naming**: Use "Genome Browser" terminology in shared code
3. **Cookie First**: Call `setFileCookie()` before fetching datahub
4. **Disable Compression**: Binary files must not be compressed
5. **Range Support**: Implement HTTP range requests for efficiency
6. **Absolute URLs for WashU**: Convert relative URLs to absolute
7. **Disable WashU Persistence**: Prevent localStorage state rehydration
8. **Event Isolation**: Stop propagation for WashU React events
9. **Force Remount**: Use dynamic keys to ensure clean state

---

## Code Reference Quick Links

### API
- **Sessions Routes**: `api/src/routes/sessions.js`
- **Datahub Endpoint**: Line ~844 (`GET /:id/datahub`)
- **File Exposure**: Line ~1040 (`GET /:id/files/expose/*`)
- **Track Serialization**: Lines ~700-850 (`serializeTrackForIGV`, `serializeTrackForWashU`)

### UI
- **Session Detail Page**: `ui/src/pages/sessions/[id].vue`
  - `initializeIGV()`: Line ~769
  - `initializeWashU()`: Line ~834
- **Browser Selection Modal**: `ui/src/components/genomeBrowser/BrowserSelectionModal.vue`
- **WashU Wrapper**: `ui/src/components/genomeBrowser/WashUBrowser.vue`
- **Constants**: `ui/src/constants.js` (Line ~292)

### Utilities
- **Genome Browser Utils**: `api/src/utils/genomeBrowserUtils.js`
- **Session Service**: `ui/src/services/session.js`

---

**End of Implementation Guide**

