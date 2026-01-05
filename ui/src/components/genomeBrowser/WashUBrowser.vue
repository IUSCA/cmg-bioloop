<template>
  <div ref="washuContainer" class="washu-browser-container"></div>
</template>

<script setup>
import toast from '@/services/toast';
// import '@/wuepgg/style.css';
import stableStringify from 'json-stable-stringify';
import { createElement } from 'react';
import { createRoot } from 'react-dom/client';
import { onBeforeUnmount, onMounted, ref } from 'vue';

const props = defineProps({
  genomeName: {
    type: String,
    required: true,
  },
  dataHub: {
    type: Array,
    required: true,
  },
  viewRegion: {
    type: String,
    default: undefined,
  },
});

const washuContainer = ref(null);
let reactRoot = null;
let clearAllStoreCaches = null;

onMounted(async () => {
  // 🧹 NUCLEAR: Purge any old WashU persisted state (one-time cleanup for zombie tracks)
  try {
    const persistKeys = Object.keys(localStorage).filter((key) => key.startsWith('persist:'));
    if (persistKeys.length > 0) {
      console.log('[WashU] Clearing old persisted state:', persistKeys);
      persistKeys.forEach((key) => localStorage.removeItem(key));
    }
  } catch (e) {
    console.warn('[WashU] Failed to clear persisted state:', e);
  }

  if (!washuContainer.value) return;

  try {
    // Import WashU GenomeViewer component (defensive import for both default and named exports)
    const mod = await import('wuepgg');
    console.log('wuepgg keys:', Object.keys(mod));

    const GenomeHub = mod.default ?? mod.GenomeHub;
    clearAllStoreCaches = mod.clearAllStoreCaches ?? null;
    console.log('GenomeHub is:', GenomeHub);
    console.log('clearAllStoreCaches is:', clearAllStoreCaches);

    // Create React root (React 18 API)
    reactRoot = createRoot(washuContainer.value);

    // Build WashU props - using the correct prop names from the library
    // HARDCODED TEST: Use a remote public track to verify WashU works
    // const dataHubPlain = [
    //   {
    //     type: 'bigwig',
    //     url: 'https://vizhub.wustl.edu/public/tmp/TW463_20-5-bonemarrow_MeDIP.bigWig',
    //     name: 'TEST_REMOTE',
    //     showOnHubLoad: true,
    //     options: { height: 100 },
    //   },
    // ];

    // TODO: Uncomment this to use actual tracks from props
    const dataHubPlain = JSON.parse(JSON.stringify(props.dataHub));

    console.log('[WashU] DataHub plain:');
    console.dir(dataHubPlain, { depth: null });

    // Generate unique store ID to prevent state rehydration from localStorage
    const uniqueStoreId = `bioloop-washu-${Date.now()}`;

    const washuProps = {
      genomeName: props.genomeName, // genome assembly like "hg19", "hg38", etc.
      tracks: dataHubPlain.map((t) => ({
        ...t,
        url: toAbsoluteUrl(t.url), // WashU uses Web Workers which require absolute URLs
      })), // array of track objects, unwrapped from Vue proxy
      viewRegion: props.viewRegion ?? 'chr1:155000000-155050000', // no commas

      // Package mode props - render UI components
      showToolBar: true,
      showNavBar: true,
      showGenomeNavigator: false, // don't show genome picker sidebar

      // IMPORTANT: Disable Redux persistence to prevent zombie tracks from localStorage
      storeConfig: {
        storeId: uniqueStoreId, // Unique ID prevents rehydration of old state
        enablePersistence: false, // No localStorage reads/writes
      },
    };

    console.log('[WashU] Initializing with props:');
    console.dir(washuProps, { depth: null });

    // Force fresh store by using a unique key that changes whenever tracks change
    // This prevents WashU's global store manager from keeping old tracks cached
    const key = `washu:${props.genomeName}:${stableStringify(washuProps.tracks)}`;
    console.log('[WashU] Using React key:', key);
    console.log('[WashU] Using unique storeId:', uniqueStoreId);

    // Create React element using createElement
    const genomeViewerElement = createElement(GenomeHub, {
      ...washuProps,
      key, // forces fresh mount
    });

    // Render into container
    reactRoot.render(genomeViewerElement);

    console.log('[WashU] Browser initialized successfully');
  } catch (error) {
    console.error('[WashU] Failed to initialize:', error);
    toast.error('Failed to load WashU browser');
  }
});

function toAbsoluteUrl(u) {
  try {
    // already absolute
    return new URL(u).toString();
  } catch {
    // relative -> absolute (works in workers)
    return new URL(u, window.location.origin).toString();
  }
}

onBeforeUnmount(() => {
  // CLEANUP: Clear WashU global store caches to prevent zombie tracks
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

<style scoped>
.washu-browser-container {
  width: 100%;
  height: 100%;
  /* min-height: 600px; */
}
</style>
