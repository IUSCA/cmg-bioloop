<template>
  <div ref="washuContainer" class="washu-browser-container"></div>
</template>

<script setup>
import toast from '@/services/toast';
import { createElement } from 'react';
import { createRoot } from 'react-dom/client';
import { onBeforeUnmount, onMounted, ref } from 'vue';

const props = defineProps({
  genomeName: {
    type: String,
    required: true,
  },
  tracks: {
    type: Array,
    required: true,
  },
  viewRegion: {
    type: String,
    default: undefined, // Let WashU use genome default if not provided
  },
});

const washuContainer = ref(null);
let reactRoot = null;

onMounted(async () => {
  if (!washuContainer.value) return;

  try {
    // Import WashU GenomeViewer component
    const { GenomeViewer } = await import('wuepgg');

    // Create React root (React 18 API)
    reactRoot = createRoot(washuContainer.value);

    // Build WashU props
    const washuProps = {
      genomeName: props.genomeName,
      tracks: props.tracks,
      viewRegion: props.viewRegion || 'chr1:1-1000000',
    };

    console.log('[WashU] Initializing with props:', washuProps);

    // Create React element using createElement (no JSX needed)
    const genomeViewerElement = createElement(GenomeViewer, washuProps);

    // Render into container
    reactRoot.render(genomeViewerElement);

    console.log('[WashU] Browser initialized successfully');
  } catch (error) {
    console.error('[WashU] Failed to initialize:', error);
    toast.error('Failed to load WashU browser');
  }
});

onBeforeUnmount(() => {
  // STRICT CLEANUP: Unmount React component
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
  min-height: 600px;
}
</style>
