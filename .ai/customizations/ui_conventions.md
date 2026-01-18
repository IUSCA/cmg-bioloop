# CMG-Bioloop UI Customizations

**Extends:** `/bioloop/ui_conventions.md`

This file documents UI patterns **specific to CMG customizations**, particularly genome browser integration.

---

## React-in-Vue Integration Pattern

When embedding React components (e.g., WashU browser) in Vue:

```vue
<template>
  <div ref="reactContainer" class="h-full w-full"></div>
</template>

<script setup>
import { createRoot } from 'react-dom/client';
import { createElement } from 'react';
import { onMounted, onBeforeUnmount, ref } from 'vue';

const props = defineProps({
  someProp: { type: String, required: true },
});

const reactContainer = ref(null);
let reactRoot = null;

onMounted(async () => {
  const ReactComponent = (await import('some-react-lib')).default;
  
  reactRoot = createRoot(reactContainer.value);
  reactRoot.render(createElement(ReactComponent, props));
});

onBeforeUnmount(() => {
  if (reactRoot) {
    reactRoot.unmount();
    reactRoot = null;
  }
});
</script>
```

**Key points:**
- Use `createRoot()` from `react-dom/client` (React 18+)
- Always unmount in `onBeforeUnmount()`
- Stop event propagation with `@click.stop` on container to prevent React-Vue conflicts
- Use `:disable-attachment="true"` on parent `va-modal` if inside a modal
- Convert relative URLs to absolute for Web Workers
- Force remounts with dynamic `:key` when props change

---

## Genome Browser Constants Pattern

```vue
<script setup>
import constants from '@/constants';

// Destructure genome browser constants
const { browserTypes: BROWSER_TYPES, browserLabels: BROWSER_LABELS } = 
  constants.genomeBrowser;
  
const { GENOME_TYPES } = constants;
</script>
```

---

## Browser Selection Pattern

```vue
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
```

---

## Track Selection with Async Autocomplete

For genome browser track selection:

```vue
<template>
  <va-async-auto-complete
    v-model="selectedTracks"
    :fetch-data="fetchTracks"
    :multiple="true"
    placeholder="Search tracks..."
    :debounce="300"
  />
</template>

<script setup>
import trackService from '@/services/track';

const fetchTracks = async (inputValue) => {
  try {
    const response = await trackService.search({
      search: inputValue,
      limit: 20,
    });
    return response.data.tracks || [];
  } catch (error) {
    console.error('Failed to fetch tracks:', error);
    return [];
  }
};
</script>
```

---

## Auto-Population for Genome Fields

When auto-populating genome_type and genome fields from track selection:

```javascript
// ✅ CORRECT: Only auto-populate if fields are empty
const updateGenomeFields = () => {
  if (!form.genome_type && !form.genome) {
    // Auto-populate only if both are empty
    const uniqueGenomes = getUniqueGenomesFromTracks();
    if (uniqueGenomes.length === 1) {
      form.genome_type = uniqueGenomes[0].type;
      form.genome = uniqueGenomes[0].value;
    }
  }
};
```

---

**Last Updated:** 2026-01-16

