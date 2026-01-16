# UI Development Conventions

## Vuestic Component Usage

**Correct Component Names:**
- ✅ `va-progress-circle` (NOT `va-progress-circular`)
- ✅ Individual `va-radio` components (NOT `va-radio-group`)
- ✅ `va-select` with `text-by` and `value-by` props

**va-select Pattern:**
```vue
<va-select
  v-model="form.genome_type"
  :options="genomeTypeOptions"
  text-by="text"
  value-by="value"
  label="Genome Type"
/>

<script setup>
const genomeTypeOptions = computed(() => {
  return Object.entries(constants.GENOME_TYPES).map(([key, value]) => ({
    text: value.label,
    value: key,
  }));
});
</script>
```

**va-radio Pattern:**
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

## Constants Import Pattern

```vue
<script setup>
import constants from '@/constants';

// Destructure what you need
const { browserTypes: BROWSER_TYPES, browserLabels: BROWSER_LABELS } = 
  constants.genomeBrowser;
  
const { GENOME_TYPES } = constants;
</script>
```

---

## Async Autocomplete Pattern

For searchable dropdowns with API data:

```vue
<template>
  <va-async-auto-complete
    v-model="selectedItems"
    :fetch-data="fetchItems"
    :multiple="true"
    placeholder="Search..."
    :debounce="300"
  />
</template>

<script setup>
import itemService from '@/services/item';

const fetchItems = async (inputValue) => {
  try {
    const response = await itemService.search({
      search: inputValue,
      limit: 20,
    });
    return response.data.items || [];
  } catch (error) {
    console.error('Failed to fetch items:', error);
    return [];
  }
};
</script>
```

---

## CSS & Styling Preferences

**DO NOT add styling classes unless explicitly asked:**
- Avoid: `text-sm`, `bg-gray-100`, `text-red-500`, etc.
- When styling is needed, refer to existing components for patterns
- Use Vuestic's built-in styling props when available

**DO use Vuestic component props for styling:**
```vue
<va-button color="primary" size="small" />
<va-card stripe color="success" />
```

---

## Auto-Population Logic

When implementing auto-population (e.g., filling form fields based on selections):

**RULE: Preserve manual user selections**

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

// ❌ WRONG: Overwriting existing values
const updateGenomeFields = () => {
  const uniqueGenomes = getUniqueGenomesFromTracks();
  if (uniqueGenomes.length === 1) {
    form.genome_type = uniqueGenomes[0].type; // Overwrites manual input!
    form.genome = uniqueGenomes[0].value;
  }
};
```

---

## Page vs List Component Pattern

**Pages should be minimal, delegating to list components:**

```vue
<!-- pages/sessions/index.vue -->
<template>
  <div>
    <h1>Sessions</h1>
    <SessionsList />
  </div>
</template>

<script setup>
import SessionsList from '@/components/sessions/SessionsList.vue';
</script>

<!-- components/sessions/SessionsList.vue -->
<template>
  <div>
    <!-- All logic, filters, tables here -->
  </div>
</template>

<script setup>
// Actual implementation
</script>
```

---

## Action Buttons vs Icons

**Prefer icons over buttons for actions:**

```vue
<!-- ✅ PREFERRED -->
<va-icon 
  name="mdi-delete" 
  @click="deleteItem(item.id)"
  class="cursor-pointer text-red-500"
/>

<!-- ❌ AVOID (unless explicitly needed) -->
<va-button @click="deleteItem(item.id)">
  Delete
</va-button>
```

---

## Toast Notifications

**Show toasts ONLY for API success/failure, not for UI interactions:**

```javascript
// ✅ CORRECT
const saveSession = async () => {
  try {
    await sessionService.create(form);
    toast.success('Session created successfully');
  } catch (error) {
    toast.error('Failed to create session');
  }
};

// ❌ WRONG
const selectTrack = (track) => {
  selectedTracks.push(track);
  toast.success('Track selected'); // No toast for UI interactions
};
```

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

## Form Validation Pattern

```vue
<script setup>
import { ref, computed } from 'vue';

const form = ref({
  name: '',
  genome_type: '',
  genome: '',
});

const isValid = computed(() => {
  return form.value.name.trim() !== '' 
    && form.value.genome_type !== ''
    && form.value.genome !== '';
});

const submit = async () => {
  if (!isValid.value) {
    toast.error('Please fill in all required fields');
    return;
  }
  
  try {
    await api.create(form.value);
    toast.success('Created successfully');
  } catch (error) {
    toast.error('Failed to create');
  }
};
</script>
```

---

## Modal Pattern

```vue
<template>
  <va-modal
    v-model="showModal"
    title="Modal Title"
    ok-text="Save"
    @ok="handleSave"
    @cancel="handleCancel"
  >
    <!-- Content -->
  </va-modal>
</template>

<script setup>
import { ref, watch } from 'vue';

const showModal = ref(false);

const handleSave = () => {
  // Save logic
  showModal.value = false;
};

const handleCancel = () => {
  // Reset form
  showModal.value = false;
};

// Reset form when modal closes
watch(showModal, (isOpen) => {
  if (!isOpen) {
    // Reset state
  }
});
</script>
```

---

## Quick Reference Checklist

### Starting New UI Component
- [ ] Import constants from `@/constants`
- [ ] Use correct Vuestic component names
- [ ] Add `text-by` and `value-by` to `va-select`
- [ ] Preserve manual user input in auto-population logic
- [ ] Use icons instead of buttons for actions
- [ ] Show toasts only for API operations

---

**Last Updated:** 2026-01-16

