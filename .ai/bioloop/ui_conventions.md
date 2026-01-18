# Bioloop Platform UI Conventions

## Vuestic Component Usage

**Correct Component Names:**
- ✅ `va-progress-circle` (NOT `va-progress-circular`)
- ✅ Individual `va-radio` components (NOT `va-radio-group`)
- ✅ `va-select` with `text-by` and `value-by` props

**va-select Pattern:**
```vue
<va-select
  v-model="form.type"
  :options="typeOptions"
  text-by="text"
  value-by="value"
  label="Type"
/>

<script setup>
const typeOptions = computed(() => {
  return Object.entries(constants.TYPES).map(([key, value]) => ({
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
    v-model="selectedOption" 
    :option="OPTIONS.A" 
    :label="LABELS[OPTIONS.A]" 
  />
  <va-radio 
    v-model="selectedOption" 
    :option="OPTIONS.B" 
    :label="LABELS[OPTIONS.B]" 
  />
</div>
```

---

## Constants Import Pattern

```vue
<script setup>
import constants from '@/constants';

// Destructure what you need
const { DATASET_TYPES } = constants;
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
const updateFields = () => {
  if (!form.field1 && !form.field2) {
    // Auto-populate only if both are empty
    const uniqueValues = getUniqueValues();
    if (uniqueValues.length === 1) {
      form.field1 = uniqueValues[0].value1;
      form.field2 = uniqueValues[0].value2;
    }
  }
};

// ❌ WRONG: Overwriting existing values
const updateFields = () => {
  const uniqueValues = getUniqueValues();
  if (uniqueValues.length === 1) {
    form.field1 = uniqueValues[0].value1; // Overwrites manual input!
    form.field2 = uniqueValues[0].value2;
  }
};
```

---

## Page vs List Component Pattern

**Pages should be minimal, delegating to list components:**

```vue
<!-- pages/items/index.vue -->
<template>
  <div>
    <h1>Items</h1>
    <ItemsList />
  </div>
</template>

<script setup>
import ItemsList from '@/components/items/ItemsList.vue';
</script>

<!-- components/items/ItemsList.vue -->
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
const saveItem = async () => {
  try {
    await itemService.create(form);
    toast.success('Item created successfully');
  } catch (error) {
    toast.error('Failed to create item');
  }
};

// ❌ WRONG
const selectItem = (item) => {
  selectedItems.push(item);
  toast.success('Item selected'); // No toast for UI interactions
};
```

---

## Form Validation Pattern

```vue
<script setup>
import { ref, computed } from 'vue';

const form = ref({
  name: '',
  type: '',
});

const isValid = computed(() => {
  return form.value.name.trim() !== '' 
    && form.value.type !== '';
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

