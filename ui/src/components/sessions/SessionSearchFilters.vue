<template>
  <div class="flex gap-2 flex-grow items-center">
    <!-- title filter -->
    <va-chip
      class="flex-none"
      closeable
      outline
      v-if="filters.title"
      @click="emit('open')"
      @update:model-value="removeFilter('title')"
    >
      Title: &nbsp;
      <span class="font-semibold"> {{ filters.title }} </span>
    </va-chip>

    <!-- genome filter -->
    <va-chip
      class="flex-none"
      closeable
      outline
      v-if="filters.genome"
      @click="emit('open')"
      @update:model-value="removeFilter('genome')"
    >
      Genome: &nbsp;
      <span class="font-semibold"> {{ filters.genome }} </span>
    </va-chip>

    <!-- genome_type filter -->
    <va-chip
      class="flex-none"
      closeable
      outline
      v-if="filters.genome_type"
      @click="emit('open')"
      @update:model-value="removeFilter('genome_type')"
    >
      Genome Type: &nbsp;
      <span class="font-semibold"> {{ filters.genome_type }} </span>
    </va-chip>

    <!-- reset search -->
    <va-button
      @click="clearAll"
      preset="secondary"
      round
      class="flex-none ml-auto"
      v-if="hasActiveFilters"
    >
      <span class="text-sm"> Reset </span>
    </va-button>
  </div>
</template>

<script setup>
import { computed } from "vue";

const props = defineProps({
  filters: {
    type: Object,
    required: true,
    default: () => ({
      title: "",
      genome: "",
      genome_type: "",
    }),
  },
});

const emit = defineEmits(["search", "open", "remove-filter", "clear-all"]);

// Computed
const hasActiveFilters = computed(() => {
  return Object.values(props.filters).some((value) => value && value !== "");
});

// Methods
function removeFilter(field) {
  emit("remove-filter", field);
}

function clearAll() {
  emit("clear-all");
}
</script>
