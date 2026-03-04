<template>
  <va-chip v-if="genomeType || genomeValue" size="small" outline>
    {{ genomeType || "" }}{{ genomeValue ? ` (${genomeValue})` : "" }}
  </va-chip>
</template>

<script setup>
defineProps({
  genomeType: {
    type: String,
    default: null,
  },
  genomeValue: {
    type: String,
    default: null,
  },
});
</script>
