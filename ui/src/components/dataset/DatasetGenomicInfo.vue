<template>
  <div class="va-table-responsive">
    <table class="va-table">
      <tbody>
        <tr v-if="formattedGenome">
          <td>Genome</td>
          <td>
            <va-chip size="small" outline>
              {{ formattedGenome }}
            </va-chip>
          </td>
        </tr>
        <tr>
          <td>Genome Files</td>
          <td>{{ props.dataset?.metadata?.num_genome_files }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import { computed } from 'vue';

const props = defineProps({
  dataset: Object,
});

// Format genome type and value
const formattedGenome = computed(() => {
  const genomicDetails = props.dataset?.genomic_details;
  if (!genomicDetails) return '';
  
  const type = genomicDetails.genome_type || '';
  const value = genomicDetails.genome_value || '';
  
  if (!type && !value) return '';
  if (!value) return type;
  if (!type) return value;
  
  return `${type} (${value})`;
});
</script>
