<template>
  <div class="va-table-responsive">
    <table class="va-table">
      <tbody>
        <tr>
          <td>Genome Type</td>
          <td>{{ props.dataset?.genomic_details?.genome_type }}</td>
        </tr>
        <tr>
          <td>Genome Value</td>
          <td>{{ props.dataset?.genomic_details?.genome_value }}</td>
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
const props = defineProps({
  dataset: Object,
});
</script>
