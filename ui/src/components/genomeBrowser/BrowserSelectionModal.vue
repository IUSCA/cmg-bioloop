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
        <va-radio v-model="selectedBrowser" option="igv" label="IGV Browser" />
        <va-radio v-model="selectedBrowser" option="washu" label="WashU Epigenome Browser" />
      </div>
    </div>
  </va-modal>
</template>

<script setup>
import { ref } from 'vue';

const emit = defineEmits(['browser-selected', 'close']);

const showModal = defineModel({ type: Boolean, default: false });
const selectedBrowser = ref('igv'); // Default to IGV

const openBrowser = () => {
  emit('browser-selected', selectedBrowser.value);
  showModal.value = false;
};

const closeModal = () => {
  showModal.value = false;
  emit('close');
};
</script>
