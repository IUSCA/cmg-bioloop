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
    </div>
  </va-modal>
</template>

<script setup>
import constants from "@/constants";
import { ref, watch } from "vue";

const {
  browserTypes: BROWSER_TYPES,
  browserLabels: BROWSER_LABELS,
  defaultBrowser: DEFAULT_BROWSER,
} = constants.genomeBrowser;

const emit = defineEmits(["browser-selected", "close"]);

const showModal = defineModel({ type: Boolean, default: false });
const selectedBrowser = ref(DEFAULT_BROWSER);

const openBrowser = () => {
  emit("browser-selected", selectedBrowser.value);
  showModal.value = false;
};

const closeModal = () => {
  showModal.value = false;
  emit("close");
};

// Reset selection to default when modal is closed
watch(showModal, (isOpen) => {
  if (!isOpen) {
    selectedBrowser.value = DEFAULT_BROWSER;
  }
});
</script>
