<template>
  <va-modal
    v-model="visible"
    :title="`Edit ${config.dataset.types[props.data?.type]?.label} / ${props.data?.name}`"
    no-outside-dismiss
    fixed-layout
    size="small"
    ok-text="Edit"
    @ok="handleOk"
    @cancel="hide"
  >
    <va-inner-loading :loading="loading">
      <div class="space-y-4">
        <va-textarea
          label="Description"
          v-model="description"
          class="w-full"
          :min-rows="3"
          :max-rows="10"
          resize
        >
        </va-textarea>

        <!-- Historical data notice -->
        <va-alert v-if="isHistoricalData" color="info" border="left">
          This is historical data from CMG. Some fields are read-only to
          preserve data integrity.
        </va-alert>

        <!-- Analysis Type field for DATA_PRODUCT datasets when genome browser is enabled -->
        <va-input
          v-if="showAnalysisType"
          v-model="analysisTypeInput"
          label="Analysis Type"
          placeholder="Enter analysis type (e.g., Raw Reads)"
          clearable
          class="w-full"
          :readonly="isHistoricalData"
          @blur="formatAnalysisTypeOnBlur"
        >
          <template #appendInner>
            <va-icon name="info" size="small" />
          </template>
        </va-input>
      </div>
    </va-inner-loading>
  </va-modal>
</template>

<script setup>
import config from "@/config";
import DatasetService from "@/services/dataset";
import legacyMigrationService from "@/services/legacyMigration";
import {
  formatAnalysisType,
  humanizeAnalysisType,
} from "@/services/sessionUtils";
import toast from "@/services/toast";
import { computed, ref, watch } from "vue";

const props = defineProps(["data"]);
const emit = defineEmits(["update"]);

// parent component can invoke these methods through the template ref
defineExpose({
  show,
  hide,
});

const visible = ref(false);
const loading = ref(false);
const description = ref(props.data.description);
const analysisType = ref(props.data.metadata?.analysis_type || null);
const analysisTypeInput = ref(
  humanizeAnalysisType(props.data.metadata?.analysis_type || ""),
);

// Check if this is historical CMG data
const isHistoricalData = computed(() => {
  return legacyMigrationService.isLegacyDataset(props.data);
});

// Show Analysis Type field only for DATA_PRODUCT datasets when genome browser is enabled
const showAnalysisType = computed(() => {
  return (
    props.data?.type === "DATA_PRODUCT" && config.enabledFeatures?.genomeBrowser
  );
});

// Format analysis type input when user finishes typing
const formatAnalysisTypeOnBlur = () => {
  if (analysisTypeInput.value) {
    const formatted = formatAnalysisType(analysisTypeInput.value);
    analysisType.value = formatted;
    analysisTypeInput.value = humanizeAnalysisType(formatted);
  } else {
    analysisType.value = null;
  }
};

// Watch for changes in the input to update the formatted value
watch(analysisTypeInput, (newValue) => {
  if (!newValue) {
    analysisType.value = null;
  }
});

function hide() {
  loading.value = false;
  visible.value = false;
}

function show() {
  visible.value = true;
}

function handleOk() {
  loading.value = true;

  const updateData = {
    description: description.value,
  };

  // Include analysis_type in metadata if it's a DATA_PRODUCT dataset with genome browser enabled
  if (showAnalysisType.value) {
    updateData.metadata = {
      ...props.data.metadata,
      analysis_type: analysisType.value,
    };
  }

  DatasetService.update({
    id: props.data.id,
    updated_data: updateData,
  })
    .then(() => {
      emit("update");
    })
    .catch((err) => {
      console.error(err);
      toast.error("Unable to update the dataset");
    })
    .finally(() => {
      hide();
    });
}
</script>
