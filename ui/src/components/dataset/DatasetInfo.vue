<template>
  <div class="va-table-responsive">
    <table class="va-table">
      <tbody>
        <tr>
          <td>ID</td>
          <td>{{ props.dataset.id }}</td>
        </tr>
        <tr>
          <td>Start Date</td>
          <td>
            <span class="spacing-wider">
              {{ datetime.absolute(props.dataset.created_at) }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Last Updated</td>
          <td>
            <span class="spacing-wider">
              {{ datetime.absolute(props.dataset.updated_at) }}
            </span>
            <span>
              {{ datetime.fromNow(null) }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Source Instrument</td>
          <td>
            {{ props.dataset.src_instrument?.name }}
          </td>
        </tr>
        <tr>
          <td>Source Path</td>
          <td>
            <span>{{ props.dataset.origin_path }}</span>
          </td>
        </tr>
        <tr>
          <td>Size</td>
          <td>
            <span v-if="props.dataset.du_size">
              {{ formatBytes(props.dataset.du_size) }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Files</td>
          <td>{{ props.dataset.num_files }}</td>
        </tr>
        <tr>
          <td>Directories</td>
          <td>{{ props.dataset.num_directories }}</td>
        </tr>
        <!--        <tr>-->
        <!--          <td>Created By</td>-->
        <!--          <td>-->
        <!--            {{ datasetCreatorDisplayed }}-->
        <!--          </td>-->
        <!--        </tr>-->
        <tr v-if="showAnalysisType">
          <td>Analysis Type</td>
          <td>
            <va-chip v-if="formattedAnalysisType" size="small" outline>
              {{ formattedAnalysisType }}
            </va-chip>
          </td>
        </tr>
        <tr>
          <td>Description</td>
          <td>
            <div class="max-h-[11.5rem] overflow-y-scroll">
              {{ props.dataset.description }}
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script setup>
import config from "@/config";
import * as datetime from "@/services/datetime";
import { humanizeAnalysisType } from "@/services/sessionUtils";
import { formatBytes } from "@/services/utils";
import { computed } from "vue";

const props = defineProps({ dataset: Object });

// Show Analysis Type only for DATA_PRODUCT datasets when genome browser is enabled
const showAnalysisType = computed(() => {
  return (
    props.dataset?.type === "DATA_PRODUCT" &&
    config.enabledFeatures?.genomeBrowser
  );
});

// Format analysis type with extension
const formattedAnalysisType = computed(() => {
  const analysisType = props.dataset?.analysis_type;
  if (!analysisType?.name) {
    return "";
  }
  const humanized = humanizeAnalysisType(analysisType.name);
  return analysisType.extension
    ? `${humanized} (${analysisType.extension})`
    : humanized;
});

// const datasetCreateLog = computed(() => {
//   return (props.dataset?.audit_logs || []).find((e) => !!e.create_method);
// });

// const datasetCreatorDisplayed = computed(() => {
//   const datasetCreator = datasetCreateLog.value?.user;
//   return datasetCreator
//     ? `${datasetCreator.username} (${datasetCreator.name})`
//     : null;
// });
</script>

<style lang="scss" scoped>
div.va-table-responsive {
  overflow: auto;

  // first column min width
  td:first-child {
    min-width: 135px;
  }
}
</style>
