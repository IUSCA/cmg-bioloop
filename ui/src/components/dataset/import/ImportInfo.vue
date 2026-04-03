<template>
  <va-card data-testid="import-info-card">
    <va-card-title>Import Details</va-card-title>
    <va-card-content>
      <div class="va-table-responsive">
        <table class="va-table w-full">
          <tbody>
            <tr>
              <td>Dataset Name</td>
              <td>
                <div v-if="props.dataset">
                  <div v-if="!auth.canOperate">
                    {{ props.dataset.name }}
                  </div>
                  <a
                    v-else
                    :href="`/datasets/${props.dataset.id}`"
                    data-testid="import-success-dataset-link"
                  >
                    {{ props.dataset.name }}
                  </a>
                </div>
                <DatasetNameInput
                  v-else
                  v-model:populated-dataset-name="datasetNameInput"
                  class="w-full"
                  :input-disabled="props.inputDisabled"
                  :error="props.datasetNameError"
                />
              </td>
            </tr>

            <tr>
              <td>Dataset Type</td>
              <td>
                <va-chip size="small" outline>
                  {{ props.datasetType }}
                </va-chip>
              </td>
            </tr>

            <tr v-if="props.fileType">
              <td>File Type</td>
              <td>
                <va-chip size="small" outline>
                  {{ formatFileType(props.fileType) }}
                </va-chip>
              </td>
            </tr>

            <tr v-if="props.genomeType || props.genomeValue">
              <td>Genome</td>
              <td>
                <va-chip size="small" outline>
                  {{ formatGenome(props.genomeType, props.genomeValue) }}
                </va-chip>
              </td>
            </tr>

            <tr>
              <td>Source Raw Data</td>
              <td class="metadata">
                <router-link
                  :to="`/datasets/${props.sourceRawData?.id}`"
                  target="_blank"
                >
                  {{ props.sourceRawData?.name }}
                </router-link>
              </td>
            </tr>

            <tr v-if="props.sourceDataProduct">
              <td>Source Data Product</td>
              <td class="metadata">
                <router-link
                  :to="`/datasets/${props.sourceDataProduct?.id}`"
                  target="_blank"
                >
                  {{ props.sourceDataProduct?.name }}
                </router-link>
              </td>
            </tr>

            <tr>
              <td>Project</td>
              <td class="metadata">
                <div v-if="props.project">
                  <router-link
                    :to="`/projects/${props.project.id}`"
                    target="_blank"
                  >
                    {{ props.project.name }}
                  </router-link>
                </div>
                <OutlinedAlert
                  v-else-if="props.creatingNewProject"
                  color="secondary"
                  border="left"
                  size="medium"
                  padding-direction="left"
                  padding-amount="sm"
                  icon="info"
                >
                  A new Project will be created
                </OutlinedAlert>
              </td>
            </tr>

            <tr>
              <td>Source Instrument</td>
              <td class="metadata">
                {{ props.sourceInstrument?.name }}
              </td>
            </tr>

            <tr>
              <td>Source Directory</td>
              <td>
                <CopyText :text="props.importDir?.path" />
              </td>
            </tr>

            <tr>
              <td>Imported from</td>
              <td>
                <va-chip size="small" outline>
                  {{ props.importSpace }}
                </va-chip>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </va-card-content>
  </va-card>
</template>

<script setup>
import OutlinedAlert from "@/components/utils/OutlinedAlert.vue";
import { useAuthStore } from "@/stores/auth";
import { formatGenome } from "@/services/sessionUtils";

const props = defineProps({
  // `dataset`: Dataset to be created
  dataset: {
    type: Object,
  },
  populatedDatasetName: {
    type: String,
    default: "",
  },
  creatingNewProject: {
    type: Boolean,
    default: false,
  },
  importSpace: {
    type: String,
  },
  importDir: {
    type: Object,
    required: true,
  },
  datasetType: {
    type: String,
    required: true,
  },
  fileType: {
    type: Object,
  },
  genomeType: {
    type: String,
  },
  genomeValue: {
    type: String,
  },
  sourceInstrument: {
    type: Object,
  },
  sourceRawData: {
    type: Object,
  },
  sourceDataProduct: {
    type: Object,
  },
  project: {
    type: Object,
  },
  datasetNameError: {
    type: String,
    default: "",
  },
});

const formatFileType = (fileType) => {
  if (!fileType) return "";
  // fileType is an object with name and extension
  return fileType.extension
    ? `${fileType.name} (${fileType.extension})`
    : fileType.name;
};

const emit = defineEmits(["update:populatedDatasetName"]);

const auth = useAuthStore();

const datasetNameInput = computed({
  get() {
    return props.populatedDatasetName;
  },
  set(value) {
    emit("update:populatedDatasetName", value);
  },
});
</script>

<style scoped>
.metadata {
  white-space: pre-wrap;
  word-wrap: break-word;
  word-break: break-word;
}
</style>
