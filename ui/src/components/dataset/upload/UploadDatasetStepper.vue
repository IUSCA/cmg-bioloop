<template>
  <va-inner-loading :loading="loading" class="h-full">
    <va-stepper
      v-model="step"
      :steps="steps"
      controlsHidden
      class="h-full create-data-product-stepper"
    >
      <!-- Step icons and labels -->
      <template
        v-for="(s, i) in steps"
        :key="s.label"
        #[`step-button-${i}`]="{ setStep, isActive, isCompleted }"
      >
        <va-button
          class="step-button p-1 sm:p-3 cursor-pointer"
          :class="{
            'step-button--active': isActive,
            'step-button--completed': isCompleted,
          }"
          @click="setStep(i)"
          :disabled="isStepperButtonDisabled(i)"
          preset="secondary"
        >
          <div class="flex flex-col items-center">
            <Icon :icon="s.icon" />
            <span class="hidden sm:block"> {{ s.label }} </span>
          </div>
        </va-button>
      </template>

      <template #step-content-0>
        <div class="flex flex-col">
          <div class="flex w-full pb-6">
            <va-select
              v-model="selectedFileType"
              :options="fileTypeOptions"
              label="File Type"
              placeholder="Select file type"
              class="flex-grow"
              :text-by="'text'"
              :value-by="'value'"
              clearable
            />
            <div class="flex items-end ml-2">
              <va-popover message="Create new File Type">
                <va-button
                  icon="add"
                  class="px-3"
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                  outline
                  @click="openCreateFileTypeModal"
                />
              </va-popover>
            </div>
          </div>

          <va-divider class="mb-6" />
          
          <SelectFileButtons
            :disabled="submitAttempted || loading || validatingForm"
            @files-added="onFilesAdded"
            @directory-added="onDirectoryAdded"
          />
          <va-divider />
          <SelectedFilesTable
            @file-removed="removeFile"
            :files="displayedFilesToUpload"
          />
        </div>
      </template>

      <template #step-content-1>
        <div class="flex w-full pb-6 items-center">
          <va-select
            v-model="selectedDatasetType"
            :text-by="'label'"
            :track-by="'value'"
            :options="datasetTypeOptions"
            label="Dataset Type"
            placeholder="Select dataset type"
            class="flex-grow"
          />
          <div class="flex items-center ml-2">
            <va-popover>
              <template #body>
                <div class="w-96">
                  Raw Data: Original, unprocessed data collected from
                  instruments.
                  <br />
                  Data Product: Processed data derived from Raw Data
                </div>
              </template>
              <Icon icon="mdi:information" class="text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>

        <div class="flex w-full pb-6">
          <div class="w-60 flex flex-shrink-0 mr-4">
            <div class="flex items-center">
              <va-checkbox
                v-model="isAssignedSourceRawData"
                @update:modelValue="resetRawDataSearch"
                :disabled="willUploadRawData"
                color="primary"
                label="Assign source Raw Data"
                class="flex-grow"
              />
            </div>
          </div>

          <div class="flex-grow flex items-center">
            <DatasetSelectAutoComplete
              v-model:selected="selectedRawData"
              v-model:search-term="datasetSearchText"
              :disabled="submitAttempted || !isAssignedSourceRawData"
              :dataset-type="config.dataset.types.RAW_DATA.key"
              placeholder="Search Raw Data"
              @clear="resetRawDataSearch"
              @open="onRawDataSearchOpen"
              @close="onRawDataSearchClose"
              class="flex-grow"
              :label="'Dataset'"
            >
            </DatasetSelectAutoComplete>
            <va-popover>
              <template #body>
                <div class="w-96">
                  Associating a Data Product with a source Raw Data establishes
                  a clear lineage between the original data and its processed
                  form. This linkage helps to trace the origins of processed
                  data
                </div>
              </template>
              <Icon icon="mdi:information" class="ml-2 text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>

        <div class="flex w-full pb-6">
          <div class="w-60 flex flex-shrink-0 mr-4">
            <div class="flex items-center">
              <va-checkbox
                v-model="isAssignedProject"
                @update:modelValue="
                  (val) => {
                    if (!val) {
                      projectSelected = null;
                    }
                  }
                "
                color="primary"
                label="Assign Project"
                class="flex-grow"
              />
            </div>
          </div>

          <div class="flex-grow flex items-center">
            <ProjectAsyncAutoComplete
              v-model:selected="projectSelected"
              v-model:search-term="projectSearchText"
              :disabled="submitAttempted || !isAssignedProject"
              placeholder="Search Projects"
              @clear="resetProjectSearch"
              @open="onProjectSearchOpen"
              @close="onProjectSearchClose"
              class="flex-grow"
              :label="'Project'"
            >
            </ProjectAsyncAutoComplete>
            <va-popover>
              <template #body>
                <div class="w-96">
                  Assigning a dataset to a project establishes a connection
                  between your data and a specific research initiatives. This
                  association helps organize and categorize datasets within the
                  context of your research projects, facilitating easier data
                  management, access control, and collaboration among team
                  members working on the same project.
                </div>
              </template>
              <Icon icon="mdi:information" class="ml-2 text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>

        <div class="flex w-full pb-6">
          <div class="w-60 flex flex-shrink-0 mr-4">
            <div class="flex items-center">
              <va-checkbox
                v-model="isAssignedSourceInstrument"
                @update:modelValue="
                  (val) => {
                    if (!val) {
                      selectedSourceInstrument = null;
                    }
                  }
                "
                color="primary"
                label="Assign source Instrument"
                class="flex-grow"
              />
            </div>
          </div>

          <div class="flex-grow flex items-center">
            <va-select
              v-model="selectedSourceInstrument"
              :options="sourceInstrumentOptions"
              :disabled="!isAssignedSourceInstrument"
              label="Source Instrument"
              placeholder="Select Source Instrument"
              class="flex-grow"
              :text-by="'name'"
              :track-by="'id'"
            />
            <div class="flex items-center ml-2">
              <va-popover>
                <template #body>
                  <div class="w-72">
                    Source instrument where this data was collected from.
                  </div>
                </template>
                <Icon icon="mdi:information" class="text-xl text-gray-500" />
              </va-popover>
            </div>
          </div>
        </div>

        <div v-if="shouldShowSourceDataProductField" class="flex w-full pb-6">
          <div class="w-60 flex flex-shrink-0 mr-4">
            <div class="flex items-center">
              <va-checkbox
                v-model="isAssignedSourceDataProduct"
                @update:modelValue="resetSourceDataProductSearch"
                :disabled="submitAttempted"
                color="primary"
                label="Assign source Data Product"
                class="flex-grow"
              />
            </div>
          </div>

          <div class="flex-grow flex items-center">
            <DatasetSelectAutoComplete
              v-model:selected="selectedSourceDataProduct"
              v-model:search-term="sourceDataProductSearchText"
              :disabled="submitAttempted || !isAssignedSourceDataProduct"
              :dataset-type="config.dataset.types.DATA_PRODUCT.key"
              placeholder="Search Data Product"
              @clear="resetSourceDataProductSearch"
              @open="onSourceDataProductSearchOpen"
              @close="onSourceDataProductSearchClose"
              class="flex-grow"
              :label="'Source Data Product'"
            >
            </DatasetSelectAutoComplete>
            <va-popover>
              <template #body>
                <div class="w-96">
                  Associating a Data Product with a source Data Product establishes a clear lineage
                  between derived datasets. This helps track data provenance and processing history.
                </div>
              </template>
              <Icon icon="mdi:information" class="ml-2 text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>
      </template>

      <template #step-content-2>
        <div class="flex w-full pb-6">
          <va-select
            v-model="selectedGenomeType"
            :options="genomeTypeOptions"
            :text-by="'text'"
            :track-by="'value'"
            label="Genome Type (Optional)"
            placeholder="Select genome type"
            class="flex-grow mr-2"
            clearable
          />
          <div class="flex items-center ml-2">
            <va-popover>
              <template #body>
                <div class="w-96">Organism type (e.g., Human, Mouse, etc.)</div>
              </template>
              <Icon icon="mdi:information" class="text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>

        <div class="flex w-full pb-6" v-if="selectedGenomeType">
          <va-select
            v-model="selectedGenomeValue"
            :options="availableGenomeValues"
            label="Genome Assembly (Optional)"
            placeholder="Select genome assembly"
            class="flex-grow mr-2"
            clearable
          />
          <div class="flex items-center ml-2">
            <va-popover>
              <template #body>
                <div class="w-96">Specific genome assembly version (e.g., hg38, mm10, etc.)</div>
              </template>
              <Icon icon="mdi:information" class="text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>
      </template>

      <template #step-content-3>
        <!-- Always show two cards: Left (metadata with dataset name) and Right (file list with upload progress) -->
        <div class="flex flex-row" v-if="selectingFiles || selectingDirectory">
          <!-- LEFT CARD: Dataset Metadata -->
          <div class="flex-1">
            <va-card class="upload-details">
              <va-card-title>
                <div class="flex flex-nowrap items-center w-full">
                  <span class="text-lg">Details</span>
                </div>
              </va-card-title>
              <va-card-content>
                <UploadedDatasetDetails
                  v-if="selectingFiles || selectingDirectory"
                  v-model:populated-dataset-name="populatedDatasetName"
                  :dataset="datasetUploadLog?.dataset"
                  :selected-dataset-type="selectedDatasetType.value"
                  :file-type="selectedFileType"
                  :genome-type="selectedGenomeType?.value || selectedGenomeType"
                  :genome-value="selectedGenomeValue"
                  :input-disabled="submitAttempted"
                  :uploaded-dataset-error="formErrors[STEP_KEYS.UPLOAD]"
                  :show-uploaded-dataset-error="
                    !!formErrors[STEP_KEYS.UPLOAD] && !stepIsPristine
                  "
                  :project="projectSelected"
                  :source-instrument="selectedSourceInstrument"
                  :source-raw-data="selectedRawData"
                  :source-data-product="selectedSourceDataProduct"
                  :submission-status="submissionStatus"
                  :submission-alert="submissionAlert"
                  :status-chip-color="statusChipColor"
                  :submission-alert-color="submissionAlertColor"
                  :is-submission-alert-visible="isSubmissionAlertVisible"
                />
              </va-card-content>
            </va-card>
          </div>

          <va-divider vertical />
          
          <!-- RIGHT CARD: File List and Upload Progress -->
          <div class="flex-1">
            <va-card>
              <va-card-title>{{ isUploadComplete ? 'Files Uploaded' : 'Files to Upload' }}</va-card-title>
              <va-card-content>
                <!-- Upload progress (always shown, above file list) -->
                <div class="mb-4 pb-4 border-b border-gray-300">
                  <!-- Checksum computation progress -->
                  <div v-if="isComputingChecksum" class="mb-3">
                    <div class="text-sm font-semibold mb-2">
                      Computing checksum: {{ checksumProgress }}%
                    </div>
                    <va-progress-bar :model-value="checksumProgress" color="info" />
                  </div>
                  
                  <!-- Overall upload progress -->
                  <div>
                    <div class="text-sm font-semibold mb-2">
                      Upload Progress: {{ submitAttempted ? `${filesUploaded} / ${totalFiles} files (${uploadProgress}%)` : 'Not started' }}
                    </div>
                    <va-progress-bar 
                      :model-value="submitAttempted ? uploadProgress : 0" 
                      :color="submitAttempted ? 'primary' : 'secondary'"
                    />
                  </div>
                </div>
                
                <!-- File list -->
                <div class="file-list" style="max-height: 400px; overflow-y: auto">
                  <div v-for="file in displayedFilesToUpload" :key="file.name" class="mb-2 pb-2 border-b border-gray-200 last:border-b-0">
                    <div class="flex items-center justify-between">
                      <span class="truncate flex-grow mr-2">{{ file.name }}</span>
                      <span class="text-sm text-gray-500 whitespace-nowrap">{{ file.formattedSize }}</span>
                    </div>
                  </div>
                </div>
              </va-card-content>
            </va-card>
          </div>
        </div>
      </template>

      <!-- custom controls -->
      <template #controls="{ nextStep, prevStep }">
        <div class="flex items-center justify-around w-full">
          <va-button
            class="flex-none"
            preset="primary"
            @click="
              () => {
                isSubmissionAlertVisible = false;
                prevStep();
              }
            "
            :disabled="isPreviousButtonDisabled"
          >
            Previous
          </va-button>
          <va-button
            v-if="uploadRegistrationFailed"
            class="flex-none"
            @click="retryApiCall"
            color="warning"
          >
            Retry
          </va-button>
          <va-button
            v-else
            class="flex-none"
            @click="onNextClick(nextStep)"
            :color="isLastStep ? 'success' : 'primary'"
            :disabled="isNextButtonDisabled"
          >
            {{ isLastStep ? "Upload" : "Next" }}
          </va-button>
        </div>
      </template>
    </va-stepper>
  </va-inner-loading>

  <!-- Create File Type Modal -->
  <va-modal
    v-model="showCreateFileTypeModal"
    title="Create New File Type"
    size="small"
    @ok="handleCreateFileType"
    @cancel="handleCancelFileType"
    :ok-button-props="{ disabled: isFileTypeFormInvalid }"
  >
      <div class="flex flex-col gap-4">
        <va-input
          v-model="newFileTypeName"
          label="Name"
          placeholder="e.g., FASTQ"
          :rules="[(value) => !!value || 'Name is required']"
        />
        <va-input
          v-model="newFileTypeExtension"
          label="Extension"
          placeholder="e.g., .fastq.gz"
          :rules="[
            (value) => !!value || 'Extension is required',
            (value) => !checkDuplicateFileType(newFileTypeName, value) || 'This file type already exists'
          ]"
        />
      </div>
  </va-modal>
</template>

<script setup>
import DatasetSelectAutoComplete from "@/components/dataset/DatasetSelectAutoComplete.vue";
import config from "@/config";
import Constants from "@/constants";
import analysisTypeService from "@/services/analysisType";
import datasetService from "@/services/dataset";
import instrumentService from "@/services/instrument";
import toast from "@/services/toast";
import { _getUploadServiceURL } from "@/services/upload";
import { formatBytes } from "@/services/utils";
import { useAuthStore } from "@/stores/auth";
import { Icon } from "@iconify/vue";
import _ from "lodash";
import * as tus from "tus-js-client";
import { VaDivider, VaPopover } from "vuestic-ui";
import {
  _computeManifestHash,
  _isChecksumVerificationEnabled,
} from "@/services/upload/checksum";

const auth = useAuthStore();

const STEP_KEYS = {
  SELECT_FILES: "selectFiles",
  GENERAL_INFO: "generalInfo",
  GENOMIC_DETAILS: "genomicDetails",
  UPLOAD: "upload",
};

const UNKNOWN_VALIDATION_ERROR = "An unknown error occurred";
const DATASET_NAME_REQUIRED_ERROR = "Dataset name cannot be empty";
const DATASET_NAME_HAS_SPACES_ERROR = "Dataset name cannot contain spaces";
const DATASET_NAME_MIN_LENGTH_ERROR =
  "Dataset name must have 3 or more characters.";

const steps = [
  {
    key: STEP_KEYS.SELECT_FILES,
    label: "Select Files",
    icon: "material-symbols:folder",
  },
  {
    key: STEP_KEYS.GENERAL_INFO,
    label: "General Info",
    icon: "material-symbols:info",
  },
  {
    key: STEP_KEYS.GENOMIC_DETAILS,
    label: "Genomic Details",
    icon: "mdi-dna",
  },
  {
    key: STEP_KEYS.UPLOAD,
    label: "Upload",
    icon: "material-symbols:play-circle",
  },
];

const datasetTypes = [
  {
    label: config.dataset.types.RAW_DATA.label,
    value: config.dataset.types.RAW_DATA.key,
  },
  {
    label: config.dataset.types.DATA_PRODUCT.label,
    value: config.dataset.types.DATA_PRODUCT.key,
  },
];

const FILE_TYPE = {
  FILE: "file",
  DIRECTORY: "directory",
};

const formErrors = ref({
  [STEP_KEYS.SELECT_FILES]: null,
  [STEP_KEYS.GENERAL_INFO]: null,
  [STEP_KEYS.GENOMIC_DETAILS]: null,
  [STEP_KEYS.UPLOAD]: null,
});
const isAssignedSourceInstrument = ref(true);
const isAssignedSourceRawData = ref(true);
const selectedRawData = ref(null);
const datasetSearchText = ref("");
const projectSearchText = ref("");
const isAssignedProject = ref(true);
const submissionSuccess = ref(false);
const datasetTypeOptions = ref(datasetTypes);
const isAssignedSourceDataProduct = ref(false);
const selectedSourceDataProduct = ref(null);
const sourceDataProductSearchText = ref('');
const selectedDatasetType = ref(
  datasetTypes.find((e) => e.value === config.dataset.types.DATA_PRODUCT.key),
);
// `willUploadRawData` determines whether the user will upload a Raw Data or a
// Data Product. By default, the user will upload a Data Product.
const willUploadRawData = ref(false);
// `stepPristineStates` tracks if a step's form fields are pristine (i.e. not
// touched by user) or not. Errors are only shown when a step's form fields are
// not pristine.
const stepPristineStates = ref([
  { [STEP_KEYS.SELECT_FILES]: true },
  { [STEP_KEYS.GENERAL_INFO]: true },
  { [STEP_KEYS.GENOMIC_DETAILS]: true },
  { [STEP_KEYS.UPLOAD]: true },
]);
const loading = ref(false);
const validatingForm = ref(false);
const selectedSourceInstrument = ref(null);
const sourceInstrumentOptions = ref([]);
const projectSelected = ref(null);
const datasetUploadLog = ref(null);
const submissionStatus = ref(Constants.UPLOAD_STATUSES.UNINITIATED);
const statusChipColor = ref("");
const submissionAlert = ref(""); // For handling network errors before upload begins
const submissionAlertColor = ref("");
const isSubmissionAlertVisible = ref(false);
const submitAttempted = ref(false);
const filesToUpload = ref([]);
const displayedFilesToUpload = ref([]);
const selectedDirectory = ref(null);
const selectingFiles = ref(false);
const selectingDirectory = ref(false);
const populatedDatasetName = ref("");
const step = ref(0);
const uploadCancelled = ref(false);
const selectedFileType = ref(null);
const selectedGenomeType = ref(null);
const selectedGenomeValue = ref(null);
const showCreateFileTypeModal = ref(false);
const newFileTypeName = ref('');
const newFileTypeExtension = ref('');
const analysisTypes = ref([]);
const newlyCreatedFileType = ref(null); // Track the file type created via modal

// Auto-prepend dot to extension
watch(newFileTypeExtension, (newVal) => {
  if (newVal && !newVal.startsWith('.')) {
    newFileTypeExtension.value = `.${newVal}`;
  }
});

// Validation errors
// Check if name/extension combo already exists (case-insensitive)
const checkDuplicateFileType = (name, extension) => {
  if (!name || !extension) return false;
  
  // Normalize name the same way we do when creating (to match API format)
  const normalizedName = name.trim().toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, '');
  const normalizedExt = extension.trim().toLowerCase();
  
  return analysisTypes.value.some(at => 
    at.name.toUpperCase() === normalizedName && 
    at.extension.toLowerCase() === normalizedExt
  );
};

// Form is invalid if fields are empty OR duplicate exists
const isFileTypeFormInvalid = computed(() => {
  return !newFileTypeName.value || 
         !newFileTypeExtension.value || 
         checkDuplicateFileType(newFileTypeName.value, newFileTypeExtension.value);
});

// Upload progress state
const uploadProgress = ref(0);
const filesUploaded = ref(0);
const totalFiles = ref(0);
const uploadProcessIds = ref([]); // Track process_ids for all uploaded files
const uploadRegistrationFailed = ref(false); // Track if final API call failed
const isComputingChecksum = ref(false); // Track checksum computation state
const checksumProgress = ref(0); // Track checksum computation progress (0-100)
const computedChecksum = ref(null); // Store computed checksum before upload

/**
 * Format duration in milliseconds to human-readable string
 * @param {number} ms - Duration in milliseconds
 * @returns {string} Formatted duration (e.g., "1.5s", "2.3m", "1.2h")
 */
const formatDuration = (ms) => {
  const seconds = ms / 1000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  } else if (seconds < 3600) {
    const minutes = seconds / 60;
    return `${minutes.toFixed(1)}m`;
  } else {
    const hours = seconds / 3600;
    return `${hours.toFixed(1)}h`;
  }
};

/**
 * Computed: Determine if upload completed successfully
 * Used to show success indicators and change UI text from "Files to Upload" to "Files Uploaded"
 * 
 * Success condition: When files are uploaded AND registered with API successfully
 * This is indicated by:
 * - statusChipColor is "success" (set in handleUploadComplete after successful API registration)
 * - OR submissionAlertColor is "success" with alert visible
 */
const isUploadComplete = computed(() => {
  return (
    statusChipColor.value === "success" ||
    (submissionAlertColor.value === "success" && isSubmissionAlertVisible.value)
  );
});

/**
 * Determines if the upload process has been completed.
 *
 * An upload is considered complete if `submissionStatus` has been set to `UPLOADED`. This occurs when:
 * - All files have been uploaded
 * - The upload has been registered with the API (process_id recorded)
 */
const isUploadIncomplete = computed(() => {
  return (
    submitAttempted.value &&
    submissionStatus.value !== Constants.UPLOAD_STATUSES.UPLOADED
  );
});

const stepHasErrors = computed(() => {
  if (step.value === 0) {
    return !!formErrors.value[STEP_KEYS.SELECT_FILES];
  } else if (step.value === 1) {
    return !!formErrors.value[STEP_KEYS.GENERAL_INFO];
  } else if (step.value === 2) {
    return !!formErrors.value[STEP_KEYS.GENOMIC_DETAILS];
  } else if (step.value === 3) {
    return !!formErrors.value[STEP_KEYS.UPLOAD];
  }
});

const isPreviousButtonDisabled = computed(() => {
  return (
    step.value === 0 ||
    submitAttempted.value ||
    loading.value ||
    validatingForm.value
  );
});

const isNextButtonDisabled = computed(() => {
  return (
    stepHasErrors.value ||
    submissionSuccess.value ||
    [
      Constants.UPLOAD_STATUSES.PROCESSING,
      Constants.UPLOAD_STATUSES.UPLOADING,
      Constants.UPLOAD_STATUSES.UPLOADED,
    ].includes(submissionStatus.value) ||
    loading.value ||
    validatingForm.value
  );
});

const stepIsPristine = computed(() => {
  return !!Object.values(stepPristineStates.value[step.value])[0];
});

const filesNotUploaded = computed(() => {
  return filesToUpload.value.filter(
    (e) => e.uploadStatus !== Constants.UPLOAD_STATUSES.UPLOADED,
  );
});

const someFilesPendingUpload = computed(
  () => filesNotUploaded.value.length > 0,
);

const isLastStep = computed(() => {
  return step.value === steps.length - 1;
});

const uploadFormData = computed(() => {
  return {
    name: populatedDatasetName.value,
    type: selectedDatasetType.value["value"],
    ...(selectedRawData.value && {
      src_dataset_id: selectedRawData.value.id,
    }),
    ...(selectedSourceDataProduct.value && {
      source_data_product_id: selectedSourceDataProduct.value.id,
    }),
    project_id: projectSelected.value ? projectSelected.value.id : null,
    src_instrument_id: selectedSourceInstrument.value
      ? selectedSourceInstrument.value.id
      : null,
    // Genomic details
    file_type: selectedFileType.value || null,
    genome_type: selectedGenomeType.value?.value || selectedGenomeType.value || null,
    genome_value: selectedGenomeValue.value || null,
    // Note: files_metadata removed - upload service tracks files internally, not in database
  };
});

const noFilesSelected = computed(() => {
  return filesToUpload.value?.length === 0;
});

const fileTypeOptions = computed(() => {
  return analysisTypes.value.map((at) => ({
    text: `${at.name} (${at.extension})`,
    value: at, // Pass the whole object
  }));
});

const genomeTypeOptions = computed(() => {
  return Object.entries(Constants.GENOME_TYPES).map(([key, value]) => ({
    text: value.label,
    value: key,
  }));
});

const shouldShowSourceDataProductField = computed(() => {
  // Show field only if:
  // 1. Dataset type is DATA_PRODUCT
  const isDataProduct = selectedDatasetType.value?.value === config.dataset.types.DATA_PRODUCT.key;
  
  // 2. File type is FASTQ
  const isFastq = selectedFileType.value?.name?.toUpperCase() === 'FASTQ';
  
  return isDataProduct && isFastq;
});

const availableGenomeValues = computed(() => {
  if (!selectedGenomeType.value) {
    return [];
  }

  // Extract the actual genome type key from the object
  const genomeTypeKey = selectedGenomeType.value.value || selectedGenomeType.value;
  const genomes = Constants.GENOME_TYPES[genomeTypeKey]?.genomes || [];

  return genomes;
});

const onFilesAdded = (files) => {
  clearSelectedDirectoryToUpload();
  setFiles(files);
  isSubmissionAlertVisible.value = false;
  setUploadedFileType(FILE_TYPE.FILE);
};

const onDirectoryAdded = (directoryDetails) => {
  clearSelectedFilesToUpload();
  setDirectory(directoryDetails);
  isSubmissionAlertVisible.value = false;
  setUploadedFileType(FILE_TYPE.DIRECTORY);
};

const clearSelectedRawData = () => {
  selectedRawData.value = null;
  datasetSearchText.value = "";
};

const resetProjectSearch = () => {
  projectSelected.value = null;
  projectSearchText.value = "";
};

const resetRawDataSearch = (val) => {
  clearSelectedRawData();
  if (!val) {
    datasetTypeOptions.value = datasetTypes;
  } else {
    datasetTypeOptions.value = datasetTypes.filter(
      (e) => e.value === config.dataset.types.DATA_PRODUCT.key,
    );
    selectedDatasetType.value = datasetTypeOptions.value.find(
      (e) => e.value === config.dataset.types.DATA_PRODUCT.key,
    );
    willUploadRawData.value = false;
  }
};

const onRawDataSearchOpen = () => {
  selectedRawData.value = null;
};

const onRawDataSearchClose = () => {
  if (!selectedRawData.value) {
    datasetSearchText.value = "";
  }
};

const clearSelectedSourceDataProduct = () => {
  selectedSourceDataProduct.value = null;
  sourceDataProductSearchText.value = '';
};

const resetSourceDataProductSearch = (val) => {
  clearSelectedSourceDataProduct();
};

const onSourceDataProductSearchOpen = () => {
  selectedSourceDataProduct.value = null;
};

const onSourceDataProductSearchClose = () => {
  if (!selectedSourceDataProduct.value) {
    sourceDataProductSearchText.value = '';
  }
};

const onProjectSearchOpen = () => {
  projectSelected.value = null;
};

const onProjectSearchClose = () => {
  if (!projectSelected.value) {
    projectSearchText.value = "";
  }
};

const isStepperButtonDisabled = (stepIndex) => {
  return (
    submitAttempted.value ||
    submissionSuccess.value ||
    step.value < stepIndex ||
    loading.value ||
    validatingForm.value
  );
};

const removeFile = (fileIndex) => {
  if (selectingDirectory.value) {
    selectingDirectory.value = false;
    clearSelectedDirectoryToUpload();
  } else if (selectingFiles.value) {
    filesToUpload.value.splice(fileIndex, 1);
    if (filesToUpload.value.length === 0) {
      selectingFiles.value = false;
    }
  }
};

const validateIfExists = (value) => {
  return new Promise((resolve, reject) => {
    // Vuestic claims that it should not run async validation if synchronous
    // validation fails, but it seems to be triggering async validation
    // nonetheless when `value` is ''. Hence the explicit check for whether
    // `value` is falsy.
    if (!value) {
      resolve(true);
    } else {
      datasetService
        .check_if_exists({
          type: selectedDatasetType.value["value"],
          name: value,
        })
        .then((res) => {
          resolve(res.data.exists);
        })
        .catch((e) => {
          console.error(e);
          reject();
        });
    }
  });
};

const resetFormErrors = () => {
  formErrors.value = {
    [STEP_KEYS.SELECT_FILES]: null,
    [STEP_KEYS.GENERAL_INFO]: null,
    [STEP_KEYS.GENOMIC_DETAILS]: null,
    [STEP_KEYS.UPLOAD]: null,
  };
};

const validateDatasetName = async () => {
  if (!populatedDatasetName.value) {
    return { isNameValid: false, error: DATASET_NAME_REQUIRED_ERROR };
  } else if (populatedDatasetName.value?.length < 3) {
    return { isNameValid: false, error: DATASET_NAME_MIN_LENGTH_ERROR };
  } else if (populatedDatasetName.value?.indexOf(" ") > -1) {
    return { isNameValid: false, error: DATASET_NAME_HAS_SPACES_ERROR };
  }

  validatingForm.value = true;
  return validateIfExists(populatedDatasetName.value)
    .then((res) => {
      const datasetExistsError = (datasetType) => {
        const datasetTypeLabel = datasetTypes.find(
          (type) => type.value === datasetType,
        ).label;
        return `A ${datasetTypeLabel} with this name already exists.`;
      };
      return {
        isNameValid: !res,
        error: res && datasetExistsError(selectedDatasetType.value["value"]),
      };
    })
    .catch(() => {
      return { isNameValid: false, error: UNKNOWN_VALIDATION_ERROR };
    })
    .finally(() => {
      validatingForm.value = false;
    });
};

const clearSelectedDirectoryToUpload = ({
  clearDirectoryFiles = true,
} = {}) => {
  // clear files within the directory being removed
  if (clearDirectoryFiles) {
    clearSelectedFilesToUpload();
  }
  // clear directory being removed
  selectedDirectory.value = null;
};

const clearSelectedFilesToUpload = () => {
  displayedFilesToUpload.value = [];
};

const setUploadedFileType = (fileType) => {
  if (fileType === FILE_TYPE.FILE) {
    selectingFiles.value = true;
    selectingDirectory.value = false;
  } else if (fileType === FILE_TYPE.DIRECTORY) {
    selectingDirectory.value = true;
    selectingFiles.value = false;
  }
};

const setFormErrors = async () => {
  resetFormErrors();

  if (step.value === 0) {
    if (displayedFilesToUpload.value.length === 0) {
      formErrors.value[STEP_KEYS.SELECT_FILES] = true;
      return;
    }
  }

  if (step.value === 1) {
    if (
      (isAssignedSourceRawData.value && !selectedRawData.value) ||
      (isAssignedProject.value && !projectSelected.value) ||
      (isAssignedSourceInstrument.value && !selectedSourceInstrument.value) ||
      (isAssignedSourceDataProduct.value && !selectedSourceDataProduct.value)
    ) {
      formErrors.value[STEP_KEYS.GENERAL_INFO] = true;
      return;
    } else {
      formErrors.value[STEP_KEYS.GENERAL_INFO] = null;
    }
  }

  if (step.value === 2) {
    // Genomic details step - all fields are optional
    formErrors.value[STEP_KEYS.GENOMIC_DETAILS] = null;
  }

  if (step.value === 3) {
    const { isNameValid: datasetNameIsValid, error} =
      await validateDatasetName();
    if (datasetNameIsValid) {
      formErrors.value[STEP_KEYS.UPLOAD] = null;
    } else {
      formErrors.value[STEP_KEYS.UPLOAD] = error;
    }
  }
};

const onSubmit = async () => {
  if (filesToUpload.value.length === 0) {
    await setFormErrors();
    return Promise.reject();
  }

  submissionStatus.value = Constants.UPLOAD_STATUSES.PROCESSING;
  statusChipColor.value = "primary";
  submissionAlert.value = null; // reset any alerts from previous submissions
  isSubmissionAlertVisible.value = false;
  submitAttempted.value = true;

  return new Promise((resolve, reject) => {
    preUpload()
      .then(async () => {
        submissionSuccess.value = true;
        
        // COMPUTE CHECKSUMS FIRST (before upload starts)
        // Skip if already computed (e.g., on retry after upload failure)
        console.log('=== CHECKSUM VERIFICATION CHECK (BEFORE UPLOAD) ===');
        console.log('Feature enabled?', _isChecksumVerificationEnabled());
        console.log('Files to hash:', filesToUpload.value.length);
        console.log('Already computed?', computedChecksum.value ? 'YES' : 'NO');
        
        let checksumStartTime = null;
        let checksumEndTime = null;
        
        if (_isChecksumVerificationEnabled() && !computedChecksum.value) {
          try {
            console.log('✓ STARTING checksum computation BEFORE upload...');
            console.log('  Setting isComputingChecksum = true');
            isComputingChecksum.value = true;
            checksumProgress.value = 0;
            
            checksumStartTime = performance.now();
            
            const files = filesToUpload.value.map(f => f.file);
            console.log('  Files mapped:', files.map(f => `${f.name} (${f.size} bytes)`));
            
            console.log('  Calling _computeManifestHash...');
            computedChecksum.value = await _computeManifestHash(files, (progress) => {
              console.log(`  Checksum progress: ${progress}%`);
              checksumProgress.value = progress;
            });
            
            checksumEndTime = performance.now();
            
            if (computedChecksum.value) {
              console.log('✓ CHECKSUM COMPUTED (BEFORE UPLOAD):', {
                manifest_hash: computedChecksum.value.manifest_hash,
                file_count: computedChecksum.value.file_count,
                total_size: computedChecksum.value.total_size,
                mode: computedChecksum.value.mode
              });
            } else {
              console.warn('⚠ Manifest hash computation returned null (checksum disabled or error)');
            }
          } catch (error) {
            console.error('✗ FAILED to compute manifest hash:', error);
            console.error('  Error stack:', error.stack);
            // Don't fail upload - allow it to proceed without checksum
          } finally {
            console.log('  Setting isComputingChecksum = false');
            isComputingChecksum.value = false;
            checksumProgress.value = 0;
            
            if (checksumStartTime && checksumEndTime) {
              const duration = checksumEndTime - checksumStartTime;
              console.log(`⏱️  CHECKSUM COMPUTATION TIME: ${formatDuration(duration)}`);
            }
            
            console.log('=== CHECKSUM COMPUTATION COMPLETE (BEFORE UPLOAD) ===');
          }
        } else if (computedChecksum.value) {
          console.log('✓ Using previously computed checksum (skipping re-computation on retry)');
          console.log('  Cached checksum:', {
            manifest_hash: computedChecksum.value.manifest_hash,
            file_count: computedChecksum.value.file_count,
            total_size: computedChecksum.value.total_size
          });
        } else {
          console.log('✗ Checksum verification disabled - skipping computation');
        }
        
        // NOW START UPLOAD
        submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOADING;

        // Use resumable upload protocol instead of old chunk system
        // Get the actual File objects to upload
        const filesToUploadList = filesToUpload.value.map(f => f.file);
        
        totalFiles.value = filesToUploadList.length;
        filesUploaded.value = 0;
        uploadProgress.value = 0;

        console.log('=== STARTING FILE UPLOAD ===');
        const uploadStartTime = performance.now();
        
        const uploadServiceURL = _getUploadServiceURL(window.location.origin);
        const uploaded = await uploadFilesWithTus(filesToUploadList, uploadServiceURL);
        
        const uploadEndTime = performance.now();
        
        if (uploaded) {
          const uploadDuration = uploadEndTime - uploadStartTime;
          console.log(`⏱️  FILE UPLOAD TIME: ${formatDuration(uploadDuration)}`);
          console.log('=== FILE UPLOAD COMPLETE ===');
          
          handleUploadComplete();
          resolve();
        } else {
          submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOAD_FAILED;
          submissionAlert.value = "Some files could not be uploaded.";
          reject();
        }
      })
      .catch((err) => {
        console.error(err);
        submissionStatus.value = Constants.UPLOAD_STATUSES.PROCESSING_FAILED;
        submissionAlert.value =
          "There was an error. Please try submitting again.";
        reject();
      });
  });
};

const setPostSubmissionSuccessState = () => {
  if (!someFilesPendingUpload.value) {
    submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOADED;
    statusChipColor.value = "primary";
    submissionAlertColor.value = "success";
    submissionAlert.value =
      "All files have been uploaded successfully. You may close this window.";
    isSubmissionAlertVisible.value = true;
  }
};

/**
 * Called when all files have been successfully uploaded, and the upload has been registered
 * with the API. The integrated workflow will be triggered by the polling job.
 */
const postSubmit = () => {
  if (uploadCancelled.value) {
    return;
  }

  // TUS handles file tracking internally, no need to update individual file statuses
  // Just set the overall success state
  setPostSubmissionSuccessState();
};

const handleSubmit = () => {
  onSubmit() // resolves once all files have been uploaded (TUS handles workflow triggering internally)
    .then(() => {
      // Upload complete - handleUploadComplete() already triggered the workflow
      // Nothing more to do here
    })
    .catch(() => {
      submissionSuccess.value = false;
      statusChipColor.value = "warning";
      submissionAlert.value = "An error occurred.";
      submissionAlertColor.value = "warning";
      isSubmissionAlertVisible.value = true;
    })
    .finally(() => {
      postSubmit();
    });
};

const onNextClick = (nextStep) => {
  if (isLastStep.value) {
    if (noFilesSelected.value) {
      isSubmissionAlertVisible.value = true;
      submissionAlert.value = "At least one file must be selected";
      submissionAlertColor.value = "warning";
    } else {
      handleSubmit();
    }
  } else {
    nextStep();
  }
};

// Evaluates selected file checksums, logs the upload
const preUpload = async () => {
  // TUS doesn't need pre-calculated checksums - it handles that internally
  // Just create or update the upload log

  const isUpdate = !!datasetUploadLog.value?.id;
  const logData = isUpdate
    ? {
        status: Constants.UPLOAD_STATUSES.UPLOADING,
      }
    : {
        ...uploadFormData.value,
      };

  console.log('[PRE-UPLOAD] Starting pre-upload registration', {
    is_update: isUpdate,
    existing_log_id: datasetUploadLog.value?.id,
    log_data: logData,
  });

  try {
    const res = await createOrUpdateUploadLog(logData);
    datasetUploadLog.value = res.data;
    
    console.log('[PRE-UPLOAD] SUCCESS: Upload log created/updated', {
      upload_log_id: datasetUploadLog.value.id,
      dataset_id: datasetUploadLog.value.audit_log?.dataset?.id,
      dataset_name: datasetUploadLog.value.audit_log?.dataset?.name,
      status: datasetUploadLog.value.status,
    });
  } catch (err) {
    console.error('[PRE-UPLOAD] FAILED: Error creating/updating upload log', {
      error_message: err.message,
      error_response: err.response?.data,
      error_status: err.response?.status,
      error_stack: err.stack,
      log_data: logData,
    });
    throw new Error("Error logging dataset upload");
  }
};

// Log (or update) upload status
const createOrUpdateUploadLog = (data) => {
  if (!uploadCancelled.value) {
    const isCreate = !datasetUploadLog.value;
    console.log(`[CREATE-OR-UPDATE-LOG] ${isCreate ? 'Creating' : 'Updating'} upload log`, {
      is_create: isCreate,
      dataset_id: datasetUploadLog.value?.audit_log?.dataset?.id,
      data,
    });
    
    return isCreate
      ? datasetService.logDatasetUpload(data)
      : datasetService.updateDatasetUploadLog(
          datasetUploadLog.value?.audit_log?.dataset.id,
          data,
        );
  } else {
    console.log('[CREATE-OR-UPDATE-LOG] Upload cancelled, rejecting');
    return Promise.reject();
  }
};

// Removed: Old uploadFiles function - replaced by uploadFilesWithTus (TUS protocol implementation)

const setFiles = (files) => {
  _.range(0, files.length).forEach((i) => {
    const file = files.item(i);
    filesToUpload.value.push({
      type: FILE_TYPE.FILE,
      file: file,
      name: file.name,
      formattedSize: formatBytes(file.size),
      progress: 0,
    });
  });
  displayedFilesToUpload.value = filesToUpload.value;
};

const setDirectory = (directoryDetails) => {
  const directoryFiles = directoryDetails.files;
  let directorySize = 0;
  _.range(0, directoryFiles.length).forEach((i) => {
    const file = directoryFiles[i];
    filesToUpload.value.push({
      type: FILE_TYPE.FILE,
      file: file,
      name: file.name,
      formattedSize: formatBytes(file.size),
      progress: 0,
      path: file.path,
    });
    directorySize += file.size;
  });
  selectedDirectory.value = {
    type: FILE_TYPE.DIRECTORY,
    name: directoryDetails.directoryName,
    formattedSize: formatBytes(directorySize),
    progress: 0,
  };

  displayedFilesToUpload.value = [selectedDirectory.value];
};

watch(selectedDatasetType, (newVal) => {
  if (newVal["value"] === config.dataset.types.RAW_DATA.key) {
    isAssignedSourceRawData.value = false;
    clearSelectedRawData();
    willUploadRawData.value = true;
    // Hide and clear source data product if switching to raw data
    isAssignedSourceDataProduct.value = false;
    clearSelectedSourceDataProduct();
  } else {
    willUploadRawData.value = false;
  }
});

// Hide and clear source data product if file type is not FASTQ
watch(selectedFileType, (newVal) => {
  if (newVal?.name?.toUpperCase() !== 'FASTQ') {
    isAssignedSourceDataProduct.value = false;
    clearSelectedSourceDataProduct();
  }
});

// Clear genome value when genome type changes
watch(selectedGenomeType, () => {
  selectedGenomeValue.value = null;
});

watch(selectingFiles, () => {
  if (selectingFiles.value) {
    populatedDatasetName.value = "";
  }
});

watch(selectingDirectory, () => {
  if (selectingDirectory.value) {
    populatedDatasetName.value = selectedDirectory.value.name;
  }
});

// Form errors are set when this component mounts, or when a form field's value
// changes, or when the current step changes.
watch(
  [
    step,
    populatedDatasetName,
    projectSelected,
    isAssignedProject,
    selectedRawData,
    isAssignedSourceRawData,
    selectedSourceInstrument,
    isAssignedSourceInstrument,
    selectedSourceDataProduct,
    isAssignedSourceDataProduct,
    selectingFiles,
    selectingDirectory,
    filesToUpload,
    selectedFileType,
    selectedGenomeType,
    selectedGenomeValue,
  ],
  async (newVals, oldVals) => {
    // Mark step's form fields as not pristine, for fields' errors to be shown
    const stepKey = Object.keys(stepPristineStates.value[step.value])[0];
    if (stepKey === STEP_KEYS.UPLOAD) {
      // `1` corresponds to `populatedDatasetName`
      stepPristineStates.value[step.value][stepKey] = !oldVals[1] && newVals[1];
    } else {
      stepPristineStates.value[step.value][stepKey] = false;
    }

    await setFormErrors();
  },
);

// Load analysis types from API
const loadAnalysisTypes = () => {
  analysisTypeService
    .getAll()
    .then((res) => {
      analysisTypes.value = res.data;
    })
    .catch((err) => {
      toast.error('Failed to load file types');
      console.error(err);
    });
};

// Open modal and clear fields
const openCreateFileTypeModal = () => {
  newFileTypeName.value = '';
  newFileTypeExtension.value = '';
  showCreateFileTypeModal.value = true;
};

// Handle creating new file type
const handleCreateFileType = () => {
  // Prevent action if form is invalid (empty fields or duplicate exists)
  if (isFileTypeFormInvalid.value) {
    return; // Do nothing if invalid
  }

  // Remove the previously created file type if it exists
  if (newlyCreatedFileType.value) {
    const index = analysisTypes.value.findIndex(at => at === newlyCreatedFileType.value);
    if (index !== -1) {
      analysisTypes.value.splice(index, 1);
    }
    // If the removed type was selected, clear selection
    if (selectedFileType.value === newlyCreatedFileType.value) {
      selectedFileType.value = null;
    }
  }

  // Create the new file type object (don't save to API yet)
  const newAnalysisType = {
    name: newFileTypeName.value.trim().toUpperCase().replace(/\s+/g, '_').replace(/[^A-Z0-9_]/g, ''),
    extension: newFileTypeExtension.value.trim(),
  };

  // Add to local list
  analysisTypes.value.push(newAnalysisType);

  // Select it
  selectedFileType.value = newAnalysisType;

  // Track this as the newly created type
  newlyCreatedFileType.value = newAnalysisType;

  // Clear fields and close modal
  newFileTypeName.value = '';
  newFileTypeExtension.value = '';
  showCreateFileTypeModal.value = false;
};

// Handle canceling file type creation
const handleCancelFileType = () => {
  // Clear fields and close modal
  newFileTypeName.value = '';
  newFileTypeExtension.value = '';
  showCreateFileTypeModal.value = false;
};

onMounted(() => {
  loading.value = true;
  instrumentService
    .getAll()
    .then((res) => {
      sourceInstrumentOptions.value = res.data;
    })
    .catch((err) => {
      toast.error("Failed to load resources");
      console.error(err);
    })
    .finally(() => {
      loading.value = false;
    });
  loadAnalysisTypes();
});

onMounted(() => {
  setFormErrors();
});

/**
 * Route Change Handling Mechanism
 *
 * This mechanism is designed to handle the scenario when a user attempts to navigate away from the current page
 * while the upload is incomplete. It uses Vue Router's navigation guards and component lifecycle hooks
 * to prompt the user for confirmation and cancel the upload if necessary.
 *
 * Key Components:
 *
 * 1. `isUploadIncomplete`:
 *    A computed property that determines if the upload is still in progress or incomplete.
 *
 * 2. `onBeforeRouteLeave`:
 *    Navigation guard triggered when the user attempts to navigate to a different route.
 *    It shows a browser confirmation dialog if the upload is incomplete.
 *
 * 3. `onBeforeUnmount`:
 *    Lifecycle hook triggered when the component is about to be unmounted (which happens during navigation).
 *    It cancels the upload if it's incomplete.
 *
 * 4. `uploadCancelled`:
 *    A reactive variable used to signal that the upload should be considered cancelled.
 *
 * Flow:
 * 1. User attempts to navigate to a different route
 * 2. `onBeforeRouteLeave` is triggered
 *    - If upload is incomplete, shows a confirmation dialog
 *    - If user confirms, allows navigation; if user cancels, prevents navigation
 * 3. If navigation is allowed, `onBeforeUnmount` is triggered
 *    - Sets `uploadCancelled` to true
 *    - If upload is incomplete, sends a request to cancel the upload
 *
 */

onBeforeRouteLeave(() => {
  // Before navigating to a different route, show user a confirmation dialog
  return isUploadIncomplete.value
    ? window.confirm(
        "Leaving this page before all files have been uploaded will" +
          " cancel the upload. Do you wish to continue?",
      )
    : true;
});

onBeforeUnmount(() => {
  uploadCancelled.value = true;
  // Upload cleanup will be handled by background monitoring process
});

/**
 * Browser Tab Closure Handling Mechanism
 *
 * This handler is triggered when the user attempts to close the browser tab or navigate away from the page.
 * It shows a browser alert to confirm the user's intention if an upload is in progress.
 *
 * @description
 * - Checks if an upload is incomplete using the `isUploadIncomplete` computed property.
 * - If an upload is incomplete:
 *   - Sets the `returnValue` of the event to `true`, which prompts the browser to show a confirmation dialog.
 * - This prevents accidental data loss by giving the user a chance to confirm before leaving the page during an upload.
 *
 */
const onBeforeUnload = (e) => {
  if (isUploadIncomplete.value) {
    e.returnValue = true;
  }
};

onMounted(() => {
  window.addEventListener("beforeunload", onBeforeUnload);
});

onBeforeUnmount(() => {
  window.removeEventListener("beforeunload", onBeforeUnload);
});

/* eslint-disable */
// /**
//  * Browser Tab Closure Handling Mechanism
//  *
//  * This mechanism is designed to handle the scenario when a user attempts to close the browser tab
//  * during an incomplete upload process. It uses a combination of browser events and a local storage
//  * flag to determine whether to cancel the upload.
//  *
//  * Key Components:
//  *
//  * 1. `isClosingBrowserTab`:
//  *    A reactive variable stored in local storage that indicates whether the browser tab is about to be closed.
//  *
//  * 2. `onBeforeUnload`:
//  *    Event handler triggered when the user attempts to close the tab. It shows a browser
//  *    confirmation dialog asking the user if they want to leave. If the upload is incomplete at this point,
//  *    a flag (`isClosingBrowserTab`) is set to track this, with a reset-timeout.
//  *
//  * 3. `onUnload`:
//  *    Event handler triggered if the user confirms that they wish to close the tab.
//  *    - if the user confirms closing the tab within a short delay (500ms):
//  *      - It checks if the upload is incomplete, and if so, cancels the upload
//  *    - if the user takes longer than 500ms to confirm closing the tab:
//  *      - The upload is not cancelled. In this case the system will end up with an incomplete upload,
//  *        which can be cleaned up later.
//  *
//  * 4. Event Listeners:
//  *    Added on component mount and removed on unmount to ensure proper cleanup of an incomplete upload.
//  *
//  * Flow:
//  * 1. User attempts to close tab
//  * 2. `onBeforeUnload` is triggered
//  *    - Shows confirmation dialog
//  *    - Sets the `isClosingBrowserTab` flag to `true`
//  *    - Starts a 500ms timeout after which the flag is reset
//  * 3. If user confirms that they wish to close the tab, `onUnload` is triggered
//  *    - Checks `isClosingBrowserTab` and `isUploadIncomplete`
//  *    - Cancels upload if both are true
//  * 4. If user doesn't confirm within 500ms, the `isClosingBrowserTab` flag is reset
//  *
//  */
//
// // Used to indicate if the user intends to close the current browser tab.
// const isClosingBrowserTab = ref(false);
//
// const onBeforeUnload = (e) => {
//   debugger;
//   if (isUploadIncomplete.value) {
//     console.log(
//         "Show user a browser alert to get confirmation before leaving the page",
//     );
//     e.returnValue = true; // this shows the browser alert before user leaves the page
//     isClosingBrowserTab.value = true;
//
//     // If user hasn't confirmed or cancelled the browser alert within a short
//     // delay, assume they are not leaving the page.
//     // setTimeout(() => {
//     //   console.log("500 ms elapsed. Assume user won't leave the page.");
//     //   isClosingBrowserTab.value = false;
//     // }, 500);
//   }
// };
//
// const onUnload = () => {
//   if (isClosingBrowserTab.value) {
//     console.log(
//         "User confirmed that they are leaving page. Upload is incomplete, and will be cancelled.",
//     );
//     datasetService.cancelDatasetUpload(
//         datasetUploadLog.value.audit_log.dataset.id,
//     );
//   } else {
//     console.log(
//         "User did not confirm leaving page within 500 ms. Upload is incomplete, but will not be cancelled.",
//     );
//   }
//   isClosingBrowserTab.value = false;
// };

// TUS upload logic
const uploadFilesWithTus = async (files, endpoint) => {
  // Safety check: ensure upload log exists
  if (!datasetUploadLog.value || !datasetUploadLog.value.dataset) {
    console.error('Dataset upload log not initialized');
    throw new Error('Dataset upload log not initialized');
  }

  // Get token directly from localStorage (more reliable than Pinia store in this context)
  const userToken = localStorage.getItem('token');
  if (!userToken) {
    console.error('No authentication token available');
    throw new Error('Authentication token not found');
  }

  console.log('Starting upload with token:', userToken ? `Token exists (length: ${userToken.length})` : 'No token');

  let uploadedCount = 0;
  let totalBytes = 0;
  let uploadedBytes = 0;

  // Calculate total size
  files.forEach(file => {
    totalBytes += file.size;
  });

  // TEST ONLY: Check if we should simulate mid-upload failure
  // Set localStorage.setItem('SIMULATE_UPLOAD_FAILURE', 'mid-upload') to enable
  // Set localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '5') to fail 5 times
  const simulateFailure = localStorage.getItem('SIMULATE_UPLOAD_FAILURE');
  const simulateFailureCount = localStorage.getItem('SIMULATE_UPLOAD_FAILURE_COUNT');
  if (simulateFailure) {
    console.warn(`🧪 [TEST MODE] Upload failure simulation ENABLED: ${simulateFailure}`);
    console.warn(`   Failure count: ${simulateFailureCount || '1'} (1=fail once then succeed, 5=exhaust retries)`);
    console.warn(`   To disable: localStorage.removeItem('SIMULATE_UPLOAD_FAILURE')`);
  }

  const uploadPromises = files.map((file, index) => {
    return new Promise((resolve, reject) => {
      console.log(`[TUS-CLIENT] Starting upload for file ${index + 1}/${files.length}:`, {
        name: file.name,
        size: file.size,
        type: file.type,
        dataset_id: datasetUploadLog.value.dataset.id,
        simulate_failure: simulateFailure || 'none',
      });
      
      // TEST ONLY: Check for failure count configuration
      // Set localStorage.setItem('SIMULATE_UPLOAD_FAILURE_COUNT', '5') to fail 5 times (exhausts retries)
      const simulateFailureCount = localStorage.getItem('SIMULATE_UPLOAD_FAILURE_COUNT');
      
      // Overall timeout for this upload (30 seconds)
      // If TUS retries don't complete within this time, give up and show "Upload Failed"
      const UPLOAD_TIMEOUT_MS = 30000; // 30 seconds
      let timeoutId = null;
      let upload = null;
      
      // Start timeout timer - will abort upload if it exceeds 30 seconds
      timeoutId = setTimeout(() => {
        console.error(`[TUS-CLIENT] ⏱️  Upload TIMEOUT after ${UPLOAD_TIMEOUT_MS / 1000}s for ${file.name}`);
        console.error(`[TUS-CLIENT] Aborting upload due to timeout...`);
        
        if (upload) {
          upload.abort(true); // true = shouldTerminate (delete partial upload on server)
        }
        
        reject(new Error(`Upload timeout after ${UPLOAD_TIMEOUT_MS / 1000} seconds - retries exhausted or server not responding`));
      }, UPLOAD_TIMEOUT_MS);
      
      upload = new tus.Upload(file, {
        endpoint,
        // Increased retries for testing: allows up to 15 attempts total (1 initial + 14 retries)
        // Delays: 0s, 1s, 2s, 3s, 5s, 8s, 13s, 21s, 34s, 55s (Fibonacci-like progression)
        // This ensures we can test scenarios where retries exceed 30s timeout
        retryDelays: [0, 1000, 2000, 3000, 5000, 8000, 13000, 21000, 34000, 55000, 89000, 144000, 233000, 377000],
        metadata: {
          dataset_id: String(datasetUploadLog.value.dataset.id),
          filename: file.name,
          filetype: file.type || 'application/octet-stream',
          selection_mode: selectingDirectory.value ? 'directory' : 'files',
          relative_path: file.webkitRelativePath || file.name,
          directory_name: selectingDirectory.value && selectedDirectory.value ? selectedDirectory.value.name : '',
        },
        headers: {
          Authorization: `Bearer ${userToken}`,
          ...(simulateFailure ? { 
            'X-Simulate-Failure': simulateFailure,
            ...(simulateFailureCount ? { 'X-Simulate-Failure-Count': simulateFailureCount } : {})
          } : {}),
        },
        onError: (error) => {
          // Clear timeout on error
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          
          console.error(`[TUS-CLIENT] Upload FAILED for ${file.name}:`, {
            error_message: error.message,
            error_type: error.constructor.name,
            error_stack: error.stack,
            file_name: file.name,
            file_size: file.size,
            dataset_id: datasetUploadLog.value.dataset.id,
            upload_url: upload.url,
            // Check if it's an HTTP error
            originalRequest: error.originalRequest ? {
              method: error.originalRequest.getMethod(),
              url: error.originalRequest.getURL(),
              status: error.originalResponse?.getStatus(),
              statusText: error.originalResponse?.getBody(),
            } : null,
          });
          reject(error);
        },
        onProgress: (bytesUploaded, bytesTotal) => {
          // Update overall progress
          const totalUploadedSoFar = uploadedBytes + bytesUploaded;
          uploadProgress.value = Math.round((totalUploadedSoFar / totalBytes) * 100);
          
          // Log progress every 10% for large files
          const fileProgress = (bytesUploaded / bytesTotal) * 100;
          if (fileProgress % 10 < 1) {
            console.log(`[TUS-CLIENT] Upload progress for ${file.name}: ${fileProgress.toFixed(1)}%`, {
              bytes_uploaded: bytesUploaded,
              bytes_total: bytesTotal,
            });
          }
        },
        onSuccess: async () => {
          // Clear timeout on success
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
          
          uploadedCount++;
          uploadedBytes += file.size;
          filesUploaded.value = uploadedCount;
          uploadProgress.value = Math.round((uploadedBytes / totalBytes) * 100);
          
          // Store the process_id for this file - will be sent to API after all uploads complete
          const processId = upload.url.split('/').pop();
          if (!uploadProcessIds.value) {
            uploadProcessIds.value = [];
          }
          uploadProcessIds.value.push({
            process_id: processId,
            relative_path: file.webkitRelativePath || file.name,
          });
          
          console.log(`[TUS-CLIENT] Upload SUCCESS for ${file.name}`, {
            process_id: processId,
            file_size: file.size,
            upload_url: upload.url,
          });
          resolve();
        },
      });

      // Start the upload
      console.log(`[TUS-CLIENT] Initiating upload.start() for ${file.name}`);
      upload.start();
    });
  });

  try {
    console.log(`[TUS-CLIENT] Waiting for all ${uploadPromises.length} upload(s) to complete...`);
    await Promise.all(uploadPromises);
    console.log(`[TUS-CLIENT] All uploads completed successfully`);
    return true;
  } catch (error) {
    console.error('[TUS-CLIENT] One or more uploads failed:', {
      error_message: error.message,
      error_type: error.constructor.name,
      total_files: files.length,
      uploaded_count: uploadedCount,
    });
    return false;
  }
};

const handleUploadComplete = async () => {
  // Call API to register all process_ids - this is the critical call
  // Only show success if this succeeds
  try {
    const datasetId = datasetUploadLog.value.dataset.id;
    
    console.log('[UPLOAD-COMPLETE] Starting upload completion API call', {
      dataset_id: datasetId,
      process_ids_count: uploadProcessIds.value?.length || 0,
    });
    
    // Build metadata with checksum (use pre-computed checksum from before upload)
    let metadata = {};
    
    console.log('=== USING PRE-COMPUTED CHECKSUM (from before upload) ===');
    if (computedChecksum.value) {
      console.log('✓ Checksum available:', {
        manifest_hash: computedChecksum.value.manifest_hash,
        file_count: computedChecksum.value.file_count,
        total_size: computedChecksum.value.total_size,
        mode: computedChecksum.value.mode
      });
      metadata.checksum = computedChecksum.value;
    } else {
      console.log('⚠ No pre-computed checksum available (checksum disabled or computation failed)');
    }
    
    // Call /complete with the last process_id (for single file) or first (for multi)
    // The worker will handle moving all files based on upload metadata
    const lastUpload = uploadProcessIds.value[uploadProcessIds.value.length - 1];
    
    const completePayload = {
      process_id: lastUpload.process_id,
      selection_mode: selectingDirectory.value ? 'directory' : 'files',
      directory_name: selectingDirectory.value && selectedDirectory.value ? selectedDirectory.value.name : '',
      relative_path: lastUpload.relative_path,
      metadata: Object.keys(metadata).length > 0 ? metadata : undefined,
    };
    
    console.log('[UPLOAD-COMPLETE] Calling /complete endpoint', {
      dataset_id: datasetId,
      payload: completePayload,
    });
    
    const response = await datasetService.completeDatasetUpload(datasetId, completePayload);
    
    console.log('[UPLOAD-COMPLETE] API call SUCCESS', {
      dataset_id: datasetId,
      response,
    });
    
    // Success - show green status
    uploadRegistrationFailed.value = false;
    submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOADED;
    statusChipColor.value = "success";
    submissionAlert.value = "All files have been uploaded successfully!";
    submissionAlertColor.value = "success";
    isSubmissionAlertVisible.value = true;
    submissionSuccess.value = true;
    
  } catch (error) {
    console.error('[UPLOAD-COMPLETE] API call FAILED:', {
      error_message: error.message,
      error_response: error.response?.data,
      error_status: error.response?.status,
      error_stack: error.stack,
      dataset_id: datasetUploadLog.value?.audit_log?.dataset?.id,
    });
    
    // API call failed - show retry option
    uploadRegistrationFailed.value = true;
    submissionStatus.value = Constants.UPLOAD_STATUSES.UPLOAD_FAILED;
    statusChipColor.value = "warning";
    submissionAlert.value = "Files uploaded but registration failed. Please retry.";
    submissionAlertColor.value = "warning";
    isSubmissionAlertVisible.value = true;
    submissionSuccess.value = false;
  }
};

// Retry the API call to register the upload
const retryApiCall = async () => {
  submissionAlert.value = "Retrying ...";
  submissionAlertColor.value = "info";
  await handleUploadComplete();
};

//
// onMounted(() => {
//   window.addEventListener("beforeunload", onBeforeUnload);
//   window.addEventListener("unload", onUnload);
// });
//
// onBeforeUnmount(() => {
//   window.removeEventListener("beforeunload", onBeforeUnload);
//   window.removeEventListener("unload", onUnload);
//   // once the `onUnload` handler is finished, the `isClosingBrowserTab` flag
//   // can be reset
//   isClosingBrowserTab.value = false;
// });
/* eslint-enable */
</script>

<style lang="scss">
.create-data-product-stepper {
  .step-button {
    color: var(--va-secondary);
  }

  .step-button--active {
    color: var(--va-primary);
  }

  .step-button--completed {
    color: var(--va-primary);
  }

  .step-button:hover {
    background-color: var(--va-background-element);
  }

  .va-stepper__step-content-wrapper {
    // flex: 1 to expand the element to available height
    // min-height: 0 to shrink the element to below its calculated min-height of children
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
  }

  .va-stepper__step-content {
    // step-content-wrapper contains step-content and controls
    // only shrink and grow step-content
    flex: 1;
    min-height: auto;
    overflow-y: scroll;
  }

  .raw_data_select .va-select-content__autocomplete {
    padding-top: 7.5px;
    padding-bottom: 7.5px;
  }

  .upload-details {
    min-height: 400px;
  }
}
</style>
