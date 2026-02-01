<template>
  <!--  <va-inner-loading :loading="loading" class="h-full">-->
  <va-stepper v-model="step" :steps="steps" controlsHidden class="h-full import-stepper">
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
      <div class="flex">
        <va-select
          class="mr-2"
          v-model="searchSpace"
          @update:modelValue="resetSearch"
          :options="FILESYSTEM_SEARCH_SPACES"
          :text-by="'label'"
          :track-by="'key'"
          label="Search space"
          :disabled="submitAttempted || searchingFiles || validatingForm"
        />

        <div class="flex flex-col w-full">
          <FileListAutoComplete
            v-model:search-text="fileListSearchText"
            v-model:selected="selectedFile"
            @update:selected="fileList = []"
            :disabled="submitAttempted"
            :base-path="searchSpaceBasePath"
            :loading="searchingFiles"
            :validating="validatingForm"
            @clear="resetSearch"
            @open="onFileSearchAutocompleteOpen"
            @close="onFileSearchAutocompleteClose"
            :options="fileList"
          />

          <div class="text-xs va-text-danger" v-if="!stepIsPristine">
            {{ formErrors[STEP_KEYS.SELECT_DIRECTORY] }}
          </div>
        </div>
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
                Raw Data: Original, unprocessed data collected from instruments.
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
              :disabled="willImportRawData"
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
                Associating a Data Product with a source Raw Data establishes a clear lineage
                between the original data and its processed form. This linkage helps to trace the
                origins of processed data
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
                Assigning a dataset to a project establishes a connection between your data and a
                specific research initiatives. This association helps organize and categorize
                datasets within the context of your research projects, facilitating easier data
                management, access control, and collaboration among team members working on the same
                project.
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
                <div class="w-72">Source instrument where this data was collected from.</div>
              </template>
              <Icon icon="mdi:information" class="text-xl text-gray-500" />
            </va-popover>
          </div>
        </div>
      </div>
    </template>

    <template #step-content-2>
      <!-- Genomic Details Step -->
      <div class="flex w-full pb-6">
        <va-select
          v-model="selectedFileType"
          :options="fileTypeOptions"
          label="File Type"
          placeholder="Select file type"
          class="flex-grow"
          :text-by="'text'"
          :value-by="'value'"
        />
        <div class="flex items-end ml-2 pb-1">
          <va-popover message="Create new File Type">
            <va-button
              icon="add"
              class="px-1"
              color="success"
              @click="openCreateFileTypeModal"
            />
          </va-popover>
        </div>
      </div>

      <div class="flex w-full pb-6">
        <va-select
          v-model="selectedGenomeType"
          :options="genomeTypeOptions"
          label="Genome Type"
          placeholder="Select genome type"
          class="flex-grow mr-2"
          :text-by="'text'"
          :track-by="'value'"
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
          label="Genome Assembly"
          placeholder="Select genome assembly"
          class="flex-grow mr-2"
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

      <div class="flex w-full pb-6">
        <va-textarea
          v-model="importNotes"
          label="Notes (Optional)"
          placeholder="Add any additional notes about this import"
          class="flex-grow"
        />
      </div>
    </template>

    <template #step-content-3>
      <ImportInfo
        v-model:populated-dataset-name="populatedDatasetName"
        :dataset="dataset"
        :import-dir="selectedFile"
        :dataset-type="selectedDatasetType?.value"
        :file-type="selectedFileType"
        :genome-type="selectedGenomeType?.value || selectedGenomeType"
        :genome-value="selectedGenomeValue"
        :project="projectSelected"
        :source-raw-data="selectedRawData"
        :source-instrument="selectedSourceInstrument"
        :import-space="searchSpace.label"
        :created-dataset-error="formErrors[STEP_KEYS.IMPORT]"
        :show-created-dataset-error="!!formErrors[STEP_KEYS.IMPORT] && !stepIsPristine"
      />
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
          class="flex-none"
          @click="onNextClick(nextStep)"
          :color="isLastStep ? 'success' : 'primary'"
          :disabled="isNextButtonDisabled"
        >
          {{ isLastStep ? 'Import' : 'Next' }}
        </va-button>
      </div>
    </template>
  </va-stepper>
  <!--  </va-inner-loading>-->

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
import DatasetSelectAutoComplete from '@/components/dataset/DatasetSelectAutoComplete.vue';
import config from '@/config';
import Constants from '@/constants';
import analysisTypeService from '@/services/analysisType';
import datasetService from '@/services/dataset';
import fileSystemService from '@/services/fs';
import instrumentService from '@/services/instrument';
import toast from '@/services/toast';
import { Icon } from '@iconify/vue';
import { watchDebounced } from '@vueuse/core';
import pm from 'picomatch';
import { VaPopover } from 'vuestic-ui';

const STEP_KEYS = {
  SELECT_DIRECTORY: 'selectDirectory',
  GENERAL_INFO: 'generalInfo',
  GENOMIC_DETAILS: 'genomicDetails',
  IMPORT: 'info',
};

const UNKNOWN_VALIDATION_ERROR = 'An unknown error occurred';
const DATASET_NAME_REQUIRED_ERROR = 'Dataset name cannot be empty';
const DATASET_NAME_HAS_SPACES_ERROR = 'Dataset name cannot contain spaces';
const DATASET_NAME_MIN_LENGTH_ERROR = 'Dataset name must have 3 or more characters.';
const NO_FILE_SELECTED_ERROR = 'A file must be selected for import';
const IMPORT_NOT_ALLOWED_ERROR = 'Selected file cannot be imported as a dataset';

const FILESYSTEM_SEARCH_SPACES = (config.filesystem_search_spaces || []).map(
  (space) => space[Object.keys(space)[0]]
);

const steps = [
  {
    key: STEP_KEYS.SELECT_DIRECTORY,
    label: 'Select Directory',
    icon: 'material-symbols:folder',
  },
  {
    key: STEP_KEYS.GENERAL_INFO,
    label: 'General Info',
    icon: 'material-symbols:info',
  },
  {
    key: STEP_KEYS.GENOMIC_DETAILS,
    label: 'Genomic Details',
    icon: 'mdi-dna',
  },
  {
    key: STEP_KEYS.IMPORT,
    label: 'Import',
    icon: 'material-symbols:play-circle',
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

const populatedDatasetName = ref('');
const datasetTypeOptions = ref(datasetTypes);
// `willImportRawData` determines whether the user will import a Raw Data or a
// Data Product. By default, the user will import a Data Product.
const willImportRawData = ref(false);
const isAssignedProject = ref(true);
const isAssignedSourceRawData = ref(true);
const submissionSuccess = ref(false);
const fileListSearchText = ref('');
const fileList = ref([]);
const dataset = ref(null);
const loadingResources = ref(false); // determines if the initial resources needed for the stepper are being fetched
const searchingFiles = ref(false);
const validatingForm = ref(false);
const isSubmissionAlertVisible = ref(false);
const submitAttempted = ref(false);
const isAssignedSourceInstrument = ref(true);
const selectedRawData = ref(null);
const datasetSearchText = ref('');
const projectSearchText = ref('');
const selectedSourceInstrument = ref(null);
const sourceInstrumentOptions = ref([]);
const selectedFileType = ref(null);
const selectedGenomeType = ref(null);
const selectedGenomeValue = ref(null);
const importNotes = ref('');
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
const searchSpace = ref(
  FILESYSTEM_SEARCH_SPACES instanceof Array && FILESYSTEM_SEARCH_SPACES.length > 0
    ? FILESYSTEM_SEARCH_SPACES[0]
    : ''
);
const step = ref(0);
const projectSelected = ref(null);
const selectedDatasetType = ref(
  datasetTypes.find((e) => e.value === config.dataset.types.DATA_PRODUCT.key)
);
// `stepPristineStates` tracks if a step's form fields are pristine (i.e. not
// touched by user) or not. Errors are only shown when a step's form fields are
// not pristine.
const stepPristineStates = ref([
  { [STEP_KEYS.SELECT_DIRECTORY]: true },
  { [STEP_KEYS.GENERAL_INFO]: true },
  { [STEP_KEYS.GENOMIC_DETAILS]: true },
  { [STEP_KEYS.IMPORT]: true },
]);
const isFileSearchAutocompleteOpen = ref(false);
const selectedFile = ref(null);
const formErrors = ref({
  [STEP_KEYS.SELECT_DIRECTORY]: null,
  [STEP_KEYS.GENERAL_INFO]: null,
  [STEP_KEYS.GENOMIC_DETAILS]: null,
  [STEP_KEYS.IMPORT]: null,
});

const loading = computed(() => {
  return loadingResources.value || searchingFiles.value || validatingForm.value;
});

const searchSpaceBasePath = computed(() => searchSpace.value.base_path);

const _searchText = computed(() => {
  return (
    (searchSpace.value.base_path.endsWith('/')
      ? searchSpace.value.base_path
      : searchSpace.value.base_path + '/') + fileListSearchText.value
  );
});

const isLastStep = computed(() => {
  return step.value === steps.length - 1;
});

const isNextButtonDisabled = computed(() => {
  return stepHasErrors.value || submissionSuccess.value || loading.value;
});

const isPreviousButtonDisabled = computed(() => {
  return step.value === 0 || submissionSuccess.value || loading.value;
});

const stepIsPristine = computed(() => {
  return !!Object.values(stepPristineStates.value[step.value])[0];
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

const availableGenomeValues = computed(() => {
  if (!selectedGenomeType.value) {
    return [];
  }

  // Extract the actual genome type key from the object
  const genomeTypeKey = selectedGenomeType.value.value || selectedGenomeType.value;
  const genomes = Constants.GENOME_TYPES[genomeTypeKey]?.genomes || [];

  return genomes;
});

const stepHasErrors = computed(() => {
  if (step.value === 0) {
    return !!formErrors.value[STEP_KEYS.SELECT_DIRECTORY];
  } else if (step.value === 1) {
    return !!formErrors.value[STEP_KEYS.GENERAL_INFO];
  } else if (step.value === 2) {
    return !!formErrors.value[STEP_KEYS.GENOMIC_DETAILS];
  } else if (step.value === 3) {
    return !!formErrors.value[STEP_KEYS.IMPORT];
  }
});

const onFileSearchAutocompleteOpen = () => {
  isFileSearchAutocompleteOpen.value = true;
  selectedFile.value = null;
};

const onFileSearchAutocompleteClose = () => {
  if (!selectedFile.value) {
    fileListSearchText.value = '';
  }
  fileList.value = [];
  isFileSearchAutocompleteOpen.value = false;
  if (validatingForm.value) {
    validatingForm.value = false;
  }
};

const resetProjectSearch = () => {
  projectSelected.value = null;
  projectSearchText.value = '';
};

const clearSelectedRawData = () => {
  selectedRawData.value = null;
  datasetSearchText.value = '';
};

const resetRawDataSearch = (val) => {
  clearSelectedRawData();
  if (!val) {
    datasetTypeOptions.value = datasetTypes;
  } else {
    datasetTypeOptions.value = datasetTypes.filter(
      (e) => e.value === config.dataset.types.DATA_PRODUCT.key
    );
    selectedDatasetType.value = datasetTypeOptions.value.find(
      (e) => e.value === config.dataset.types.DATA_PRODUCT.key
    );
    willImportRawData.value = false;
  }
};

const onRawDataSearchOpen = () => {
  selectedRawData.value = null;
};

const onRawDataSearchClose = () => {
  if (!selectedRawData.value) {
    datasetSearchText.value = '';
  }
};

const onProjectSearchOpen = () => {
  projectSelected.value = null;
};

const onProjectSearchClose = () => {
  if (!projectSelected.value) {
    projectSearchText.value = '';
  }
};

const isStepperButtonDisabled = (stepIndex) => {
  return (
    submitAttempted.value || submissionSuccess.value || step.value < stepIndex || loading.value
  );
};

const resetFormErrors = () => {
  formErrors.value = {
    [STEP_KEYS.SELECT_DIRECTORY]: null,
    [STEP_KEYS.GENERAL_INFO]: null,
    [STEP_KEYS.GENOMIC_DETAILS]: null,
    [STEP_KEYS.IMPORT]: null,
  };
};

const setFormErrors = async () => {
  resetFormErrors();

  if (step.value === 0) {
    if (!selectedFile.value) {
      formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = NO_FILE_SELECTED_ERROR;
      return;
    }
    // check if the selected file is allowed to be imported as a dataset
    const restricted_dataset_paths = getRestrictedImportPaths();
    const origin_path_is_restricted = selectedFile.value
      ? restricted_dataset_paths.some((pattern) => {
          const _path = selectedFile.value.path;
          let isMatch = pm(pattern);
          const matches = isMatch(_path, pattern);
          return matches.isMatch;
        })
      : false;
    if (origin_path_is_restricted) {
      formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = IMPORT_NOT_ALLOWED_ERROR;
      return;
    } else {
      formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = null;
    }
  }

  if (step.value === 1) {
    if (
      (isAssignedSourceRawData.value && !selectedRawData.value) ||
      (isAssignedProject.value && !projectSelected.value) ||
      (isAssignedSourceInstrument.value && !selectedSourceInstrument.value)
    ) {
      formErrors.value[STEP_KEYS.GENERAL_INFO] = true;
    }
  }

  if (step.value === 2) {
    // Genomic details step - all fields are optional
    formErrors.value[STEP_KEYS.GENOMIC_DETAILS] = null;
  }

  if (step.value === 3) {
    const { isNameValid: datasetNameIsValid, error } = await validateDatasetName();
    if (datasetNameIsValid) {
      formErrors.value[STEP_KEYS.IMPORT] = null;
    } else {
      formErrors.value[STEP_KEYS.IMPORT] = error;
    }
  }
};

// determines if a dataset named `value` already exists
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
          type: selectedDatasetType.value['value'],
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

const validateDatasetName = async () => {
  if (!populatedDatasetName.value) {
    return { isNameValid: false, error: DATASET_NAME_REQUIRED_ERROR };
  } else if (populatedDatasetName.value.length < 3) {
    return { isNameValid: false, error: DATASET_NAME_MIN_LENGTH_ERROR };
  } else if (populatedDatasetName.value.indexOf(' ') > -1) {
    return { isNameValid: false, error: DATASET_NAME_HAS_SPACES_ERROR };
  }

  validatingForm.value = true;
  return validateIfExists(populatedDatasetName.value)
    .then((res) => {
      const datasetExistsError = (datasetType) => {
        const datasetTypeLabel = datasetTypes.find((type) => type.value === datasetType).label;
        return `A ${datasetTypeLabel} with this name already exists.`;
      };
      return {
        isNameValid: !res,
        error: res && datasetExistsError(selectedDatasetType.value['value']),
      };
    })
    .catch(() => {
      return { isNameValid: false, error: UNKNOWN_VALIDATION_ERROR };
    })
    .finally(() => {
      validatingForm.value = false;
    });
};

const resetSearch = () => {
  selectedFile.value = null;
  fileListSearchText.value = '';
  setRetrievedFiles([]);
  formErrors.value[STEP_KEYS.SELECT_DIRECTORY] = null;
  if (validatingForm.value) {
    validatingForm.value = false;
  }
};

const searchFiles = async () => {
  fileSystemService
    .getPathFiles({
      path: _searchText.value,
      dirs_only: true,
      search_space: searchSpace.value.key,
    })
    .then((response) => {
      setRetrievedFiles(response.data);
    })
    .catch((err) => {
      // console.error(err);
      if (err.response.status === 403 || err.response.status === 404) {
        setRetrievedFiles([]);
      } else {
        toast.error('Error fetching files');
      }
    })
    .finally(() => {
      searchingFiles.value = false;
    });
};

const setRetrievedFiles = (files) => {
  fileList.value = files;
};

const preImport = () => {
  if (!dataset.value) {
    return datasetService.create_dataset({
      name: populatedDatasetName.value,
      type: selectedDatasetType.value?.value || config.dataset.types.DATA_PRODUCT.key,
      origin_path: selectedFile.value.path,
      import_space: searchSpace.value.key,
      project_id: projectSelected.value?.id,
      src_instrument_id: selectedSourceInstrument.value?.id,
      src_dataset_id: selectedRawData.value?.id,
      create_method: Constants.DATASET_CREATE_METHODS.IMPORT,
      // Genomic details
      file_type: selectedFileType.value || null,
      genome_type: selectedGenomeType.value?.value || selectedGenomeType.value || null,
      genome_value: selectedGenomeValue.value || null,
      import_notes: importNotes.value || null,
    });
  } else {
    return Promise.resolve({ data: dataset.value });
  }
};

const initiateImport = async () => {
  return datasetService
    .initiate_workflow_on_dataset({
      dataset_id: dataset.value.id,
      workflow: 'integrated',
    })
    .then(() => {
      toast.success('Initiated dataset import');
      submissionSuccess.value = true;
    })
    .catch((err) => {
      toast.error('Failed to initiate import');
      console.error(err);
      submissionSuccess.value = false;
    });
};

const onSubmit = async () => {
  if (!selectedFile.value) {
    await setFormErrors();
    return Promise.reject();
  }

  submitAttempted.value = true;

  return new Promise((resolve, reject) => {
    preImport()
      .then(async (res) => {
        dataset.value = res.data;
        return Promise.resolve();
      })
      .catch((err) => {
        // handle 409 error when dataset already exists
        if (err.response.status === 409) {
          toast.error(`A ${selectedDatasetType.value['label']} with this name already exists.`);
          // TODO
          return Promise.reject();
        }
        return Promise.reject(err);
      })
      .then(() => {
        return initiateImport();
      })
      .catch((err) => {
        toast.error('Failed to initiate import');
        // console.error(err);
        reject(err);
      });
  });
};

const getRestrictedImportPaths = () => {
  return config.restricted_import_dirs[searchSpace.value.key].paths.split(',');
};

const onNextClick = (nextStep) => {
  if (isLastStep.value) {
    onSubmit();
  } else {
    nextStep();
  }
};

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
    selectedFile,
    fileListSearchText,
    isFileSearchAutocompleteOpen,
    searchSpace,
    selectedFileType,
    selectedGenomeType,
    selectedGenomeValue,
    importNotes,
  ],
  async (newVals, oldVals) => {
    // mark step's form fields as not pristine, for fields' errors to be shown
    const stepKey = Object.keys(stepPristineStates.value[step.value])[0];
    if (stepKey === STEP_KEYS.IMPORT) {
      // `1` corresponds to `populatedDatasetName`
      stepPristineStates.value[step.value][stepKey] = !oldVals[1] && newVals[1];
    } else {
      stepPristineStates.value[step.value][stepKey] = false;
    }

    await setFormErrors();
  }
);

// separate watcher for when step changes, since we don't want to mark the form
// fields as not pristine upon step changes
watch(step, async () => {
  if (step.value !== 3) {
    // step 4 (index 3) is the `Import` step
    await setFormErrors();
  }
});

watch(selectedDatasetType, (newVal) => {
  if (newVal['value'] === config.dataset.types.RAW_DATA.key) {
    isAssignedSourceRawData.value = false;
    clearSelectedRawData();
    willImportRawData.value = true;
  } else {
    willImportRawData.value = false;
  }
});

// Clear genome value when genome type changes
watch(selectedGenomeType, () => {
  selectedGenomeValue.value = null;
});

// Set loading to true when FileListAutoComplete is either opened or typed into.
// The actual search begins after a delay, but a loading indicator should be
// shown before the search begins.
watch([isFileSearchAutocompleteOpen, fileListSearchText], () => {
  if (isFileSearchAutocompleteOpen.value) {
    searchingFiles.value = true;
  }
});

// Begin search once FileListAutoComplete is opened, or typed into, but
// after a delay.
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

watchDebounced(
  [isFileSearchAutocompleteOpen, fileListSearchText],
  () => {
    if (isFileSearchAutocompleteOpen.value) {
      searchFiles();
    }
  },
  { debounce: 1000, maxWait: 3000 }
);

onMounted(async () => {
  await setFormErrors();
  loadAnalysisTypes();
});

onMounted(() => {
  loadingResources.value = true;
  instrumentService
    .getAll()
    .then((res) => {
      sourceInstrumentOptions.value = res.data;
    })
    .catch((err) => {
      toast.error('Failed to load resources');
      console.error(err);
    })
    .finally(() => {
      loadingResources.value = false;
    });
});
</script>

<style lang="scss">
.import-stepper {
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
}
</style>
