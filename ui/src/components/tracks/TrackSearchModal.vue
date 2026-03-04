<template>
  <va-modal
    v-model="visible"
    fixed-layout
    hide-default-actions
    size="small"
    title="Track Search"
    @close="onModalClose"
  >
    <div class="w-full">
      <va-form class="flex flex-col gap-3 md:gap-5">
        <!-- name filter -->
        <va-input
          label="Name"
          v-model="form.name"
          placeholder="Enter a term that matches any part of the track name"
        />

        <!-- project filter -->
        <ProjectAsyncAutoComplete
          label="Project"
          :search-term="form.project_search_term"
          @update:search-term="form.project_search_term = $event"
          @update:selected="onProjectSelected"
          @clear="onProjectCleared"
        />

        <!-- dataset filter -->
        <DatasetSelectAutoComplete
          label="Dataset"
          :search-term="form.dataset_search_term"
          :project-id="form.project?.id ?? null"
          @update:search-term="form.dataset_search_term = $event"
          @update:selected="onDatasetSelected"
          @clear="onDatasetCleared"
        />

        <!-- file_type filter -->
        <va-select
          v-model="form.file_type"
          :options="fileTypeOptions"
          text-by="label"
          value-by="value"
          label="File Type"
          placeholder="Choose a file type"
          :loading="loadingFileTypes"
        >
          <template #prependInner>
            <Icon icon="mdi:file-document" class="text-xl" />
          </template>
        </va-select>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-3">
          <!-- genome_type filter -->
          <va-select
            v-model="form.genome_type"
            :options="genomeTypeOptions"
            text-by="label"
            value-by="value"
            label="Genome Type"
            placeholder="Choose a genome type"
            @update:model-value="onGenomeTypeChange"
          >
            <template #prependInner>
              <Icon icon="mdi:dna" class="text-xl" />
            </template>
          </va-select>

          <!-- genome_value filter -->
          <va-select
            v-model="form.genome_value"
            :options="genomeValueOptions"
            text-by="label"
            value-by="value"
            label="Genome Value"
            placeholder="Choose a genome value"
          >
            <template #prependInner>
              <Icon icon="mdi:dna" class="text-xl" />
            </template>
          </va-select>
        </div>
      </va-form>
    </div>

    <template #footer>
      <div class="flex gap-2 justify-end">
        <va-button preset="secondary" @click="resetForm"> Reset </va-button>
        <va-button preset="primary" @click="applyFilters">
          Apply Filters
        </va-button>
      </div>
    </template>
  </va-modal>
</template>

<script setup>
import ProjectAsyncAutoComplete from "@/components/project/ProjectAsyncAutoComplete.vue";
import DatasetSelectAutoComplete from "@/components/dataset/DatasetSelectAutoComplete.vue";
import analysisTypeService from "@/services/analysisType";
import constants from "@/constants";

const visible = ref(false);

const defaultForm = () => ({
  name: "",
  project: null,
  project_search_term: "",
  dataset: null,
  dataset_search_term: "",
  file_type: null,
  genome_type: null,
  genome_value: null,
});

const form = ref(defaultForm());

// Analysis types loaded from API
const analysisTypes = ref([]);
const loadingFileTypes = ref(false);

const fileTypeOptions = computed(() => {
  const options = [{ label: "All", value: null }];
  analysisTypes.value.forEach((at) => {
    if (at.name) {
      options.push({ label: at.name, value: at.name });
    }
  });
  return options;
});

// Genome type options from constants
const genomeTypeOptions = computed(() => {
  const options = [{ label: "All", value: null }];
  Object.entries(constants.GENOME_TYPES).forEach(([key, val]) => {
    options.push({ label: val.label || key, value: key });
  });
  return options;
});

// Genome value options based on selected genome type
const genomeValueOptions = computed(() => {
  const options = [{ label: "All", value: null }];
  if (
    form.value.genome_type &&
    constants.GENOME_TYPES[form.value.genome_type]
  ) {
    constants.GENOME_TYPES[form.value.genome_type].genomes.forEach((genome) => {
      options.push({ label: genome, value: genome });
    });
  }
  return options;
});

function onGenomeTypeChange() {
  // Reset genome value when type changes
  form.value.genome_value = null;
}

function onProjectSelected(project) {
  form.value.project = project;
  // Clear dataset when project changes
  form.value.dataset = null;
  form.value.dataset_search_term = "";
}

function onProjectCleared() {
  form.value.project = null;
  // Keep dataset value intact when project is cleared (user may want to filter by dataset only)
}

function onDatasetSelected(dataset) {
  form.value.dataset = dataset;
  // Do NOT clear project when dataset changes
}

function onDatasetCleared() {
  form.value.dataset = null;
}

async function loadAnalysisTypes() {
  loadingFileTypes.value = true;
  try {
    const res = await analysisTypeService.getAll();
    analysisTypes.value = res.data || [];
  } catch (err) {
    console.error("Failed to load analysis types", err);
  } finally {
    loadingFileTypes.value = false;
  }
}

function resetForm() {
  form.value = defaultForm();
}

function onModalClose() {
  resetForm();
}

const emit = defineEmits(["search"]);

function applyFilters() {
  const filters = {
    name: form.value.name || null,
    // If dataset is specified, omit project (dataset is more specific)
    project_id: form.value.dataset ? null : (form.value.project?.id ?? null),
    dataset_id: form.value.dataset?.id ?? null,
    // Store project/dataset objects for display in filter chips
    project: form.value.project || null,
    dataset: form.value.dataset || null,
    file_type: form.value.file_type,
    genome_type: form.value.genome_type,
    genome_value: form.value.genome_value,
  };

  visible.value = false;
  emit("search", filters);
  resetForm();
}

// Expose show method
defineExpose({
  show: () => {
    visible.value = true;
  },
});

onMounted(() => {
  loadAnalysisTypes();
});
</script>
