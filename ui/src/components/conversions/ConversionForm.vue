<template>
  <va-stepper v-model="currentStep" :steps="visibleSteps" :controls-hidden="visibleSteps.length <= 1">
    <!-- Step 1: Pipeline Selection -->
    <template #step-content-0>
      <!-- Dataset selection (shown above pipeline fields when allowRawDataSelection is true) -->
      <div v-if="allowRawDataSelection" class="mb-10">
        <DatasetSelectAutoComplete
          v-model:selected="selectedRawDataset"
          v-model:search-term="datasetSearchTerm"
          dataset-type="RAW_DATA"
          label="Dataset"
          @clear="selectedRawDataset = null"
        />
      </div>

      <ConversionDefinitionSelect v-model="definition" class="w-full" />

      <div v-if="definition" class="mt-3">
        <div class="space-y-2 pl-3">
          <!-- description -->
          <div>
            <span class="va-text-secondary">
              {{ definition.description }}
            </span>
          </div>

          <!-- output and log directories -->
          <div v-if="definition.output_directory">
            <span class=""> Output Directory : </span>
            <span class="truncate">
              {{ definition.output_directory }}
            </span>
          </div>

          <div v-if="definition.logs_directory">
            <span class=""> Logs Directory : </span>
            <span class="truncate">
              {{ definition.logs_directory }}
            </span>
          </div>

          <div class="flex">
            <!-- dataset types -->
            <div>
              <va-chip
                v-for="dt in definition.dataset_types"
                :key="dt"
                size="small"
                square
              >
                <span class="uppercase"> {{ dt }} </span>
              </va-chip>
            </div>

            <!-- tags -->
            <div class="flex gap-2 items-center ml-auto">
              <va-chip
                v-for="tag in definition.tags"
                :key="tag"
                size="small"
                color="info"
              >
                <span class="uppercase"> {{ tag }} </span>
              </va-chip>
            </div>
          </div>
        </div>

        <!-- Program Arguments -->
        <va-card class="mt-5">
          <va-card-content>
            <ConversionProgramForm
              :program="definition.program"
              v-model:argValues="argValues"
            />
          </va-card-content>
        </va-card>
      </div>

      <div v-else>
        <div class="flex flex-col justify-center items-center h-40">
          <span class="text-gray-500">
            Select a conversion definition to see associated program and
            arguments.
          </span>
        </div>
      </div>
    </template>

    <!-- Step 2: Execution Platform (slot index 1 when visible; va-stepper won't request this slot if the step is hidden) -->
    <template #step-content-1>
      <div class="space-y-4">
        <div class="flex items-center gap-3">
          <va-checkbox
            v-model="usePlatform"
            label="Use external platform for execution (SLURM, K8s, etc.)"
          />
        </div>

        <!-- Execution Platform Form -->
        <ExecutionPlatformForm
          v-if="usePlatform"
          v-model:platform="platform"
          v-model:metadata="platformMetadata"
        />

        <!-- Message when not using platform -->
        <div v-else class="flex flex-col justify-center items-center h-40">
          <span class="text-gray-500">
            Job will run on the application host.
          </span>
        </div>
      </div>
    </template>
  </va-stepper>
</template>

<script setup>
import { useAuthStore } from "@/stores/auth";

const auth = useAuthStore();

const props = defineProps({
  allowRawDataSelection: { type: Boolean, default: false },
});

const definition = defineModel("definition");
const argValues = defineModel("argValues");
const executionMetadata = defineModel("executionMetadata");
const selectedRawDataset = defineModel("selectedRawDataset", { default: null });
const datasetSearchTerm = defineModel("datasetSearchTerm", { default: "" });

const currentStep = ref(0);
const usePlatform = ref(false);

const isPlatformBasedExecutionEnabled = computed(() =>
  auth.isFeatureEnabled("platformBasedExecution"),
);

// All possible steps. Each step may declare a `hidden` condition; steps where
// hidden === true are excluded from visibleSteps and the stepper never renders them.
const allSteps = computed(() => [
  {
    label: "Pipeline & Arguments",
    icon: "settings",
    hidden: false,
  },
  {
    label: "Execution Platform",
    icon: "cloud",
    hidden: !isPlatformBasedExecutionEnabled.value,
  },
]);

// Only non-hidden steps are passed to va-stepper. Slot indices (#step-content-N)
// correspond to position in this array, so hidden steps do not shift visible ones.
const visibleSteps = computed(() =>
  allSteps.value
    .filter((s) => !s.hidden)
    .map(({ hidden: _hidden, ...rest }) => rest),
);

// Computed property for nested v-model binding
const platform = computed({
  get: () => executionMetadata.value?.platform,
  set: (value) => {
    executionMetadata.value = {
      ...executionMetadata.value,
      platform: value,
    };
  },
});

// Computed property for nested v-model binding
const platformMetadata = computed({
  get: () => executionMetadata.value?.metadata || {},
  set: (value) => {
    executionMetadata.value = {
      ...executionMetadata.value,
      metadata: value,
    };
  },
});

watch(usePlatform, () => {
  executionMetadata.value = {};
});

watch(isPlatformBasedExecutionEnabled, (enabled) => {
  if (!enabled) {
    usePlatform.value = false;
    executionMetadata.value = {};
    currentStep.value = 0;
  }
});

</script>
