<template>
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
          <!-- <span class="font-semibold mr-2"> Runs on </span> -->
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
          <!-- <span class="font-semibold mr-2"> Tags </span> -->
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

    <!-- Choose whether to use a specific Platform -->
    <div class="space-y-2 pl-3">
      <div class="flex items-center gap-3">
        <div class="flex-1">
          <va-checkbox v-model="usePlatform" label="Use platform" />
        </div>
      </div>
      <!-- Execution Platform -->
      <ExecutionPlatformForm
        v-if="usePlatform"
        v-model:platform="platform"
        v-model:metadata="platformMetadata"
      />
    </div>

    <!-- Program -->
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
        Select a conversion definition to see associated program and arguments.
      </span>
    </div>
  </div>
</template>

<script setup>
const definition = defineModel("definition");
const argValues = defineModel("argValues");
const executionMetadata = defineModel("executionMetadata");

const usePlatform = ref(false);

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

watch(
  executionMetadata,
  (newValue) => {
    console.log("-------------- ConversionForm ------------------");
    console.log("executionMetadata WATCH, new value:", newValue);
    console.log("-------------- ConversionForm ------------------");
  },
  { deep: true },
);

watch(usePlatform, (newValue) => {
  console.log("-------------- ConversionForm ------------------");
  console.log("usePlatform WATCH, new value:", newValue);
  executionMetadata.value = {};
  console.log("-------------- ConversionForm ------------------");
});

onMounted(() => {
  console.log("-------------- ConversionForm ------------------");
  console.log("executionMetadata ON MOUNTED, value:", executionMetadata.value);
  console.log("usePlatform ON MOUNTED, value:", usePlatform.value);
  console.log("-------------- ConversionForm ------------------");
});
</script>
