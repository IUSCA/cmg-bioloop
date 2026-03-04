<template>
  <div class="flex flex-col gap-4">
    <!-- Mode Selection (Radio buttons side-by-side) -->
    <div class="flex items-center gap-6">
      <va-radio
        v-model="slurmMode"
        option="script"
        label="Use SLURM Script"
        @update:modelValue="onModeChange"
      />
      <va-radio
        v-model="slurmMode"
        option="directives"
        label="Use SLURM Directives"
        @update:modelValue="onModeChange"
      />
    </div>

    <!-- SLURM Script Upload (when using script mode) -->
    <div v-if="slurmMode === 'script'">
      <va-file-upload
        v-model="files"
        dropzone
        dropZoneText="Select SLURM script"
        label="SLURM script"
        preset="bordered"
        class="w-full"
      />
    </div>

    <!-- SLURM Directives Form (when using directives mode) -->
    <SlurmDirectivesForm
      v-if="slurmMode === 'directives'"
      v-model:directives="directives"
    />
  </div>
</template>

<script setup>
import SlurmDirectivesForm from "./SlurmDirectivesForm.vue";

const metadata = defineModel("metadata");

// Local reactive refs
const files = ref([]);
const directives = ref({});
const slurmMode = ref("script"); // Default to script mode

// Initialize metadata if needed
onMounted(() => {
  console.log("-------------- SlurmPlatformForm ------------------");
  console.log("metadata ON MOUNTED, value:", metadata.value);
  if (!metadata.value) {
    console.log("metadata ON MOUNTED, value is null, setting to empty object");
    metadata.value = {};
  }
  console.log("metadata ON MOUNTED, value:", metadata.value);
  console.log("-------------- SlurmPlatformForm ------------------");
});

// Watch files array and update metadata
watch(
  files,
  (newFiles) => {
    console.log("-------------- SlurmPlatformForm ------------------");
    console.log("files WATCH triggered");
    console.log("Number of files:", newFiles?.length);
    console.log("Files:", newFiles);

    // Note: replace the entire object to trigger reactivity up the chain
    // using spread operator creates a new object reference, which Vue detects
    metadata.value = {
      ...metadata.value,
      files: newFiles,
      slurm_mode: "script",
    };

    console.log("Updated metadata.value:", metadata.value);
    console.log("-------------- SlurmPlatformForm ------------------");
  },
  { deep: true },
);

// Handle mode change
function onModeChange(mode) {
  slurmMode.value = mode;

  // Clear the opposite mode's data
  if (mode === "directives") {
    files.value = [];
  } else if (mode === "script") {
    directives.value = {};
  }

  // Update metadata with mode information
  metadata.value = {
    ...metadata.value,
    slurm_mode: mode,
  };
}

// Watch directives and update metadata
watch(
  directives,
  (newDirectives) => {
    console.log("-------------- SlurmPlatformForm ------------------");
    console.log("directives WATCH triggered");
    console.log("Directives:", newDirectives);

    // Update metadata with directives as execution_config
    metadata.value = {
      ...metadata.value,
      execution_config: newDirectives,
      slurm_mode: "directives",
    };

    console.log(
      "Updated metadata.value with execution_config:",
      metadata.value,
    );
    console.log("-------------- SlurmPlatformForm ------------------");
  },
  { deep: true },
);

// Watch metadata changes from parent
watch(
  metadata,
  (newValue) => {
    console.log(
      "-------------- SlurmPlatformForm (metadata from parent) ------------------",
    );
    console.log("metadata WATCH, new value:", newValue);
    console.log(
      "-------------- SlurmPlatformForm (metadata from parent) ------------------",
    );
  },
  { deep: true },
);

onMounted(() => {
  console.log("-------------- SlurmPlatformForm ------------------");
  console.log("metadata ON MOUNTED, value:", metadata.value);
  console.log("-------------- SlurmPlatformForm ------------------");
});
</script>
