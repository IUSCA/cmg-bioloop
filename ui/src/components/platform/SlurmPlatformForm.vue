<template>
  <div class="flex flex-col gap-4">
    <!-- Mode Selection -->
    <div class="flex items-center gap-3">
      <va-checkbox
        v-model="useDirectives"
        @update:modelValue="onModeChange"
      />
      <span class="font-semibold">Use SLURM Directives (instead of script upload)</span>
    </div>

    <!-- SLURM Script Upload (when not using directives) -->
    <div v-if="!useDirectives">
      <va-file-upload
        v-model="files"
        dropzone
        dropZoneText="Select SLURM script(s)"
        label="SLURM script"
        preset="bordered"
        class="w-full"
      />
    </div>

    <!-- SLURM Directives Form (when using directives) -->
    <SlurmDirectivesForm 
      v-if="useDirectives"
      v-model:directives="directives" 
    />
  </div>
</template>

<script setup>
import SlurmDirectivesForm from './SlurmDirectivesForm.vue';

const metadata = defineModel("metadata");

// Local reactive refs
const files = ref([]);
const directives = ref({});
const useDirectives = ref(false);

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

    // IMPORTANT: Replace the entire object to trigger reactivity up the chain
    // Using spread operator creates a new object reference, which Vue detects
    metadata.value = {
      ...metadata.value,
      files: newFiles,
    };

    console.log("Updated metadata.value:", metadata.value);
    console.log("-------------- SlurmPlatformForm ------------------");
  },
  { deep: true },
);

// Handle mode change
function onModeChange(useDirectivesMode) {
  useDirectives.value = useDirectivesMode;
  
  // Clear the opposite mode's data
  if (useDirectivesMode) {
    files.value = [];
  } else {
    directives.value = {};
  }
  
  // Update metadata with mode information
  metadata.value = {
    ...metadata.value,
    use_directives: useDirectivesMode,
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
    };

    console.log("Updated metadata.value with execution_config:", metadata.value);
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
