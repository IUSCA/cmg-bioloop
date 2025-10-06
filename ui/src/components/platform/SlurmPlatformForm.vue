<template>
  <div>
    <va-file-upload
      v-model="files"
      dropzone
      dropZoneText="Select SLURM script(s)"
      label="SLURM script"
      preset="bordered"
      class="w-full"
    />

    <!-- Debug display -->
    <!-- <div v-if="files && files.length > 0" class="mt-2 text-sm text-gray-600">
      Selected: {{ files.length }} file(s)
    </div> -->
  </div>
</template>

<script setup>
const metadata = defineModel("metadata");

// Local reactive ref for files
const files = ref([]);

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
