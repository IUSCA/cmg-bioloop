<template>
  <va-card class="mt-5">
    <va-card-title>
      <span class="text-lg">Execution Platform</span>
    </va-card-title>
    <va-card-content>
      <div class="flex flex-col gap-4">
        <!-- Platform selection -->
        <div class="flex items-center gap-3">
          <div class="flex-1">
            <va-select
              v-model="selectedPlatform"
              :options="platforms"
              label="Platform"
              :text-by="(option) => option.label"
              :track-by="(option) => option.value"
              :value-by="(option) => option.value"
              placeholder="Select execution platform"
              preset="bordered"
              class="w-full"
              clearable
            />
          </div>
          <!-- Platform-specific form -->
          <div v-if="selectedPlatform === 'SLURM'">
            <SlurmPlatformForm v-model:metadata="metadata" />
          </div>
        </div>
      </div>
    </va-card-content>
  </va-card>
</template>

<script setup>
const platform = defineModel("platform");
const metadata = defineModel("metadata");

// TODO: Fetch platforms from API
// const platformsResponse = await fetch('/api/conversions/platforms')
const platforms = ref([
  { value: "LOCAL", label: "Local (App Server)" },
  { value: "SLURM", label: "SLURM Cluster" },
  { value: "PBS", label: "PBS/Torque" },
  { value: "KUBERNETES", label: "Kubernetes" },
]);

const selectedPlatform = computed({
  get: () => platform.value,
  set: (value) => {
    console.log("-------------- ExecutionPlatformForm ------------------");
    console.log("selectedPlatform SET, old value:", platform.value);
    console.log("selectedPlatform SET, new value:", value);
    console.log("-------------- ExecutionPlatformForm ------------------");
    // Only reset metadata if platform actually changed
    if (platform.value !== value) {
      metadata.value = {};
    }
    platform.value = value;
  },
});

watch([metadata, platform], (newValues, oldValues) => {
  console.log("-------------- ExecutionPlatformForm ------------------");
  console.log("metadata WATCH, old value:", oldValues[0]);
  console.log("metadata WATCH, new value:", newValues[0]);
  console.log("platform WATCH, old value:", oldValues[1]);
  console.log("platform WATCH, new value:", newValues[1]);
  console.log("-------------- ExecutionPlatformForm ------------------");
});

onMounted(() => {
  console.log("-------------- ExecutionPlatformForm ------------------");
  console.log("metadata ON MOUNTED, value:", metadata.value);
  console.log("platform ON MOUNTED, value:", platform.value);
  console.log("-------------- ExecutionPlatformForm ------------------");
});
</script>
