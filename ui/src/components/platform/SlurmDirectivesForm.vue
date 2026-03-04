<template>
  <va-card class="mt-5">
    <va-card-title>
      <div class="flex items-center justify-between w-full">
        <va-select
          v-model="selectedCommand"
          :options="commandOptions"
          :text-by="(option) => option.label"
          :value-by="(option) => option.value"
          placeholder="Select command"
          preset="bordered"
          class="w-48"
          label="Slurm Directive"
          @update:modelValue="onCommandChange"
        />
      </div>
    </va-card-title>
    <va-card-content>
      <div class="flex flex-col gap-4">
        <!-- SLURM Directives Form -->
        <div v-if="currentDirectives" class="flex flex-col gap-3">
          <div
            v-for="(directive, key) in currentDirectives"
            :key="key"
            class="flex items-center gap-3"
          >
            <div class="flex flex-col w-[200px]">
              <span class="font-semibold">
                {{ directive.name }}
                <span v-if="directive.required" class="text-red-500">*</span>
              </span>
              <span class="text-sm text-gray-500 mt-[-3px]">
                {{ directive.description }}
              </span>
            </div>

            <div class="flex-1">
              <!-- Number input -->
              <va-input
                v-if="directive.type === 'number'"
                v-model.number="directiveValues[key]"
                type="number"
                :min="directive.min"
                :max="directive.max"
                :placeholder="
                  directive.placeholder || directive.default?.toString()
                "
                preset="bordered"
                class="w-full"
              />

              <!-- String input -->
              <va-input
                v-else-if="
                  directive.type === 'string' || directive.type === 'email'
                "
                v-model="directiveValues[key]"
                :type="directive.type === 'email' ? 'email' : 'text'"
                :placeholder="directive.placeholder"
                preset="bordered"
                class="w-full"
              />

              <!-- Select dropdown -->
              <va-select
                v-else-if="directive.type === 'select'"
                v-model="directiveValues[key]"
                :options="directive.options"
                :placeholder="directive.placeholder || 'Select option'"
                searchable
                preset="bordered"
                class="w-full"
                clearable
              />

              <!-- Multi-select -->
              <va-select
                v-else-if="directive.type === 'multiselect'"
                v-model="directiveValues[key]"
                :options="directive.options"
                :placeholder="directive.placeholder || 'Select options'"
                searchable
                multiple
                preset="bordered"
                class="w-full"
                clearable
              />
            </div>
          </div>
        </div>

        <!-- No directives message -->
        <div v-else class="text-center text-gray-500 py-4">
          <i-mdi-information-outline class="inline-block text-2xl mr-2" />
          No SLURM directives available for the selected command.
        </div>
      </div>
    </va-card-content>
  </va-card>
</template>

<script setup>
import {
  getSlurmDirectives,
  validateSlurmDirective,
} from "@/config/slurmDirectives";

const directives = defineModel("directives");

// Reactive data
const selectedCommand = ref("sbatch");
const currentDirectives = ref(null);
const directiveValues = ref({});

// Command options
const commandOptions = [
  { value: "sbatch", label: "sbatch (Batch Job)" },
  { value: "srun", label: "srun (Interactive Job)" },
];

// Initialize directives based on selected command
function initializeDirectives() {
  currentDirectives.value = getSlurmDirectives(selectedCommand.value);

  // Initialize directive values with defaults
  const newDirectives = {};
  Object.entries(currentDirectives.value).forEach(([key, directive]) => {
    if (directive.default !== undefined) {
      newDirectives[key] = directive.default;
    }
  });

  directiveValues.value = newDirectives;
  updateParentDirectives();
}

// Handle command change
function onCommandChange(newCommand) {
  selectedCommand.value = newCommand;
  initializeDirectives();
}

// Update parent directives
function updateParentDirectives() {
  // Filter out empty values for cleaner data
  const filteredDirectives = {};
  Object.entries(directiveValues.value).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== "") {
      if (Array.isArray(value) && value.length > 0) {
        filteredDirectives[key] = value;
      } else if (!Array.isArray(value)) {
        filteredDirectives[key] = value;
      }
    }
  });

  directives.value = filteredDirectives;
}

// Watch for changes in directive values and validate
watch(
  directiveValues,
  (newValue) => {
    // Validate all directive values
    const validationErrors = {};
    let hasErrors = false;

    Object.entries(newValue).forEach(([key, value]) => {
      if (currentDirectives.value && currentDirectives.value[key]) {
        const validation = validateSlurmDirective(
          currentDirectives.value[key],
          value,
        );
        if (!validation.valid) {
          validationErrors[key] = validation.error;
          hasErrors = true;
        }
      }
    });

    // Update parent with validated directives
    if (!hasErrors) {
      updateParentDirectives();
    }
  },
  { deep: true },
);

// Watch for changes from parent
watch(
  directives,
  (newValue) => {
    if (newValue && typeof newValue === "object") {
      // Update local directive values with values from parent
      Object.entries(newValue).forEach(([key, value]) => {
        if (currentDirectives.value && currentDirectives.value[key]) {
          directiveValues.value[key] = value;
        }
      });
    }
  },
  { deep: true },
);

// Initialize on mount
onMounted(() => {
  initializeDirectives();
});
</script>

<style scoped lang="scss">
// Match the styling from Argument.vue
:deep(.va-input-wrapper__field) {
  --va-input-wrapper-min-height: 28px;
}
</style>
