<template>
  <div class="va-table-responsive">
    <table class="va-table">
      <tbody>
        <tr>
          <td>Converted Dataset</td>
          <td>
            <router-link
              v-if="props.conversion.dataset"
              :to="`/datasets/${props.conversion.dataset.id}`"
              class="va-link"
            >
              {{ props.conversion.dataset.name }}
            </router-link>
          </td>
        </tr>
        <tr>
          <td>Total Size</td>
          <td>
            <span v-if="props.conversion.dataset?.du_size">
              {{ formatBytes(props.conversion.dataset.du_size) }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Number of Files</td>
          <td>
            <span v-if="props.conversion.dataset?.num_files != null">
              {{ props.conversion.dataset.num_files }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Number of Directories</td>
          <td>
            <span v-if="props.conversion.dataset?.num_directories != null">
              {{ props.conversion.dataset.num_directories }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Program</td>
          <td>
            <div v-if="props.conversion.definition?.program">
              <div class="font-medium">
                {{ props.conversion.definition.program.name }}
              </div>
              <div
                v-if="props.conversion.definition.program.description"
                class="text-sm text-gray-600 dark:text-gray-400"
              >
                {{ props.conversion.definition.program.description }}
              </div>
            </div>
          </td>
        </tr>
        <tr>
          <td>Sample Sheet</td>
          <td>
            <div v-if="sampleSheetContent" class="flex items-start gap-2">
              <div
                class="bg-gray-100 dark:bg-gray-800 px-3 py-2 rounded text-sm overflow-x-auto overflow-y-auto max-h-32 max-w-sm"
              >
                <pre class="whitespace-pre">{{ sampleSheetContent }}</pre>
              </div>
              <div class="flex flex-col gap-5">
                <CopyButton
                  :text="sampleSheetContent"
                  preset="plain"
                  class="flex-none"
                />
                <va-popover message="Expand" placement="top">
                  <va-button
                    preset="plain"
                    icon="open_in_full"
                    size="small"
                    @click="openSampleSheetModal"
                    class="flex-none"
                  />
                </va-popover>
              </div>
            </div>
          </td>
        </tr>
        <tr>
          <td>Arguments</td>
          <td>
            <div
              v-if="
                props.conversion.argsList &&
                props.conversion.argsList.length > 0
              "
              class="flex items-start gap-2"
            >
              <code
                class="bg-gray-100 dark:bg-gray-800 px-2 py-1 rounded text-sm break-all flex-1"
              >
                {{ filteredArgumentsString }}
              </code>
              <div class="flex flex-col gap-5">
                <CopyButton
                  :text="filteredArgumentsString"
                  preset="plain"
                  class="flex-none"
                />
                <va-popover message="Expand" placement="top">
                  <va-button
                    preset="plain"
                    icon="open_in_full"
                    size="small"
                    @click="openArgumentsModal"
                    class="flex-none"
                  />
                </va-popover>
              </div>
            </div>
          </td>
        </tr>
        <tr>
          <td>Created At</td>
          <td>
            <span class="spacing-wider">
              {{ datetime.absolute(props.conversion.initiated_at) }}
            </span>
          </td>
        </tr>
        <tr>
          <td>Initiator</td>
          <td>
            <span v-if="props.conversion.initiator">
              {{ props.conversion.initiator.name }} ({{
                props.conversion.initiator.username
              }})
            </span>
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <!-- Sample Sheet Modal -->
  <va-modal v-model="showSampleSheetModal" size="large" title="Sample Sheet">
    <div class="flex flex-row gap-3">
      <div class="flex flex-none">
        <CopyButton
          :text="sampleSheetContent"
          preset="secondary"
          class="items-baseline"
        />
      </div>
      <div
        class="bg-gray-100 dark:bg-gray-800 p-4 rounded text-sm max-h-96 overflow-auto"
      >
        <pre class="whitespace-pre">{{ sampleSheetContent }}</pre>
      </div>
    </div>
  </va-modal>

  <!-- Arguments Modal -->
  <va-modal v-model="showArgumentsModal" size="large" title="Arguments">
    <div class="flex">
      <CopyButton
        :text="filteredArgumentsString"
        preset="secondary"
        class="items-baseline"
      />
      <div
        class="bg-gray-100 dark:bg-gray-800 p-4 rounded text-sm max-h-96 overflow-auto"
      >
        <code class="whitespace-pre-wrap">{{ filteredArgumentsString }}</code>
      </div>
    </div>
  </va-modal>
</template>

<script setup>
import CopyButton from "@/components/utils/buttons/CopyButton.vue";
import * as datetime from "@/services/datetime";
import { formatBytes } from "@/services/utils";
const props = defineProps({ conversion: Object });

const showSampleSheetModal = ref(false);
const showArgumentsModal = ref(false);

function openSampleSheetModal() {
  showSampleSheetModal.value = true;
}

function openArgumentsModal() {
  showArgumentsModal.value = true;
}

const sampleSheetContent = computed(() => {
  if (!props.conversion.argsList) return null;

  // Find --sample-sheet in the flat argsList and return the next element
  for (let i = 0; i < props.conversion.argsList.length - 1; i++) {
    if (props.conversion.argsList[i] === "--sample-sheet") {
      return props.conversion.argsList[i + 1];
    }
  }
  return null;
});

const filteredArgumentsString = computed(() => {
  if (!props.conversion.argsList) return "";

  // Filter out Sample Sheet arguments and join the rest
  const filteredArgs = props.conversion.argsList.filter((arg) => {
    // Check if this is a Sample Sheet argument by looking at the argument values
    const sampleSheetArg = props.conversion.argument_values?.find(
      (argVal) =>
        argVal.argument.name === "Sample Sheet" ||
        argVal.argument.name === "--sample-sheet" ||
        argVal.argument.name === "sample-sheet",
    );

    // If we found a Sample Sheet argument, exclude its value from the args list
    if (sampleSheetArg && arg === sampleSheetArg.value) {
      return false;
    }

    // Also exclude the argument name itself if it matches Sample Sheet patterns
    if (
      arg === "Sample Sheet" ||
      arg === "--sample-sheet" ||
      arg === "sample-sheet"
    ) {
      return false;
    }

    return true;
  });

  return filteredArgs.join(" ");
});
</script>

<style lang="scss" scoped>
div.va-table-responsive {
  overflow: auto;

  // first column min width
  td:first-child {
    min-width: 135px;
  }
}
</style>
