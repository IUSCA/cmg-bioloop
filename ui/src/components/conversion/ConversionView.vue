<template>
  <va-inner-loading :loading="loading">
    <!-- Content -->
    <div class="flex flex-col gap-3">
      <!-- Conversion Info Card -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <!-- Conversion Info -->
        <va-card class="min-w-0">
          <va-card-title>
            <div class="flex flex-nowrap items-center w-full">
              <span class="flex-auto text-lg"> Conversion Info </span>
            </div>
          </va-card-title>
          <va-card-content>
            <ConversionDetails :conversion="conversion"></ConversionDetails>
          </va-card-content>
        </va-card>

        <!-- Run Info -->
        <va-card class="flex flex-col min-w-0">
            <va-card-title>
              <span class="text-lg"> Run Info </span>
            </va-card-title>
            <va-card-content class="flex-1 flex flex-col min-w-0">
              <div class="va-table-responsive min-w-0">
                <table class="va-table">
                  <tbody>
                    <!-- Output Directory -->
                    <tr v-if="conversionOutputDir">
                      <td>Output Directory</td>
                      <td>
                        <CopyText
                          :text="getConversionOutputDir(conversion)"
                        />
                      </td>
                    </tr>

                    <!-- Reports -->
                    <tr>
                      <td>Reports</td>
                      <td>
                        <va-button
                          preset="secondary"
                          icon="open_in_new"
                          size="small"
                          @click="openReports"
                        >
                          View Reports
                        </va-button>
                      </td>
                    </tr>

                    <!-- Logs -->
                    <tr v-if="logs.length > 0">
                      <td>Logs</td>
                      <td>
                        <div class="flex items-start gap-2">
                          <div
                            class="bg-gray-100 dark:bg-gray-800 px-3 py-2 rounded text-sm overflow-x-auto overflow-y-auto max-w-md"
                            style="min-height: 150px; max-height: 400px;"
                          >
                            <pre class="whitespace-pre">{{ formattedLogs }}</pre>
                          </div>
                          <div class="flex flex-col gap-5">
                            <CopyButton
                              :text="formattedLogs"
                              preset="plain"
                              class="flex-none"
                            />
                            <va-popover message="Expand" placement="top">
                              <va-button
                                preset="plain"
                                icon="open_in_full"
                                size="small"
                                @click="openLogsModal"
                                class="flex-none"
                              />
                            </va-popover>
                          </div>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </va-card-content>
        </va-card>
      </div>

      <!-- Derived Datasets Card -->
      <va-card>
        <va-card-title>
          <div class="flex flex-nowrap items-center w-full">
            <span class="flex-auto text-lg"> Derived Datasets </span>
          </div>
        </va-card-title>
        <va-card-content>
          <ConversionDerivedDatasets :conversion-id="conversion?.id" />
        </va-card-content>
      </va-card>

      <!-- Workflows -->
      <div v-if="conversion?.workflow_id">
        <span class="flex text-xl my-2 font-bold">WORKFLOW</span>
        <div v-if="workflow && Object.keys(workflow).length > 0" class="space-y-2">
          <Collapsible v-model="workflow.collapse_model">
            <template #header-content>
              <WorkflowCompact :workflow="workflow" />
            </template>

            <div>
              <Workflow
                :workflow="workflow"
                @update="fetch_conversion(true)"
              ></Workflow>
            </div>
          </Collapsible>
        </div>
        <div
          v-else
          class="text-center bg-slate-200 dark:bg-slate-800 py-2 rounded shadow"
        >
          <i-mdi-card-remove-outline class="inline-block text-4xl pr-3" />
          <span class="text-lg">
            Loading workflow...
          </span>
        </div>
      </div>
    </div>
  </va-inner-loading>

  <!-- Logs Modal -->
  <va-modal v-model="showLogsModal" size="large" title="Conversion Logs">
    <div class="flex flex-row gap-3">
      <CopyButton
        :text="formattedLogs"
        preset="secondary"
        class="items-baseline"
      />
      <div
        class="bg-gray-100 dark:bg-gray-800 p-4 rounded text-sm max-h-96 overflow-auto"
      >
        <pre class="whitespace-pre">{{ formattedLogs }}</pre>
      </div>
    </div>
  </va-modal>
</template>

<script setup>
import config from "@/config";
import { getConversionOutputDir } from "@/services/conversion";
import conversionApiService from "@/services/conversion/api";
import toast from "@/services/toast";
import workflowService from "@/services/workflow";

const props = defineProps({ conversionId: String });

const conversion = ref({});
const workflow = ref({});
const logs = ref([]);
const loading = ref(false);
const showLogsModal = ref(false);

function openLogsModal() {
  showLogsModal.value = true;
}

function openReports() {
  console.log("openReports");

  const conversionId = props.conversionId;
  console.log("conversionId", conversionId);

  console.log("will call getReports");
  conversionApiService.getReports(conversionId)
    .then((res) => {
      console.log("res", res);
      // index_url already has the token appended
      const indexUrlWithToken = res.data.index_url;
      console.log("indexUrlWithToken", indexUrlWithToken);
      
      // Get secure_download base URL from environment variable
      const secureDownloadBaseUrl = import.meta.env.VITE_UPLOAD_API_BASE_PATH;
      console.log("secureDownloadBaseUrl (from VITE_UPLOAD_API_BASE_PATH):", secureDownloadBaseUrl);
      
      // Construct full URL (token already in index_url)
      const fullUrl = `${secureDownloadBaseUrl}${indexUrlWithToken}`;
      console.log("Opening:", fullUrl);
      window.open(fullUrl, "_blank");
    })
    .catch((err) => {
      console.error("error", err);
      toast.error("Could not load reports");
    })
    .finally(() => {
      console.log("finally");
    });
}

function fetch_conversion(show_loading = false) {
  loading.value = show_loading;
  console.log("fetching conversion", props.conversionId);

  // Fetch conversion details and logs in parallel
  Promise.all([
    conversionApiService.get(props.conversionId, {
      include_dataset: true,
      include_derived_datasets: true,
      include_definition: true,
    }),
    conversionApiService.getLogs(props.conversionId),
  ])
    .then(([conversionRes, logsRes]) => {
      conversion.value = conversionRes.data;
      logs.value = logsRes.data;
      
      // Fetch workflow if workflow_id exists
      if (conversion.value.workflow_id) {
        fetch_workflow(conversion.value.workflow_id);
      }
    })
    .catch((err) => {
      console.error(err);
      if (err?.response?.status == 404)
        toast.error("Could not find the conversion");
      else toast.error("Could not fetch conversion");
    })
    .finally(() => {
      loading.value = false;
    });
}

function fetch_workflow(workflow_id) {
  workflowService.getById(workflow_id, true, true)
    .then((res) => {
      const _workflow = res.data;
      // Keep collapse_model state if it exists
      _workflow.collapse_model = 
        !workflowService.is_workflow_done(_workflow) ||
        workflow.value?.collapse_model ||
        false;
      workflow.value = _workflow;
    })
    .catch((err) => {
      console.error("Error fetching workflow:", err);
    });
}

const formattedLogs = computed(() => {
  return logs.value.map((log) => `${log.message.trim()}`).join("\n");
});


const conversionOutputDir = computed(() => {
  return (
    Object.entries(conversion.value) > 0 &&
    getConversionOutputDir(conversion.value)
  );
});

const active_wf = computed(() => {
  if (!workflow.value || Object.keys(workflow.value).length === 0) {
    return false;
  }
  return !workflowService.is_workflow_done(workflow.value);
});

const polling_interval = computed(() => {
  return active_wf.value ? config.dataset_polling_interval : null;
});

onMounted(() => {
  console.log("ConversionView onMounted", props.conversionId);
  fetch_conversion(true);
});

// Set up polling for active workflows
const poll = useIntervalFn(fetch_conversion, polling_interval);

watch(active_wf, (newVal, _) => {
  if (newVal) {
    poll.resume();
  } else {
    poll.pause();
  }
});
</script>

<style lang="scss" scoped>
div.va-table-responsive {
  overflow: auto;

  table.va-table {
    width: auto;
  }

  // first column fixed width to match Conversion Info card
  td:first-child {
    width: 135px;
    min-width: 135px;
    white-space: nowrap;
  }
  
  // second column should shrink to fit content
  td:last-child {
    width: 1%;
  }
}
</style>
