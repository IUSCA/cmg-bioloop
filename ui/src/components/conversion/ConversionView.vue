<template>
  <va-inner-loading :loading="loading">
    <!-- Content -->
    <div class="flex flex-col gap-3">
      <!-- Conversion Info Card -->
      <div class="grid gird-cols-1 lg:grid-cols-2 gap-3">
        <!-- Conversion Info -->
        <div class="">
          <va-card>
            <va-card-title>
              <div class="flex flex-nowrap items-center w-full">
                <span class="flex-auto text-lg"> Conversion Info </span>
              </div>
            </va-card-title>
            <va-card-content>
              <ConversionDetails :conversion="conversion"></ConversionDetails>
            </va-card-content>
          </va-card>
        </div>

        <div>
          <va-card>
            <va-card-title>
              <span class="text-lg"> Run Info </span>
            </va-card-title>
            <va-card-content>
              <!-- output and log directories -->
              <div
                class="flex gap-2 items-center w-full"
                v-if="conversionOutputDir"
              >
                <i-mdi-folder class="text-lg" />
                <span class="font-semibold flex-none">
                  Output Directory :
                </span>

                <CopyText
                  :text="getConversionOutputDir(conversion)"
                  class="w-96"
                />
              </div>

              <!-- Reports Button -->
              <div class="mt-4">
                <div class="flex gap-2 items-center w-full">
                  <i-mdi-file-document-multiple class="text-lg" />
                  <span class="font-semibold flex-none">Reports</span>
                  <va-button
                    preset="secondary"
                    icon="open_in_new"
                    size="small"
                    @click="openReports"
                  >
                    View Reports
                  </va-button>
                </div>
              </div>

              <!-- Logs Section -->
              <div class="mt-4" v-if="logs.length > 0">
                <div class="flex items-start gap-2">
                  <span class="font-semibold flex-none">Logs</span>
                  <div class="flex items-start gap-2">
                    <div
                      class="bg-gray-100 dark:bg-gray-800 px-3 py-2 rounded text-sm overflow-x-auto overflow-y-auto max-h-32 max-w-md"
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
                </div>
              </div>

              <div class="flex gap-2 items-center w-full"></div>
            </va-card-content>
          </va-card>
        </div>
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
import { getConversionOutputDir } from "@/services/conversion";
import conversionApiService from "@/services/conversion/api";
import toast from "@/services/toast";

const props = defineProps({ conversionId: String });

const conversion = ref({});
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

  console.log("will call getReportsUrl");
  conversionApiService.getReportsUrl(conversionId)
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
      // console.log("conversion.value", conversion.value);
      // console.log("logs.value", logs.value);
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

const formattedLogs = computed(() => {
  return logs.value.map((log) => `${log.message.trim()}`).join("\n");
});

const conversionOutputDir = computed(() => {
  return (
    Object.entries(conversion.value) > 0 &&
    getConversionOutputDir(conversion.value)
  );
});

onMounted(() => {
  console.log("ConversionView onMounted", props.conversionId);
  fetch_conversion(true);
});
</script>
