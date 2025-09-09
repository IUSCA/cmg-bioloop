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

              <!-- <div
                class="flex gap-2 items-center w-full"
                v-if="conversion?.definition?.logs_directory"
              >
                <i-mdi-file-document class="text-lg" />
                <span class="font-semibold"> Logs Directory : </span>

                <CopyText
                  :text="conversion?.definition?.logs_directory"
                  class="w-96"
                />
              </div> -->
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
</template>

<script setup>
import { getConversionOutputDir } from "@/services/conversion";
import conversionApiService from "@/services/conversion/api";
import toast from "@/services/toast";

const props = defineProps({ conversionId: String });

const conversion = ref({});
const loading = ref(false);

function fetch_conversion(show_loading = false) {
  loading.value = show_loading;
  console.log("fetching conversion", props.conversionId);
  conversionApiService
    .get(props.conversionId, {
      include_dataset: true,
      include_derived_datasets: true,
      include_definition: true,
    })
    .then((res) => {
      conversion.value = res.data;
      console.log("conversion.value", conversion.value);
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
