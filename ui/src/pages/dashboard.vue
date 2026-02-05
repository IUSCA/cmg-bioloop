<template>
  <div class="flex flex-col gap-4">
    <!-- Storage -->
    <div>
      <span class="text-xl font-bold block mb-1">RESOURCE USAGE</span>
      <Storage></Storage>
    </div>

    <!-- Legacy migration notice -->
    <va-alert
      v-if="config.legacy?.enabled"
      color="warning"
      dense
      class="w-full text-sm"
      icon="warning"
    >
      Genomic File Count may not be reflected accurately for legacy Datasets, until they are migrated from CMG.
    </va-alert>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-3">
      <!-- Raw Data Stats -->
      <div class="">
        <!-- <span class="text-xl font-bold block my-1">RAW DATA</span> -->
        <router-link to="/rawdata" class="va-link">
          <stats :data="raw_data_stats" title="RAW DATA"></stats>
        </router-link>
      </div>

      <!-- Data Product Stats -->
      <div class="">
        <!-- <span class="text-xl font-bold block my-1"> DATA PRODUCTS </span> -->
        <router-link to="/dataproducts" class="va-link">
          <stats :data="data_products_stats" title="DATA PRODUCTS"></stats>
        </router-link>
      </div>
    </div>

    <!-- Workflows -->
    <!-- <div>
      <span class="text-xl font-bold block my-1">ACTIVE WORKFLOWS</span>
      <div v-if="(workflows || []).length > 0">
        <collapsible
          v-for="workflow in workflows"
          :key="workflow.id"
          v-model="workflow.collapse_model"
        >
          <template #header-content>
            <div class="flex-[0_0_90%]">
              <workflow-compact :workflow="workflow" show_dataset />
            </div>
          </template>

          <div>
            <workflow :workflow="workflow" @update="update"></workflow>
          </div>
        </collapsible>
      </div>
      <div
        v-else
        class="text-center bg-slate-200 dark:bg-slate-800 py-2 rounded shadow"
      >
        <i-mdi-card-remove-outline class="inline-block text-4xl pr-3" />
        <span class="text-lg block md:inline">
          There are no active workflows.
        </span>
      </div>
    </div> -->

    <Tasks />
  </div>
</template>

<script setup>
import config from "@/config";
import DatasetService from "@/services/dataset";
import toast from "@/services/toast";
// import workflowService from "@/services/workflow";
import { useNavStore } from "@/stores/nav";

const nav = useNavStore();

nav.setNavItems([], false);

// const workflows = ref([]);
const raw_data_stats = ref({});
const data_products_stats = ref({});

// workflowService
//   .getAll({ last_task_run: true, status: "ACTIVE" })
//   .then((res) => {
//     workflows.value = res.data.results;
//     console.log("workflows", workflows.value);
//   });

// function update() {
//   console.log("workflow updated");
// }

DatasetService.getStats({ type: "RAW_DATA" })
  .then((res) => {
    raw_data_stats.value = res.data;
  })
  .catch((err) => {
    console.error(err);
    toast.error("Unable to fetch raw data stats");
  });

DatasetService.getStats({ type: "DATA_PRODUCT" })
  .then((res) => {
    data_products_stats.value = res.data;
  })
  .catch((err) => {
    console.error(err);
    toast.error("Unable to fetch data products stats");
  });
</script>

<route lang="yaml">
meta:
  title: Dashboard
</route>
