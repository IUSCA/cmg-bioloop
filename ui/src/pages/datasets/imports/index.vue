<template>
  <va-alert
    color="warning"
    icon="warning"
    v-if="!auth.isFeatureEnabled('import')"
  >
    This feature is currently disabled
  </va-alert>

  <div v-else>
    <div class="flex mb-3 gap-3">
      <!-- search bar -->
      <div class="flex-1">
        <va-input
          v-model="filterInput"
          class="w-full"
          placeholder="Type / to search Dataset Imports"
          outline
          clearable
          input-class="search-input"
        >
          <template #prependInner>
            <Icon icon="material-symbols:search" class="text-xl" />
          </template>
        </va-input>
      </div>

      <!-- import button -->
      <div class="flex-none">
        <va-button
          icon="mdi-file-cog-outline"
          class="px-1"
          color="success"
          @click="router.push('/datasets/import')"
        >
          Import Dataset
        </va-button>
      </div>
    </div>

    <!-- table -->
    <va-data-table :items="pastImports" :columns="columns">
      <template #cell(imported_dataset)="{ rowData }">
        <div v-if="!auth.canOperate">
          {{ rowData.dataset.name }}
        </div>
        <router-link
          v-else
          :to="`/datasets/${rowData.dataset.id}`"
          class="va-link"
        >
          {{ rowData.dataset.name }}
        </router-link>
      </template>

      <template #cell(imported_dataset_type)="{ rowData }">
        <va-chip size="small" outline>
          {{ rowData.dataset.type }}
        </va-chip>
      </template>

      <template #cell(user)="{ rowData }">
        <span v-if="rowData.dataset.audit_logs && rowData.dataset.audit_logs.length > 0">
          {{ rowData.dataset.audit_logs[0].user.name }} ({{ rowData.dataset.audit_logs[0].user.username }})
        </span>
        <span v-else>-</span>
      </template>

      <template #cell(imported_at)="{ rowData }">
        <span class="text-sm lg:text-base">
          {{ datetime.date(rowData.created_at) }}
        </span>
      </template>
    </va-data-table>

    <Pagination
      v-model:page="currentPageIndex"
      v-model:page_size="pageSize"
      :total_results="total_results"
      :curr_items="pastImports.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />
  </div>
</template>

<script setup>
import useSearchKeyShortcut from "@/composables/useSearchKeyShortcut";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import datasetService from "@/services/dataset";
import { useAuthStore } from "@/stores/auth";
import { useNavStore } from "@/stores/nav";
import _ from "lodash";

const nav = useNavStore();
const router = useRouter();
const auth = useAuthStore();

nav.setNavItems([{ label: "Dataset Imports" }]);

useSearchKeyShortcut();

const PAGE_SIZE_OPTIONS = [10, 20, 50];

const filterInput = ref("");
const pastImports = ref([]);

const currentPageIndex = ref(1);
const pageSize = ref(10);
const total_results = ref(0);
// used for OFFSET clause in the SQL used to retrieve the next paginated batch
// of results
const offset = computed(() => (currentPageIndex.value - 1) * pageSize.value);
// Criterion based on search input
const search_query = computed(() => {
  return filterInput.value?.length > 0 && { dataset_name: filterInput.value };
});
// Criteria used to limit the number of results retrieved, and to define the
// offset starting at which the next batch of results will be retrieved.
const imports_batching_query = computed(() => {
  return { offset: offset.value, limit: pageSize.value };
});
// Aggregation of all filtering criteria. Used for retrieving results, and
// configuring number of pages for pagination.
const filter_query = computed(() => {
  return {
    ...imports_batching_query.value,
    ...(!!search_query.value && { ...search_query.value }),
    username: auth.user?.username,
    forSelf: !auth.canOperate,
  };
});

const columns = [
  {
    key: "imported_dataset",
    label: "Imported Dataset",
    width: "30%",
    thAlign: "center",
    tdAlign: "center",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "imported_dataset_type",
    label: "Dataset Type",
    width: "20%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "user",
    label: "Initiated By",
    width: "25%",
    thAlign: "center",
    tdAlign: "center",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "imported_at",
    label: "Import Date",
    width: "25%",
    thAlign: "right",
    tdAlign: "right",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
];

const getImportLogs = async () => {
  return datasetService
    .getDatasetImportLogs(filter_query.value)
    .then((res) => {
      pastImports.value = res.data.imports;
      total_results.value = res.data.metadata.count;
    })
    .catch((err) => {
      toast.error("Could not retrieve past imports");
      console.error("Error fetching import logs:", err);
    });
};

onMounted(() => {
  getImportLogs();
});

watch(filterInput, () => {
  currentPageIndex.value = 1;
});

watch(filter_query, (newQuery, oldQuery) => {
  // Retrieve updated results whenever retrieval criteria changes
  if (!_.isEqual(newQuery, oldQuery)) {
    getImportLogs();
  }
});
</script>

<route lang="yaml">
meta:
  title: Dataset Imports
</route>
