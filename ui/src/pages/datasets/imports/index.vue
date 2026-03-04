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

      <!-- create button -->
      <div class="flex-none">
        <va-button
          icon="add"
          class="px-1"
          color="success"
          @click="router.push('/datasets/imports/new')"
        >
          Import Dataset
        </va-button>
      </div>
    </div>

    <!-- table -->
    <va-data-table :items="pastImports" :columns="columns" :loading="loading">
      <template #cell(status)="{ rowData }">
        <!-- Legacy import: migrated from CMG, no workflow tracked -->
        <div
          v-if="rowData.metadata?.origin === 'legacy'"
          class="flex justify-center"
        >
          <va-popover :message="'Registration completed successfully'">
            <va-icon name="check_circle" color="success" />
          </va-popover>
        </div>
        <!-- Integrated workflow running -->
        <div
          v-else-if="rowData.integrated_status === 'ACTIVE'"
          class="flex justify-center"
        >
          <va-popover :message="'Registration in progress'">
            <half-circle-spinner
              class="flex-none"
              :animation-duration="1000"
              :size="24"
              :color="colors.warning"
            />
          </va-popover>
        </div>
        <!-- Integrated workflow succeeded -->
        <div
          v-else-if="rowData.integrated_status === 'SUCCESS'"
          class="flex justify-center"
        >
          <va-popover :message="'Registration completed successfully'">
            <va-icon name="check_circle" color="success" />
          </va-popover>
        </div>
        <!-- Integrated workflow failed -->
        <div
          v-else-if="rowData.integrated_status === 'FAILURE'"
          class="flex justify-center"
        >
          <va-popover :message="'Registration failed'">
            <va-icon name="warning" color="warning" />
          </va-popover>
        </div>
      </template>

      <template #cell(imported_dataset)="{ rowData }">
        <div v-if="!auth.canOperate">
          {{ rowData.imported_dataset.name }}
        </div>
        <router-link
          v-else
          :to="`/datasets/${rowData.imported_dataset.id}`"
          class="va-link"
        >
          {{ rowData.imported_dataset.name }}
        </router-link>
      </template>

      <template #cell(imported_dataset_type)="{ value }">
        <va-chip size="small" outline v-if="value">
          {{ value }}
        </va-chip>
      </template>

      <template #cell(file_type)="{ value }">
        <va-chip size="small" outline v-if="value">
          {{ value.toUpperCase() }}
        </va-chip>
      </template>

      <template #cell(genome)="{ rowData }">
        <va-chip
          size="small"
          outline
          v-if="rowData.genome_type || rowData.genome_value"
        >
          {{ rowData.genome_type || ""
          }}{{ rowData.genome_value ? ` (${rowData.genome_value})` : "" }}
        </va-chip>
      </template>

      <template #cell(source_dataset)="{ rowData }">
        <div v-if="rowData.source_dataset">
          <div v-if="!auth.canOperate">
            {{ rowData.source_dataset.name }}
          </div>
          <router-link
            v-else
            :to="`/datasets/${rowData.source_dataset.id}`"
            class="va-link"
          >
            {{ rowData.source_dataset.name }}
          </router-link>
        </div>
      </template>

      <template #cell(user)="{ rowData }">
        <span v-if="rowData.user"
          >{{ rowData.user.name }} ({{ rowData.user.username }})</span
        >
      </template>

      <template #cell(initiated_at)="{ value }">
        <span class="text-sm lg:text-base" v-if="value">
          {{ datetime.date(value) }}
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
import datasetService from "@/services/dataset";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import wfService from "@/services/workflow";
import { useAuthStore } from "@/stores/auth";
import { useNavStore } from "@/stores/nav";
import config from "@/config";
import { HalfCircleSpinner } from "epic-spinners";
import { useColors } from "vuestic-ui";
import _ from "lodash";

const { colors } = useColors();
const nav = useNavStore();
const router = useRouter();
const auth = useAuthStore();

nav.setNavItems([{ label: "Dataset Imports" }]);

useSearchKeyShortcut();

const PAGE_SIZE_OPTIONS = [10, 20, 50];

const filterInput = ref("");
const pastImports = ref([]);
const _datasets = ref({}); // Mapping of dataset_id to dataset object for polling

const currentPageIndex = ref(1);
const pageSize = ref(10);
const total_results = ref(0);
const loading = ref(false);

const offset = computed(() => (currentPageIndex.value - 1) * pageSize.value);

const search_query = computed(() => {
  return filterInput.value?.length > 0 && { dataset_name: filterInput.value };
});

const imports_batching_query = computed(() => {
  return { offset: offset.value, limit: pageSize.value };
});

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
    key: "status",
    label: "Status",
    width: "8%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "imported_dataset",
    label: "Imported Dataset",
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
    width: "12%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "file_type",
    label: "File Type",
    width: "10%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "genome",
    label: "Genome",
    width: "15%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "source_dataset",
    label: "Source Raw Data",
    width: "15%",
    thAlign: "center",
    tdAlign: "center",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "user",
    label: "Imported By",
    width: "15%",
    thAlign: "center",
    tdAlign: "center",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "initiated_at",
    label: "Imported On",
    width: "10%",
    thAlign: "right",
    tdAlign: "right",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
];

const getImportLogs = async () => {
  loading.value = true;
  return datasetService
    .getDatasetImportLogs(filter_query.value)
    .then((res) => {
      pastImports.value = res.data.imports.map((e) => {
        let imported_dataset = e.dataset;
        const status = wfService.get_integrated_workflow_status(
          imported_dataset.workflows,
        );
        const genomicDetails = imported_dataset.genomic_details;
        // Get user from create audit log (filtered by action='create', only one exists)
        const createAuditLog = imported_dataset.audit_logs?.[0];
        return {
          ...e,
          initiated_at: e.created_at,
          user: createAuditLog?.user,
          imported_dataset,
          source_dataset:
            imported_dataset.source_datasets.length > 0
              ? imported_dataset.source_datasets[0].source_dataset
              : null,
          imported_dataset_type: imported_dataset.type,
          file_type: imported_dataset.analysis_type?.name,
          genome_type: genomicDetails?.genome_type,
          genome_value: genomicDetails?.genome_value,
          integrated_status: status,
        };
      });
      total_results.value = res.data.metadata.count;
    })
    .catch((err) => {
      toast.error("Could not retrieve past imports");
      console.error("Error fetching import logs:", err);
    })
    .finally(() => {
      loading.value = false;
    });
};

// _datasets is a mapping of dataset_ids to dataset objects. While polling one
// or more datasets, this object is updated with latest dataset values.
watch(
  pastImports,
  () => {
    _datasets.value = pastImports.value.reduce((acc, obj) => {
      acc[obj.imported_dataset.id] = obj.imported_dataset;
      return acc;
    }, {});
  },
  {
    immediate: true,
  },
);

// Track datasets that have active integrated workflows
const tracking = computed(() => {
  return pastImports.value
    .filter((imp) => imp.integrated_status === "ACTIVE")
    .map((imp) => imp.imported_dataset.id);
});

// Fetch and update a single dataset's workflow status
function fetch_and_update_dataset(id) {
  datasetService
    .getById({ id, include_projects: false, bundle: true })
    .then((res) => {
      _datasets.value[id] = res.data;
      // Update the corresponding import in pastImports
      const importIndex = pastImports.value.findIndex(
        (imp) => imp.imported_dataset.id === id,
      );
      if (importIndex !== -1) {
        pastImports.value[importIndex].imported_dataset = res.data;
        pastImports.value[importIndex].integrated_status =
          wfService.get_integrated_workflow_status(res.data.workflows);
      }
    })
    .catch((err) => {
      console.error("Unable to fetch dataset", id, err);
    });
}

// Poll datasets with pending workflows
function poll_datasets() {
  tracking.value.forEach(fetch_and_update_dataset);
}

// Set up polling interval
const poll = useIntervalFn(
  () => {
    poll_datasets();
  },
  config.dataset_polling_interval,
  {
    immediate: false,
  },
);

// Start/stop polling based on whether there are datasets to track
watch(tracking, () => {
  if (tracking.value.length > 0) {
    poll.resume();
  } else {
    poll.pause();
  }
});

onMounted(() => {
  getImportLogs();
});

watch(filterInput, () => {
  currentPageIndex.value = 1;
});

watch(filter_query, (newQuery, oldQuery) => {
  if (!_.isEqual(newQuery, oldQuery)) {
    getImportLogs();
  }
});
</script>

<route lang="yaml">
meta:
  title: Dataset Imports
</route>
