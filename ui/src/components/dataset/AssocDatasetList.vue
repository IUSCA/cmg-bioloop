<template>
  <VaInnerLoading :loading="data_loading">
    <va-data-table
      :items="rows"
      :columns="columns"
      v-model:sort-by="sort_by"
      v-model:sorting-order="sort_order"
      disable-client-side-sorting
    >
      <template #cell(name)="{ rowData }">
        <router-link v-if="auth.canOperate" :to="`/datasets/${rowData.id}`" class="va-link">
          {{ rowData.name }}
        </router-link>
        <span v-else>{{ rowData.name }}</span>
      </template>

      <template #cell(type)="{ rowData }">
        <DatasetType v-if="rowData.type" :type="rowData.type" />
      </template>

      <template #cell(derivation_method)="{ rowData }">
        <va-chip
          v-if="rowData.create_method"
          :color="rowData.create_method === 'CONVERSION' ? 'info' : 'secondary'"
          size="small"
        >
          {{ snakeCaseToTitleCase(rowData.create_method) }}
        </va-chip>
      </template>

      <template #cell(stage)="{ rowData }">
        <div v-if="rowData.is_staged">
          <va-button
            class="shadow"
            preset="primary"
            color="info"
            icon="cloud_sync"
            disabled
          />
        </div>
        <div v-else class="flex justify-center">
          <va-popover
            v-if="rowData.is_archival_pending"
            :message="'Dataset is pending archival to SDA'"
          >
            <half-circle-spinner
              class="flex-none"
              :animation-duration="1000"
              :size="24"
              :color="colors.info"
            />
          </va-popover>
          <va-popover
            v-else-if="rowData.is_staging_pending"
            :message="'Dataset is being staged'"
          >
            <half-circle-spinner
              class="flex-none"
              :animation-duration="1000"
              :size="24"
              :color="colors.warning"
            />
          </va-popover>
          <va-button
            v-else
            class="shadow flex-none"
            preset="primary"
            color="info"
            icon="cloud_sync"
            @click="openStageModal(rowData)"
          />
        </div>
      </template>

      <template #cell(download)="{ rowData }">
        <div class="">
          <va-button
            class="shadow"
            preset="primary"
            color="info"
            icon="cloud_download"
            @click="openDownloadModal(rowData)"
            :disabled="!rowData.is_staged"
          />
        </div>
      </template>

      <template #cell(du_size)="{ source }">
        <span>{{ source != null ? formatBytes(source) : "" }}</span>
      </template>

      <template #cell(created_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(updated_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(archive_path)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>

      <template #cell(is_staged)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>
      <template #cell(is_deleted)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>
    </va-data-table>
    <!-- pagination -->
    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="page"
      v-model:page_size="page_size"
      :total_results="total_results"
      :curr_items="datasets.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />

    <!-- Download Modal -->
    <DatasetDownloadModal ref="downloadModal" :dataset="datasetToDownload" />

    <!-- Stage Modal -->
    <StageDatasetModal
      ref="stageModal"
      :dataset="datasetToStage"
      @update="fetchAndUpdateDataset"
    />
  </VaInnerLoading>
</template>

<script setup>
import DatasetService from "@/services/dataset";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import { formatBytes, snakeCaseToTitleCase } from "@/services/utils";
import wfService from "@/services/workflow";
import { useAuthStore } from "@/stores/auth";
import DatasetDownloadModal from "@/components/project/datasets/DatasetDownloadModal.vue";
import StageDatasetModal from "@/components/project/datasets/StageDatasetModal.vue";
import DatasetType from "@/components/dataset/DatasetType.vue";
import { HalfCircleSpinner } from "epic-spinners";
import { useColors } from "vuestic-ui";
import { useIntervalFn } from "@vueuse/core";
import config from "@/config";

const { colors } = useColors();
const auth = useAuthStore();

const props = defineProps({
  datasets_meta: {
    type: Array,
    default: () => [],
  },
  show_derivation_method: {
    type: Boolean,
    default: false,
  },
  // 'derived' = show child datasets (use derived_id from dataset_hierarchy)
  // 'source' = show parent datasets (use source_id from dataset_hierarchy)
  relationship_type: {
    type: String,
    default: 'derived',
    validator: (value) => ['derived', 'source'].includes(value),
  },
});

const datasets = ref([]);
const data_loading = ref(false);

// Extract dataset IDs based on relationship type
const dataset_ids = computed(() =>
  props.datasets_meta.map((meta) => {
    if (props.relationship_type === 'source') {
      return meta.source_id;
    }
    return meta.derived_id;
  })
);

// pagination
const page = ref(1);
const page_size = ref(10);
const total_results = computed(() => dataset_ids.value.length);
const PAGE_SIZE_OPTIONS = [5, 10, 20, 50, 100];
const offset = computed(() => (page.value - 1) * page_size.value);

// sorting
const sort_by = ref("updated_at");
const sort_order = ref("desc");

// Compute rows with workflow status
const rows = computed(() => {
  return datasets.value.map((ds) => ({
    ...ds,
    is_staging_pending: wfService.is_staging_workflow_active(ds.workflows),
    is_archival_pending: wfService.is_step_pending('archive', ds.workflows),
  }));
});

// Track datasets being staged for polling
const tracking = computed(() => {
  return rows.value.filter((ds) => ds.is_staging_pending).map((ds) => ds.id);
});

const baseColumns = [
  { key: "name", sortable: true },
  { key: "stage", width: "7%", thAlign: "center", tdAlign: "center" },
  { key: "download", width: "8%", thAlign: "center", tdAlign: "center" },
  { key: "type", sortable: true, width: "12%" },
  {
    key: "du_size",
    label: "size",
    sortable: true,
    width: "8%",
  },
  { key: "created_at", label: "created on", sortable: true, width: "10%" },
  {
    key: "updated_at",
    label: "last updated",
    sortable: true,
    width: "10%",
  },
  {
    key: "archive_path",
    label: "archived",
    thAlign: "center",
    tdAlign: "center",
    width: "8%",
  },
  {
    key: "is_staged",
    label: "staged",
    thAlign: "center",
    tdAlign: "center",
    width: "8%",
  },
  {
    key: "is_deleted",
    label: "deleted",
    thAlign: "center",
    tdAlign: "center",
    width: "8%",
  },
];

const columns = computed(() => {
  const cols = [...baseColumns];
  if (props.show_derivation_method) {
    cols.splice(4, 0, {
      key: "derivation_method",
      label: "Derivation Method",
      sortable: false,
      thAlign: "left",
      tdAlign: "left",
      width: "12%",
    });
  }
  return cols;
});

watch([() => props.datasets_meta], fetchDatasets, { immediate: true });
watch(page, fetchDatasets);
watch([page_size, sort_by, sort_order], () => {
  if (page.value !== 1) {
    page.value = 1;
  } else {
    fetchDatasets();
  }
});

function fetchDatasets() {
  if (!dataset_ids.value.length) {
    return;
  }
  data_loading.value = true;
  const ids_to_fetch = dataset_ids.value.slice(
    offset.value,
    offset.value + page_size.value,
  );
  DatasetService.getAll({
    id: ids_to_fetch,
    sort_by: sort_by.value,
    sort_order: sort_order.value,
    bundle: true,
  })
    .then((res) => {
      datasets.value = res.data.datasets;
    })
    .catch((err) => {
      console.error("Unable to fetch datasets", err);
      toast.error("Unable to fetch datasets");
    })
    .finally(() => {
      data_loading.value = false;
    });
}

// Fetch and update individual dataset after staging
const fetchAndUpdateDataset = async (id) => {
  try {
    const response = await DatasetService.getById({ id, include_projects: true, bundle: true });
    const index = datasets.value.findIndex((ds) => ds.id === id);
    if (index !== -1) {
      datasets.value[index] = response.data;
    }
  } catch (error) {
    console.error(`Failed to update dataset ${id}:`, error);
  }
};

const poll = useIntervalFn(
  () => {
    tracking.value.forEach(fetchAndUpdateDataset);
  },
  config.dataset_polling_interval,
  { immediate: false },
);

watch(tracking, () => {
  if (tracking.value.length > 0) {
    poll.resume();
  } else {
    poll.pause();
  }
});

// Download modal
const downloadModal = ref(null);
const datasetToDownload = ref({});

function openDownloadModal(dataset) {
  datasetToDownload.value = dataset;
  downloadModal.value.show();
}

// Stage modal
const stageModal = ref(null);
const datasetToStage = ref({});

function openStageModal(dataset) {
  datasetToStage.value = dataset;
  stageModal.value.show();
}
</script>
