<template>
  <div>
    <!-- table -->
    <div class="overflow-x-auto">
      <va-data-table
        :items="rows"
        :columns="columns"
        v-model:sort-by="query.sort_by"
        v-model:sorting-order="query.sort_order"
        disable-client-side-sorting
        hoverable
        :loading="loading"
      >
        <template #cell(name)="{ rowData }">
          <router-link
            v-if="auth.canOperate"
            :to="`/datasets/${rowData.dataset.id}`"
            class="va-link"
          >
            {{ rowData.dataset.name }}
          </router-link>
          <span v-else>{{ rowData.dataset.name }}</span>
        </template>

        <template #cell(type)="{ rowData }">
          <DatasetType :type="rowData.dataset.type" />
        </template>

        <template #cell(stage)="{ rowData }">
          <div v-if="rowData.dataset.is_staged">
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
              @click="openStageModal(rowData.dataset)"
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
              @click="openDownloadModal(rowData.dataset)"
              :disabled="!rowData.dataset.is_staged"
            />
          </div>
        </template>

        <template #cell(updated_at)="{ rowData }">
          <span>{{ datetime.date(rowData.dataset.updated_at) }}</span>
        </template>

        <template #cell(metadata)="{ rowData }">
          <maybe :data="rowData.dataset?.metadata?.num_genome_files" />
        </template>

        <template #cell(du_size)="{ rowData }">
          <span v-if="rowData.dataset.du_size != null">
            {{ formatBytes(rowData.dataset.du_size) }}
          </span>
        </template>
      </va-data-table>
    </div>

    <!-- pagination -->
    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="query.page"
      v-model:page_size="query.page_size"
      :total_results="total_results"
      :curr_items="derivedDatasets.length"
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
  </div>
</template>

<script setup>
import DatasetType from "@/components/dataset/DatasetType.vue";
import Pagination from "@/components/utils/Pagination.vue";
import DatasetDownloadModal from "@/components/project/datasets/DatasetDownloadModal.vue";
import StageDatasetModal from "@/components/project/datasets/StageDatasetModal.vue";
import ConversionApiService from "@/services/conversion/api";
import DatasetService from "@/services/dataset";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import wfService from "@/services/workflow";
import { formatBytes } from "@/services/utils";
import { computed, ref, watch } from "vue";
import { useAuthStore } from "@/stores/auth";
import { HalfCircleSpinner } from "epic-spinners";
import { useColors } from "vuestic-ui";
import { useIntervalFn } from "@vueuse/core";
import config from "@/config";

const { colors } = useColors();
const auth = useAuthStore();

const props = defineProps({
  conversionId: {
    type: [Number],
    required: true,
  },
});

const derivedDatasets = ref([]);
const loading = ref(false);
const total_results = ref(0);

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100];

const query = ref({
  page: 1,
  page_size: 25,
  sort_by: "created_at",
  sort_order: "desc",
});

const columns = [
  {
    key: "name",
    sortable: true,
  },
  { key: "stage", width: "7%", thAlign: "center", tdAlign: "center" },
  { key: "download", width: "8%", thAlign: "center", tdAlign: "center" },
  {
    key: "type",
    sortable: true,
    width: "12%",
  },
  {
    key: "updated_at",
    label: "last updated",
    sortable: true,
    width: "12%",
  },
  {
    key: "metadata",
    label: "data files",
    sortable: false,
    width: "10%",
  },
  {
    key: "du_size",
    label: "size",
    sortable: true,
    width: "10%",
  },
];

// Compute rows with workflow status (dataset is nested under rowData.dataset)
const rows = computed(() => {
  return derivedDatasets.value.map((item) => ({
    ...item,
    is_staging_pending: wfService.is_staging_workflow_active(
      item.dataset?.workflows,
    ),
    is_archival_pending: wfService.is_step_pending(
      "archive",
      item.dataset?.workflows,
    ),
  }));
});

const tracking = computed(() => {
  return rows.value
    .filter((item) => item.is_staging_pending)
    .map((item) => item.dataset?.id)
    .filter(Boolean);
});

const fetchParams = computed(() => {
  const offset = (query.value.page - 1) * query.value.page_size;
  return {
    limit: query.value.page_size,
    offset,
    sort_by: query.value.sort_by,
    sort_order: query.value.sort_order,
  };
});

async function fetchDerivedDatasets() {
  if (!props.conversionId) return;

  loading.value = true;
  try {
    const response = await ConversionApiService.getDerivedDatasets(
      props.conversionId,
      fetchParams.value,
    );
    derivedDatasets.value = response.data.derived_datasets || [];
    total_results.value = response.data.metadata.count || 0;
  } catch (error) {
    toast.error("Error fetching derived datasets:", error);
    derivedDatasets.value = [];
    total_results.value = 0;
  } finally {
    loading.value = false;
  }
}

// Fetch and update a specific dataset in the list
const fetchAndUpdateDataset = async (id) => {
  try {
    const response = await DatasetService.getById({
      id,
      include_projects: true,
      bundle: true,
    });
    const index = derivedDatasets.value.findIndex(
      (item) => item.dataset?.id === id,
    );
    if (index !== -1) {
      derivedDatasets.value[index] = {
        ...derivedDatasets.value[index],
        dataset: response.data,
      };
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

watch(
  () => [
    query.value.page,
    query.value.page_size,
    query.value.sort_by,
    query.value.sort_order,
  ],
  () => {
    fetchDerivedDatasets();
  },
  { immediate: true },
);

watch(
  () => props.conversionId,
  () => {
    query.value.page = 1;
    fetchDerivedDatasets();
  },
  { immediate: true },
);

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
