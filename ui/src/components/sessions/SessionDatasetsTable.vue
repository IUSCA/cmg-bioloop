<template>
  <va-card>
    <va-card-title>
      <span class="text-lg">Associated Datasets</span>
    </va-card-title>
    <va-card-content>
      <va-inner-loading :loading="loading">
        <div v-if="rows.length > 0" class="space-y-4">
          <va-data-table
            :items="paginatedRows"
            :columns="columns"
            :loading="loading"
            disable-client-side-sorting
          >
            <template #cell(name)="{ rowData }">
              <router-link
                v-if="auth.canOperate"
                :to="`/datasets/${rowData.id}`"
                class="va-link font-medium"
              >
                {{ rowData.name }}
              </router-link>
              <span v-else>{{ rowData.name }}</span>
            </template>

            <template #cell(type)="{ rowData }">
              <DatasetType v-if="rowData.type" :type="rowData.type" />
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

            <template #cell(updated_at)="{ value }">
              <span>{{ datetime.date(value) }}</span>
            </template>

            <template #cell(metadata)="{ rowData }">
              <maybe :data="rowData?.metadata?.num_genome_files" />
            </template>

            <template #cell(du_size)="{ source }">
              <span>{{ source != null ? formatBytes(source) : '' }}</span>
            </template>
          </va-data-table>

          <!-- Pagination -->
          <Pagination
            v-model:page="currentPage"
            v-model:page_size="pageSize"
            :total_results="datasets.length"
            :curr_items="paginatedRows.length"
            :page_size_options="PAGE_SIZE_OPTIONS"
          />
        </div>

        <div v-else class="text-center py-8">
          No datasets associated with this session.
        </div>
      </va-inner-loading>
    </va-card-content>
  </va-card>

  <!-- Download Modal -->
  <DatasetDownloadModal ref="downloadModal" :dataset="datasetToDownload" />

  <!-- Stage Modal -->
  <StageDatasetModal
    ref="stageModal"
    :dataset="datasetToStage"
    @update="fetchAndUpdateDataset"
  />
</template>

<script setup>
import { useIntervalFn } from '@vueuse/core';
import { computed, ref, watch } from 'vue';
import * as datetime from '@/services/datetime';
import sessionService from '@/services/session';
import datasetService from '@/services/dataset';
import wfService from '@/services/workflow';
import { formatBytes } from '@/services/utils';
import config from '@/config';
import Pagination from '@/components/utils/Pagination.vue';
import DatasetDownloadModal from '@/components/project/datasets/DatasetDownloadModal.vue';
import StageDatasetModal from '@/components/project/datasets/StageDatasetModal.vue';
import DatasetType from '@/components/dataset/DatasetType.vue';
import { useAuthStore } from '@/stores/auth';
import { HalfCircleSpinner } from 'epic-spinners';
import { useColors } from 'vuestic-ui';

const { colors } = useColors();
const auth = useAuthStore();

const props = defineProps({
  sessionId: {
    type: Number,
    required: false,
    default: null,
  },
  dataRequested: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(['datasets-updated']);

const loading = ref(false);
const datasets = ref([]);
const currentPage = ref(1);
const pageSize = ref(25);
const PAGE_SIZE_OPTIONS = [25, 50, 100];

const columns = [
  {
    key: 'name',
    sortable: true,
  },
  { key: 'stage', width: '7%', thAlign: 'center', tdAlign: 'center' },
  { key: 'download', width: '8%', thAlign: 'center', tdAlign: 'center' },
  {
    key: 'type',
    sortable: true,
    width: '12%',
  },
  {
    key: 'updated_at',
    label: 'last updated',
    sortable: true,
    width: '12%',
  },
  {
    key: 'metadata',
    label: 'data files',
    sortable: false,
    width: '10%',
  },
  {
    key: 'du_size',
    label: 'size',
    sortable: true,
    width: '10%',
  },
];

// Compute rows with workflow status
const rows = computed(() => {
  return datasets.value.map((ds) => ({
    ...ds,
    is_staging_pending: wfService.is_staging_workflow_active(ds.workflows),
    is_archival_pending: wfService.is_step_pending('archive', ds.workflows),
  }));
});

// Pagination
const startIndex = computed(() => (currentPage.value - 1) * pageSize.value);

const paginatedRows = computed(() => {
  return rows.value.slice(startIndex.value, startIndex.value + pageSize.value);
});

// Track datasets that are being staged
const tracking = computed(() => {
  return rows.value
    .filter((ds) => ds.is_staging_pending)
    .map((ds) => ds.id);
});

// Load datasets
const loadDatasets = async () => {
  if (!props.sessionId) {
    loading.value = false;
    return;
  }

  loading.value = true;
  try {
    const response = await sessionService.getDatasets(props.sessionId);
    datasets.value = response.data.datasets || [];
    emit('datasets-updated', datasets.value);
  } catch (error) {
    console.error('Failed to load session datasets:', error);
    datasets.value = [];
  } finally {
    loading.value = false;
  }
};

// Fetch and update individual dataset
const fetchAndUpdateDataset = async (id) => {
  try {
    const response = await datasetService.getById({ id, include_projects: true, bundle: true });
    const index = datasets.value.findIndex((ds) => ds.id === id);
    if (index !== -1) {
      datasets.value[index] = response.data;
    }
  } catch (error) {
    console.error(`Failed to update dataset ${id}:`, error);
  }
};

// Poll datasets that are being staged
const pollDatasets = () => {
  tracking.value.forEach(fetchAndUpdateDataset);
};

// Set up polling
const poll = useIntervalFn(
  () => {
    pollDatasets();
  },
  config.dataset_polling_interval || 5000,
  {
    immediate: false,
  },
);

// Watch tracking to start/stop polling
watch(tracking, (newTracking) => {
  if (newTracking.length > 0) {
    poll.resume();
  } else {
    poll.pause();
  }
}, { immediate: true });

// Watch data_requested to reload when staging is initiated
watch(() => props.dataRequested, (newVal) => {
  if (newVal) {
    loadDatasets();
  }
});

// Load datasets on mount
watch(
  () => props.sessionId,
  (newId) => {
    if (newId) {
      loadDatasets();
    }
  },
  { immediate: true }
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
