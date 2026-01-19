<template>
  <va-card>
    <va-card-title>
      <span class="text-lg">Associated Datasets</span>
    </va-card-title>
    <va-card-content>
      <va-inner-loading :loading="loading">
        <div v-if="datasets.length > 0" class="space-y-4">
          <va-data-table
            :items="paginatedDatasets"
            :columns="columns"
            :loading="loading"
            disable-client-side-sorting
          >
            <template #cell(name)="{ rowData }">
              <router-link
                :to="`/datasets/${rowData.id}`"
                class="va-link font-medium"
              >
                {{ rowData.name }}
              </router-link>
            </template>

            <template #cell(type)="{ rowData }">
              <va-chip size="small">
                {{ rowData.type }}
              </va-chip>
            </template>

            <template #cell(genome)="{ rowData }">
              <div v-if="rowData.genomic_details" class="flex gap-2">
                <va-chip v-if="rowData.genomic_details.genome_type" size="small">
                  {{ rowData.genomic_details.genome_type }}
                </va-chip>
                <va-chip v-if="rowData.genomic_details.genome_value" size="small" outline>
                  {{ rowData.genomic_details.genome_value }}
                </va-chip>
              </div>
              <span v-else>—</span>
            </template>

            <template #cell(is_staged)="{ rowData }">
              <div class="flex justify-center">
                <!-- Dataset is staged -->
                <div v-if="rowData.is_staged">
                  <va-icon name="check_circle" color="success" />
                </div>
                <!-- Dataset is being staged -->
                <va-popover
                  v-else-if="rowData.is_staging_pending"
                  message="Dataset is being staged"
                >
                  <half-circle-spinner
                    :animation-duration="1000"
                    :size="24"
                    color="#ffc107"
                  />
                </va-popover>
                <!-- Dataset is not staged -->
                <va-icon v-else name="cancel" color="danger" />
              </div>
            </template>

            <template #cell(created_at)="{ rowData }">
              <span>{{ datetime.fromNow(rowData.created_at) }}</span>
            </template>
          </va-data-table>

          <!-- Pagination - only show if more than one page -->
          <div v-if="totalPages > 1" class="flex justify-between items-center">
            <div>
              Showing {{ startIndex + 1 }} to {{ endIndex }} of {{ datasets.length }} datasets
            </div>
            <va-pagination
              v-model="currentPage"
              :pages="totalPages"
              :visible-pages="5"
            />
          </div>
        </div>

        <div v-else class="text-center py-8">
          No datasets associated with this session.
        </div>
      </va-inner-loading>
    </va-card-content>
  </va-card>
</template>

<script setup>
import { HalfCircleSpinner } from 'epic-spinners';
import { useIntervalFn } from '@vueuse/core';
import { computed, ref, watch } from 'vue';
import * as datetime from '@/services/datetime';
import sessionService from '@/services/session';
import datasetService from '@/services/dataset';
import wfService from '@/services/workflow';
import config from '@/config';

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
const pageSize = ref(10);

const columns = [
  {
    key: 'name',
    label: 'Dataset Name',
    sortable: true,
    width: '30%',
    thAlign: 'left',
    tdAlign: 'left',
  },
  {
    key: 'type',
    label: 'Type',
    sortable: true,
    width: '15%',
  },
  {
    key: 'genome',
    label: 'Genome',
    sortable: false,
    width: '20%',
  },
  {
    key: 'is_staged',
    label: 'Staged',
    sortable: true,
    width: '10%',
  },
  {
    key: 'created_at',
    label: 'Created',
    sortable: true,
    width: '15%',
    thAlign: 'right',
    tdAlign: 'right',
  },
];

// Compute rows with workflow status
const datasetsWithStatus = computed(() => {
  return datasets.value.map((ds) => ({
    ...ds,
    is_staging_pending: wfService.is_step_pending('stage', ds.workflows),
  }));
});

// Pagination
const totalPages = computed(() => Math.ceil(datasets.value.length / pageSize.value));
const startIndex = computed(() => (currentPage.value - 1) * pageSize.value);
const endIndex = computed(() => Math.min(startIndex.value + pageSize.value, datasets.value.length));

const paginatedDatasets = computed(() => {
  return datasetsWithStatus.value.slice(startIndex.value, endIndex.value);
});

// Track datasets that are being staged
const tracking = computed(() => {
  return datasetsWithStatus.value
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
</script>

