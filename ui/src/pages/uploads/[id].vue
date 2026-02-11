<template>
  <div class="px-4 py-6">
    <va-inner-loading :loading="loading">
      <div v-if="upload">
        <!-- Upload Overview Card -->
        <va-card class="mb-4">
          <!-- <va-card-title>Upload Overview</va-card-title> -->
          <span class="flex-auto text-lg"> Upload Overview </span>
                
          <va-card-content>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <!-- Entity Name -->
              <div>
                <div class="mb-1">Entity</div>
                <router-link
                  v-if="upload.dataset"
                  :to="`/datasets/${upload.dataset.id}`"
                  class="va-link"
                >
                  {{ upload.dataset.name }}
                </router-link>
              </div>

              <!-- Entity Type -->
              <div>
                <div class="mb-1">Entity Type</div>
                <va-chip size="small" outline>Dataset</va-chip>
              </div>

              <!-- Status -->
              <div>
                <div class="mb-1">Status</div>
                <va-chip
                  :color="getStatusColor(upload.status)"
                  size="small"
                >
                  {{ upload.status }}
                </va-chip>
              </div>

              <!-- Updated At -->
              <div>
                <div class="mb-1">Last Updated</div>
                <div>
                  {{ formatDate(upload.updated_at) }}
                </div>
              </div>

              <!-- Process ID (TUS Upload ID) -->
              <div v-if="upload.process_id">
                <div class="mb-1">Process ID</div>
                <code class="text-sm">{{ upload.process_id }}</code>
              </div>

              <!-- Verification Task ID -->
              <div v-if="upload.metadata?.verification_task_id">
                <div class="mb-1">Verification Task ID</div>
                <code class="text-sm">{{ upload.metadata.verification_task_id }}</code>
              </div>

              <!-- Worker Process ID -->
              <div v-if="upload.metadata?.worker_process_id">
                <div class="mb-1">Worker Process ID</div>
                <code class="text-sm">{{ upload.metadata.worker_process_id }}</code>
              </div>

              <!-- Retry Count -->
              <div v-if="upload.retry_count > 0">
                <div class="mb-1">Retry Count</div>
                <div>{{ upload.retry_count }}</div>
              </div>

              <!-- Checksum Info -->
              <div v-if="upload.metadata?.checksum" class="col-span-2">
                <div class="mb-1">Checksum Information</div>
                <div class="grid grid-cols-2 gap-2 text-sm">
                  <div>Algorithm: <code>{{ upload.metadata.checksum.algorithm }}</code></div>
                  <div>File Count: {{ upload.metadata.checksum.file_count }}</div>
                  <div class="col-span-2">
                    Manifest Hash: <code class="text-xs">{{ upload.metadata.checksum.manifest_hash }}</code>
                  </div>
                </div>
              </div>

              <!-- Failure Reason -->
              <div v-if="upload.metadata?.failure_reason" class="col-span-2">
                <div class="mb-1">Failure Reason</div>
                <va-alert color="danger" class="mb-0">
                  {{ upload.metadata.failure_reason }}
                </va-alert>
              </div>
            </div>
          </va-card-content>
        </va-card>

        <!-- Verification Logs Card -->
        <va-card v-if="upload.metadata?.worker_process_id">
          <va-card-title>
            <div class="flex items-center justify-between w-full">
              <span>Verification Logs</span>
              <div class="flex items-center gap-2">
                <va-chip
                  v-if="autoRefresh"
                  color="info"
                  size="small"
                >
                  Auto-refresh: {{ refreshCountdown }}s
                </va-chip>
                <va-button
                  size="small"
                  @click="toggleAutoRefresh"
                >
                  <Icon
                    :icon="autoRefresh ? 'mdi:pause' : 'mdi:play'"
                  />
                  {{ autoRefresh ? 'Pause' : 'Resume' }}
                </va-button>
                <va-button
                  size="small"
                  @click="fetchLogs"
                  :disabled="loadingLogs"
                >
                  <Icon icon="mdi:refresh" />
                  Refresh
                </va-button>
              </div>
            </div>
          </va-card-title>
          <va-card-content>
            <va-inner-loading :loading="loadingLogs">
              <div v-if="logs.length > 0" class="bg-gray-900 text-gray-100 p-4 rounded font-mono text-sm overflow-x-auto" style="max-height: 600px; overflow-y: auto;">
                <div v-for="(log, index) in logs" :key="index" class="mb-1 whitespace-pre-wrap">
                  <span :class="getLogLevelClass(log.level)">
                    [{{ formatLogTime(log.created_at) }}] {{ log.level.toUpperCase() }}: {{ log.message }}
                  </span>
                </div>
              </div>
              <div v-else class="text-center py-8">
                No logs available yet
              </div>
            </va-inner-loading>
          </va-card-content>
        </va-card>

        <!-- No Logs Message -->
        <va-card v-else>
          <va-card-content>
            <div class="text-center py-8">
              <Icon icon="mdi:information-outline" class="text-4xl mb-2" />
              <div>No verification task logs available for this upload.</div>
              <div class="text-sm mt-1">
                Logs will appear here once verification begins.
              </div>
            </div>
          </va-card-content>
        </va-card>
      </div>
    </va-inner-loading>
  </div>
</template>

<script setup>
import constants from '@/constants';
import datasetService from '@/services/dataset.js';
import { useNavStore } from '@/stores/nav';
import { Icon } from '@iconify/vue';
import { useToast } from 'vuestic-ui';

const props = defineProps({
  id: {
    type: String,
    required: true,
  },
});

const toast = useToast();
const nav = useNavStore();

const loading = ref(true);
const loadingLogs = ref(false);
const upload = ref(null);
const logs = ref([]);
const autoRefresh = ref(true);
const refreshCountdown = ref(10);
let refreshInterval = null;
let countdownInterval = null;

// Fetch upload details
const fetchUpload = async () => {
  try {
    const response = await datasetService.getUploadLogById(props.id);
    upload.value = response.data;
  } catch (err) {
    console.error('Failed to fetch upload:', err);
  } finally {
    loading.value = false;
  }
};

// Fetch verification logs
const fetchLogs = async () => {
  if (!upload.value?.metadata?.worker_process_id) {
    return;
  }

  loadingLogs.value = true;
  try {
    const processId = upload.value.metadata.worker_process_id;
    const response = await fetch(
      `/api/workflows/processes/${processId}/logs`,
      {
        headers: {
          Authorization: `Bearer ${localStorage.getItem('token')}`,
        },
      }
    );

    if (!response.ok) {
      throw new Error('Failed to fetch logs');
    }

    const data = await response.json();
    logs.value = data.sort((a, b) => new Date(a.created_at) - new Date(b.created_at));
  } catch (err) {
    console.error('Failed to fetch logs:', err);
  } finally {
    loadingLogs.value = false;
  }
};

// Status helpers
const getStatusColor = (status) => {
  const colorMap = {
    [constants.UPLOAD_STATUSES.UPLOADING]: 'info',
    [constants.UPLOAD_STATUSES.UPLOADED]: 'success',
    [constants.UPLOAD_STATUSES.VERIFYING]: 'info',
    [constants.UPLOAD_STATUSES.VERIFIED]: 'success',
    [constants.UPLOAD_STATUSES.VERIFICATION_FAILED]: 'danger',
    [constants.UPLOAD_STATUSES.PROCESSING]: 'info',
    [constants.UPLOAD_STATUSES.COMPLETE]: 'success',
    [constants.UPLOAD_STATUSES.UPLOAD_FAILED]: 'danger',
    [constants.UPLOAD_STATUSES.PROCESSING_FAILED]: 'danger',
    [constants.UPLOAD_STATUSES.PERMANENTLY_FAILED]: 'danger',
  };
  return colorMap[status] || 'secondary';
};

const getLogLevelClass = (level) => {
  const classMap = {
    error: 'text-red-400',
    warning: 'text-yellow-400',
    info: 'text-blue-400',
    debug: 'text-gray-400',
    stdout: 'text-gray-200',
  };
  return classMap[level?.toLowerCase()] || 'text-gray-200';
};

// Date formatting
const formatDate = (date) => {
  if (!date) return 'N/A';
  return new Date(date).toLocaleString();
};

const formatLogTime = (date) => {
  if (!date) return '';
  const d = new Date(date);
  return d.toLocaleTimeString();
};

// Auto-refresh
const toggleAutoRefresh = () => {
  autoRefresh.value = !autoRefresh.value;
  if (autoRefresh.value) {
    startAutoRefresh();
  } else {
    stopAutoRefresh();
  }
};

const startAutoRefresh = () => {
  fetchUpload();
  fetchLogs();
  
  refreshCountdown.value = 10;
  
  refreshInterval = setInterval(() => {
    fetchUpload();
    fetchLogs();
    refreshCountdown.value = 10;
  }, 10000);

  countdownInterval = setInterval(() => {
    if (refreshCountdown.value > 0) {
      refreshCountdown.value--;
    }
  }, 1000);
};

const stopAutoRefresh = () => {
  if (refreshInterval) {
    clearInterval(refreshInterval);
    refreshInterval = null;
  }
  if (countdownInterval) {
    clearInterval(countdownInterval);
    countdownInterval = null;
  }
};

// Lifecycle
onMounted(async () => {
  await fetchUpload();
  
  if (upload.value?.dataset) {
    nav.setNavItems([
      {
        label: 'Uploads',
        to: '/datasets/uploads',
      },
      {
        label: upload.value.dataset.name || `Upload #${props.id}`,
      },
    ]);
  }
  
  if (upload.value?.metadata?.worker_process_id) {
    await fetchLogs();
  }
  if (autoRefresh.value) {
    startAutoRefresh();
  }
});

onBeforeUnmount(() => {
  stopAutoRefresh();
});
</script>

<route lang="yaml">
meta:
  title: Upload Details
  requiresRoles: ["admin"]
</route>
