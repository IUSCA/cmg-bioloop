<template>
  <div class="container mx-auto px-4 py-6">
    <!-- Header -->
    <div class="mb-6">
      <h1 class="text-3xl font-bold">Upload Verification Details</h1>
    </div>

    <!-- Loading State -->
    <va-inner-loading :loading="loading">
      <div v-if="!loading && upload">
        <!-- Upload Overview Card -->
        <va-card class="mb-4">
          <va-card-title>Upload Overview</va-card-title>
          <va-card-content>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <!-- Dataset Name -->
              <div>
                <div class="text-sm text-gray-500 mb-1">Dataset</div>
                <router-link
                  v-if="upload.dataset"
                  :to="`/datasets/${upload.dataset.id}`"
                  class="text-blue-600 hover:underline font-semibold"
                >
                  {{ upload.dataset.name }}
                </router-link>
                <div v-else class="text-gray-400">N/A</div>
              </div>

              <!-- Upload Type -->
              <div>
                <div class="text-sm text-gray-500 mb-1">Upload Type</div>
                <div class="font-semibold">Dataset</div>
              </div>

              <!-- Status -->
              <div>
                <div class="text-sm text-gray-500 mb-1">Status</div>
                <div class="flex items-center gap-2">
                  <Icon
                    :icon="getStatusIcon(upload.status)"
                    :class="getStatusIconClass(upload.status)"
                    class="text-2xl"
                  />
                  <va-chip
                    :color="getStatusColor(upload.status)"
                    size="large"
                  >
                    {{ upload.status }}
                  </va-chip>
                </div>
              </div>

              <!-- Uploaded At -->
              <div>
                <div class="text-sm text-gray-500 mb-1">Uploaded At</div>
                <div class="font-semibold">
                  {{ formatDate(upload.created_at) }}
                </div>
              </div>

              <!-- Verification Task ID -->
              <div v-if="upload.metadata?.verification_task_id">
                <div class="text-sm text-gray-500 mb-1">Verification Task ID</div>
                <div class="font-mono text-sm">
                  {{ upload.metadata.verification_task_id }}
                </div>
              </div>

              <!-- Worker Process ID -->
              <div v-if="upload.metadata?.worker_process_id">
                <div class="text-sm text-gray-500 mb-1">Worker Process ID</div>
                <div class="font-mono text-sm">
                  {{ upload.metadata.worker_process_id }}
                </div>
              </div>

              <!-- Failure Reason -->
              <div v-if="upload.metadata?.failure_reason" class="col-span-2">
                <div class="text-sm text-gray-500 mb-1">Failure Reason</div>
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
                  class="animate-pulse"
                >
                  Auto-refresh: {{ refreshCountdown }}s
                </va-chip>
                <va-button
                  size="small"
                  preset="secondary"
                  @click="toggleAutoRefresh"
                >
                  <Icon
                    :icon="autoRefresh ? 'mdi:pause' : 'mdi:play'"
                    class="text-lg mr-1"
                  />
                  {{ autoRefresh ? 'Pause' : 'Resume' }}
                </va-button>
                <va-button
                  size="small"
                  preset="secondary"
                  @click="fetchLogs"
                  :disabled="loadingLogs"
                >
                  <Icon icon="mdi:refresh" class="text-lg mr-1" />
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
              <div v-else class="text-gray-500 text-center py-8">
                No logs available yet
              </div>
            </va-inner-loading>
          </va-card-content>
        </va-card>

        <!-- No Logs Message -->
        <va-card v-else>
          <va-card-content>
            <div class="text-gray-500 text-center py-8">
              <Icon icon="mdi:information-outline" class="text-4xl mb-2" />
              <div>No verification task logs available for this upload.</div>
              <div class="text-sm mt-1">
                Logs will appear here once verification begins.
              </div>
            </div>
          </va-card-content>
        </va-card>
      </div>

      <!-- Error State -->
      <va-card v-else-if="!loading && error">
        <va-card-content>
          <va-alert color="danger">
            {{ error }}
          </va-alert>
        </va-card-content>
      </va-card>
    </va-inner-loading>
  </div>
</template>

<script setup>
import Constants from '@/constants';
import datasetService from '@/services/dataset.js';
import { Icon } from '@iconify/vue';
import { onBeforeUnmount, onMounted, ref } from 'vue';
import { useToast } from 'vuestic-ui';
import { useNavStore } from '@/stores/nav';

defineOptions({
  meta: {
    requiresRoles: ['admin'],
  },
});

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
const error = ref(null);
const autoRefresh = ref(true);
const refreshCountdown = ref(10);
let refreshInterval = null;
let countdownInterval = null;

// Fetch upload details
const fetchUpload = async () => {
  try {
    const response = await datasetService.getDatasetUploadLog(props.id);
    upload.value = response.data;
    error.value = null;
  } catch (err) {
    console.error('Failed to fetch upload:', err);
    error.value = 'Failed to load upload details. Please try again.';
    toast.error(error.value);
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
    // Don't show error toast for log fetches - they'll retry
  } finally {
    loadingLogs.value = false;
  }
};

// Status helpers
const getStatusIcon = (status) => {
  const iconMap = {
    [Constants.UPLOAD_STATUSES.UPLOADING]: 'mdi:upload',
    [Constants.UPLOAD_STATUSES.UPLOADED]: 'mdi:check-circle-outline',
    [Constants.UPLOAD_STATUSES.VERIFYING]: 'mdi:shield-search',
    [Constants.UPLOAD_STATUSES.VERIFIED]: 'mdi:shield-check',
    [Constants.UPLOAD_STATUSES.VERIFICATION_FAILED]: 'mdi:alert-circle',
    [Constants.UPLOAD_STATUSES.PROCESSING]: 'mdi:cog',
    [Constants.UPLOAD_STATUSES.COMPLETE]: 'mdi:check-circle',
    [Constants.UPLOAD_STATUSES.UPLOAD_FAILED]: 'mdi:close-circle',
    [Constants.UPLOAD_STATUSES.PROCESSING_FAILED]: 'mdi:alert-circle',
  };
  return iconMap[status] || 'mdi:help-circle';
};

const getStatusIconClass = (status) => {
  const classMap = {
    [Constants.UPLOAD_STATUSES.UPLOADING]: 'text-blue-500 animate-pulse',
    [Constants.UPLOAD_STATUSES.UPLOADED]: 'text-green-500',
    [Constants.UPLOAD_STATUSES.VERIFYING]: 'text-blue-500 animate-pulse',
    [Constants.UPLOAD_STATUSES.VERIFIED]: 'text-green-500',
    [Constants.UPLOAD_STATUSES.VERIFICATION_FAILED]: 'text-red-500',
    [Constants.UPLOAD_STATUSES.PROCESSING]: 'text-blue-500 animate-spin',
    [Constants.UPLOAD_STATUSES.COMPLETE]: 'text-green-500',
    [Constants.UPLOAD_STATUSES.UPLOAD_FAILED]: 'text-red-500',
    [Constants.UPLOAD_STATUSES.PROCESSING_FAILED]: 'text-red-500',
  };
  return classMap[status] || 'text-gray-500';
};

const getStatusColor = (status) => {
  const colorMap = {
    [Constants.UPLOAD_STATUSES.UPLOADING]: 'info',
    [Constants.UPLOAD_STATUSES.UPLOADED]: 'success',
    [Constants.UPLOAD_STATUSES.VERIFYING]: 'info',
    [Constants.UPLOAD_STATUSES.VERIFIED]: 'success',
    [Constants.UPLOAD_STATUSES.VERIFICATION_FAILED]: 'danger',
    [Constants.UPLOAD_STATUSES.PROCESSING]: 'info',
    [Constants.UPLOAD_STATUSES.COMPLETE]: 'success',
    [Constants.UPLOAD_STATUSES.UPLOAD_FAILED]: 'danger',
    [Constants.UPLOAD_STATUSES.PROCESSING_FAILED]: 'danger',
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
  // Fetch immediately
  fetchUpload();
  fetchLogs();
  
  // Then set up intervals
  refreshCountdown.value = 10;
  
  refreshInterval = setInterval(() => {
    fetchUpload();
    fetchLogs();
    refreshCountdown.value = 10;
  }, 10000); // 10 seconds

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
  
  // Set dynamic breadcrumb navigation
  if (upload.value?.dataset) {
    nav.setNavItems([
      {
        label: 'Dataset Uploads',
        to: '/datasetUpload',
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

<style scoped>
.container {
  max-width: 1200px;
}
</style>
