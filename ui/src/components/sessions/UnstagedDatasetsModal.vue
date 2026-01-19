<template>
  <va-modal
    v-model="showModal"
    title="Unstaged Datasets"
    size="large"
    ok-text="Stage All Datasets"
    :ok-disabled="stagingInProgress"
    @ok="handleStageAll"
    @cancel="handleCancel"
  >
    <va-inner-loading :loading="loading || stagingInProgress">
      <div class="space-y-4">
        <div v-if="!loading && datasets.length > 0">
          <p class="mb-4">
            The following datasets need to be staged before they can be viewed in the genome
            browser. Would you like to stage all of them?
          </p>

          <va-data-table
            :items="datasets"
            :columns="columns"
            :loading="false"
            disable-client-side-sorting
          >
            <template #cell(name)="{ rowData }">
              <router-link
                :to="`/datasets/${rowData.id}`"
                target="_blank"
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

            <template #cell(created_at)="{ rowData }">
              <span>{{ datetime.fromNow(rowData.created_at) }}</span>
            </template>
          </va-data-table>

          <div v-if="stagingResults.length > 0" class="mt-4">
            <va-divider />
            <h3 class="text-base font-medium mb-2">Staging Results</h3>
            <div class="space-y-2">
              <div
                v-for="result in stagingResults"
                :key="result.dataset_id"
                class="flex items-center gap-2 p-2 rounded"
                :class="{
                  'bg-green-50': result.success,
                  'bg-red-50': !result.success,
                }"
              >
                <va-icon
                  :name="result.success ? 'check_circle' : 'error'"
                  :color="result.success ? 'success' : 'danger'"
                />
                <span class="flex-1">
                  {{ result.dataset_name }}:
                  {{ result.success ? 'Staging workflow started' : `Failed: ${result.error}` }}
                </span>
              </div>
            </div>
          </div>
        </div>

        <div v-else-if="!loading && datasets.length === 0">
          <p>All datasets for this session are already staged.</p>
        </div>
      </div>
    </va-inner-loading>
  </va-modal>
</template>

<script setup>
import * as datetime from '@/services/datetime';
import sessionService from '@/services/session';
import toast from '@/services/toast';
import { ref, watch } from 'vue';

const props = defineProps({
  sessionId: {
    type: Number,
    required: true,
  },
});

const emit = defineEmits(['close', 'staging-requested']);

const showModal = defineModel({ type: Boolean, default: false });
const loading = ref(false);
const stagingInProgress = ref(false);
const datasets = ref([]);
const stagingResults = ref([]);

const columns = [
  {
    key: 'name',
    label: 'Dataset Name',
    sortable: true,
    width: '35%',
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
    width: '25%',
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

const loadUnstagedDatasets = async () => {
  if (!props.sessionId) return;

  loading.value = true;
  try {
    const response = await sessionService.getDatasets(props.sessionId, { staged: false });
    datasets.value = response.data.datasets || [];

    if (datasets.value.length === 0) {
      toast.info('All datasets are already staged');
    }
  } catch (error) {
    console.error('Failed to load unstaged datasets:', error);
    toast.error('Failed to load unstaged datasets');
    datasets.value = [];
  } finally {
    loading.value = false;
  }
};

const handleStageAll = async () => {
  if (datasets.value.length === 0) {
    handleCancel();
    return;
  }

  stagingInProgress.value = true;
  stagingResults.value = [];

  try {
    const response = await sessionService.stageDatasets(props.sessionId);
    stagingResults.value = response.data.results || [];

    const successCount = stagingResults.value.filter((r) => r.success).length;
    const failCount = stagingResults.value.length - successCount;

    if (failCount === 0) {
      toast.success(`Staging requested for ${successCount} dataset(s)`);
      setTimeout(() => {
        emit('staging-requested');
        handleCancel();
      }, 2000);
    } else {
      toast.warning(`Staging requested for ${successCount} dataset(s), ${failCount} failed`);
    }
  } catch (error) {
    console.error('Failed to stage datasets:', error);
    toast.error('Failed to stage datasets');
  } finally {
    stagingInProgress.value = false;
  }
};

const handleCancel = () => {
  showModal.value = false;
  stagingResults.value = [];
  emit('close');
};

// Load datasets when modal opens
watch(showModal, (isOpen) => {
  if (isOpen) {
    stagingResults.value = [];
    loadUnstagedDatasets();
  }
});
</script>
