<template>
  <div class="track-detail">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circle indeterminate />
    </div>

    <div v-else-if="track" class="space-y-6">
      <!-- Track & Dataset Information Grid -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Track Information -->
        <va-card>
          <va-card-title>
            <span class="text-lg">Track Details</span>
          </va-card-title>
          <va-card-content>
            <div class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Track Name:</span>
                <span>{{ track.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Analysis Type</span>
                <va-chip
                  v-if="track.analysis_type"
                  :color="trackService._getTrackColor(track.analysis_type)"
                  size="small"
                  outline
                >
                  {{ track.analysis_type?.toUpperCase() }}
                </va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome</span>
                <GenomeDisplay
                  :genome-type="track.dataset_file?.dataset?.genomic_details?.genome_type"
                  :genome-value="track.dataset_file?.dataset?.genomic_details?.genome_value"
                />
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created</span>
                <span>{{ datetime.date(track.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated</span>
                <span>{{ datetime.fromNow(track.updated_at) }}</span>
              </div>
            </div>
          </va-card-content>
        </va-card>

        <!-- Dataset Information -->
        <va-card>
          <va-card-title>
            <span class="text-lg">Dataset Information</span>
          </va-card-title>
          <va-card-content>
            <div v-if="track.dataset_file?.dataset" class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Dataset Name</span>
                <router-link
                  v-if="auth.canOperate"
                  :to="`/datasets/${track.dataset_file.dataset.id}`"
                  class="va-link"
                >
                  {{ track.dataset_file.dataset.name }}
                </router-link>
                <span v-else>{{ track.dataset_file.dataset.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Dataset Type</span>
                <DatasetType v-if="track.dataset_file.dataset.type" :type="track.dataset_file.dataset.type" />
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Name</span>
                <span>{{ track.dataset_file.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Size</span>
                <span>{{ formatFileSize(track.dataset_file.size) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Path</span>
                <div class="w-80">
                  <CopyText :text="track.dataset_file.path" />
                </div>
              </div>
              <div class="flex justify-between items-center">
                <span class="font-medium">Staged</span>
                <va-popover
                  v-if="trackDatasetStagingStatus.is_stage_or_migrated_active"
                  message="Dataset is being staged"
                >
                  <half-circle-spinner
                    :animation-duration="1000"
                    :size="24"
                    :color="colors.warning"
                  />
                </va-popover>
                <va-popover
                  v-else-if="trackDatasetStagingStatus.is_integrated_active"
                  message="Dataset is being integrated (will be staged on completion)"
                >
                  <half-circle-spinner
                    :animation-duration="1000"
                    :size="24"
                    :color="colors.info"
                  />
                </va-popover>
                <va-icon
                  v-else-if="track.dataset_file.dataset.is_staged"
                  name="check_circle"
                  color="success"
                />
                <va-icon
                  v-else
                  name="cancel"
                  color="danger"
                />
              </div>

            </div>
            <div v-else class="text-center py-4">No dataset information available</div>
          </va-card-content>
        </va-card>
      </div>

      <!-- Actions Card -->
      <va-card v-if="track.dataset_file?.dataset">
        <va-card-title>
          <span class="text-lg">Actions</span>
        </va-card-title>
        <va-card-content>
          <div class="flex flex-col gap-3">
            <!-- Buttons row -->
            <div class="flex flex-wrap gap-3 items-center">
              <!-- Stage Dataset -->
              <va-button
                :disabled="
                  track.dataset_file.dataset.is_staged ||
                  trackDatasetStagingStatus.is_staging_pending ||
                  trackDatasetStagingStatus.is_archival_pending
                "
                color="primary"
                border-color="primary"
                preset="secondary"
                class="flex-initial"
                @click="stageModal.show()"
              >
                <i-mdi-cloud-sync class="pr-2 text-2xl" />
                Stage Dataset
              </va-button>

              <!-- Download Dataset -->
              <va-button
                :disabled="
                  !track.dataset_file.dataset.is_staged ||
                  trackDatasetStagingStatus.is_staging_pending ||
                  trackDatasetStagingStatus.is_archival_pending
                "
                color="primary"
                border-color="primary"
                preset="secondary"
                class="flex-initial"
                @click="openDownloadDatasetModal"
              >
                <i-mdi-download class="pr-2 text-2xl" />
                Download Dataset
              </va-button>

              <!-- Download File -->
              <va-button
                :disabled="
                  !track.dataset_file.dataset.is_staged ||
                  trackDatasetStagingStatus.is_staging_pending ||
                  trackDatasetStagingStatus.is_archival_pending
                "
                color="primary"
                border-color="primary"
                preset="secondary"
                class="flex-initial"
                @click="downloadFile_"
              >
                <i-mdi-file-download class="pr-2 text-2xl" />
                Download File
              </va-button>
            </div>

            <!-- Alert shown below buttons when any staging-related workflow is active -->
            <div
              v-if="trackDatasetStagingStatus.is_staging_pending || trackDatasetStagingStatus.is_archival_pending"
              class="flex items-center gap-3"
            >
              <va-alert
                dense
                color="info"
                outline
                icon="info"
                class="flex-1"
              >
                Actions are disabled while Dataset is being staged
              </va-alert>
            </div>
          </div>
        </va-card-content>
      </va-card>

      <!-- Associated Sessions -->
      <va-card>
        <va-card-title>
          <span class="text-lg">Associated Sessions</span>
        </va-card-title>
        <va-card-content>
          <div v-if="track.session_tracks?.length" class="space-y-3">
            <va-data-table
              :items="associatedSessions"
              :columns="sessionColumns"
              :loading="false"
              disable-client-side-sorting
            >
              <template #cell(session_title)="{ rowData }">
                <router-link :to="`/sessions/${rowData.id}`" class="va-link font-medium">
                  {{ rowData.title }}
                </router-link>
              </template>

              <template #cell(genome)="{ rowData }">
                <GenomeDisplay
                  :genome-type="rowData.genome_type"
                  :genome-value="rowData.genome"
                />
              </template>

              <template #cell(created_by)="{ rowData }">
                <span>
                  {{ rowData.user?.name || rowData.user?.username }}
                </span>
              </template>

              <template #cell(created_at)="{ rowData }">
                <span>
                  {{ datetime.fromNow(rowData.created_at) }}
                </span>
              </template>
            </va-data-table>
          </div>
          <div v-else class="text-center py-8">This track is not used in any sessions yet.</div>
        </va-card-content>
      </va-card>

    </div>
  </div>

  <!-- Stage Dataset Modal -->
  <StageDatasetModal
    ref="stageModal"
    :dataset="track?.dataset_file?.dataset || {}"
    @update="onStageUpdate"
  />

  <!-- Download Dataset Modal -->
  <DatasetDownloadModal ref="downloadDatasetModal" :dataset="track?.dataset_file?.dataset || {}" />
</template>

<script setup>
import GenomeDisplay from '@/components/genome/GenomeDisplay.vue';
import DatasetType from '@/components/dataset/DatasetType.vue';
import DatasetDownloadModal from '@/components/project/datasets/DatasetDownloadModal.vue';
import StageDatasetModal from '@/components/project/datasets/StageDatasetModal.vue';
import * as datetime from '@/services/datetime';
import datasetService from '@/services/dataset';
import toast from '@/services/toast';
import trackService from '@/services/track';
import { downloadFile, formatBytes } from '@/services/utils';
import wfService from '@/services/workflow';
import { HalfCircleSpinner } from 'epic-spinners';
import { useColors } from 'vuestic-ui';
import { useNavStore } from '@/stores/nav';
import { useTracksStore } from '@/stores/tracks';
import { useAuthStore } from '@/stores/auth';

const route = useRoute();
const tracksStore = useTracksStore();
const nav = useNavStore();
const auth = useAuthStore();

// Computed
const track = computed(() => tracksStore.currentTrack);
const loading = computed(() => tracksStore.loading);
const error = computed(() => tracksStore.error);

// Session table columns
const sessionColumns = [
  {
    key: 'session_title',
    label: 'Session Title',
    sortable: true,
    width: '60%',
    thAlign: 'left',
    tdAlign: 'left',
  },
  {
    key: 'created_by',
    label: 'Created By',
    sortable: true,
    width: '10%',
  },
  {
    key: 'genome',
    label: 'Genome',
    sortable: true,
    width: '25%',
  },
  {
    key: 'created_at',
    label: 'Created',
    sortable: true,
    thAlign: 'right',
    tdAlign: 'right',
  },
];

const associatedSessions = computed(() => {
  return track.value.session_tracks.map((st) => st.session);
});

const trackDatasetStagingStatus = computed(() => {
  const workflows = track.value?.dataset_file?.dataset?.workflows;
  const stageNames = ['stage', 'stage_migrated'];
  const isStageStageMigratedActive = (workflows || []).some(
    (wf) => stageNames.includes(wf.name) && !wfService.is_workflow_done(wf),
  );
  const isIntegratedActive = (workflows || []).some(
    (wf) => wf.name === 'integrated' && !wfService.is_workflow_done(wf),
  );
  return {
    is_staging_pending: wfService.is_staging_workflow_active(workflows),
    is_archival_pending: wfService.is_step_pending('archive', workflows),
    is_stage_or_migrated_active: isStageStageMigratedActive,
    is_integrated_active: isIntegratedActive,
  };
});

const formatFileSize = (bytes) => {
  if (!bytes) return 'Unknown';
  return formatBytes(bytes);
};

const { colors } = useColors();

// Stage modal
const stageModal = ref(null);

async function onStageUpdate() {
  await tracksStore.fetchTrack(route.params.id);
}

// Download Dataset modal
const downloadDatasetModal = ref(null);

function openDownloadDatasetModal() {
  downloadDatasetModal.value.show();
}

// Download individual file
async function downloadFile_() {
  const datasetFile = track.value?.dataset_file;
  if (!datasetFile) return;
  const datasetId = datasetFile.dataset?.id;
  if (!datasetId) return;

  try {
    const res = await datasetService.get_file_download_data({
      dataset_id: datasetId,
      file_id: datasetFile.id,
    });
    const url = new URL(res.data.url);
    url.searchParams.set('token', res.data.bearer_token);
    downloadFile({
      url: url.toString(),
      filename: datasetFile.name,
    });
  } catch (err) {
    console.error(err);
    toast.error('Unable to download file');
  }
}

// Load track data
onMounted(async () => {
  try {
    await tracksStore.fetchTrack(route.params.id);

    // Set dynamic breadcrumb navigation
    if (track.value) {
      nav.setNavItems([
        {
          label: 'Tracks',
          to: '/tracks',
        },
        {
          label: track.value.name,
        },
      ]);
    }
  } catch (error) {
    console.error('Failed to load track:', error);
    toast.error('Failed to load track');
  }
});
</script>

<route lang="yaml">
meta:
  title: Track Details
</route>
