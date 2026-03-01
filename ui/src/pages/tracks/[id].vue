<template>
  <div class="track-detail">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circle indeterminate />
    </div>

    <!-- <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div> -->

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
                <va-chip v-if="track.genomeType || track.genomeValue" size="small" outline>
                  {{ track.genomeType || '' }}{{ track.genomeValue ? ` (${track.genomeValue})` : '' }}
                </va-chip>
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
                <router-link :to="`/datasets/${track.dataset_file.dataset.id}`" class="va-link">
                  {{ track.dataset_file.dataset.name }}
                </router-link>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Dataset Type</span>
                <va-chip v-if="track.dataset_file.dataset.type" size="small" outline>
                  {{ formatDatasetType(track.dataset_file.dataset.type) }}
                </va-chip>
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
              <div class="flex justify-between">
                <span class="font-medium">Staged</span>
                <div v-if="track.dataset_file.dataset.is_staged">
                  <va-icon name="check_circle" color="success" />
                </div>
                <div v-else>
                  <va-icon name="cancel" color="danger" />
                </div>
              </div>
            </div>
            <div v-else class="text-center py-4">No dataset information available</div>
          </va-card-content>
        </va-card>
      </div>

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
                <va-chip v-if="rowData.genome_type || rowData.genome" size="small" outline>
                  {{ rowData.genome_type || '' }}{{ rowData.genome ? ` (${rowData.genome})` : '' }}
                </va-chip>
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
</template>

<script setup>
import * as datetime from '@/services/datetime';
import { formatDatasetType } from '@/services/sessionUtils';
import toast from '@/services/toast';
import trackService from '@/services/track';
import { useNavStore } from '@/stores/nav';
import { useTracksStore } from '@/stores/tracks';

const route = useRoute();
const tracksStore = useTracksStore();
const nav = useNavStore();

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
  // {
  //   key: "color",
  //   label: "Color",
  //   sortable: false,
  //   width: "15%",
  // },
];

const associatedSessions = computed(() => {
  return track.value.session_tracks.map((st) => st.session);
});

const formatFileSize = (bytes) => {
  if (!bytes) return 'Unknown';
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + ' ' + sizes[i];
};

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
  requiresRoles: ['operator', 'admin']
</route>
