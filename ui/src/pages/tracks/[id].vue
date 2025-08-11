<template>
  <div class="track-detail p-6">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circular indeterminate />
    </div>

    <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div>

    <div v-else-if="track" class="space-y-6">
      <!-- Breadcrumbs -->
      <div class="flex items-center space-x-2 text-sm">
        <router-link to="/tracks" class="hover:underline">Tracks</router-link>
        <span>/</span>
        <span>{{ track.name }}</span>
      </div>

      <!-- Header -->
      <div class="flex justify-between items-start">
        <div>
          <h1 class="text-3xl font-bold">{{ track.name }}</h1>
          <p class="text-gray-600 mt-2">
            Track ID: {{ track.id }} • Created {{ datetime.fromNow(track.created_at) }}
          </p>
        </div>
        
        <!-- Action Buttons -->
        <div class="flex gap-3">
          <va-button
            v-if="auth.canOperate"
            preset="primary"
            @click="editModal.show()"
          >
            <va-icon name="edit" />
            Edit Track
          </va-button>
          
          <va-button
            preset="secondary"
            @click="addToSession"
          >
            <va-icon name="plus" />
            Add to Session
          </va-button>
          
          <va-button
            v-if="auth.canOperate"
            preset="danger"
            @click="deleteTrack"
          >
            <va-icon name="delete" />
            Delete Track
          </va-button>
        </div>
      </div>

      <!-- Track Information Grid -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Basic Information -->
        <va-card>
          <va-card-title>Basic Information</va-card-title>
          <va-card-content>
            <div class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Track Name:</span>
                <span>{{ track.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Type:</span>
                <va-chip :color="getFileTypeColor(track.file_type)" size="small">
                  {{ track.file_type?.toUpperCase() || 'Unknown' }}
                </va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Type:</span>
                <va-chip outline size="small">{{ track.genomeType }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Version:</span>
                <va-chip outline size="small">{{ track.genomeValue }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created:</span>
                <span>{{ datetime.date(track.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated:</span>
                <span>{{ datetime.fromNow(track.updated_at) }}</span>
              </div>
            </div>
          </va-card-content>
        </va-card>

        <!-- Dataset Information -->
        <va-card>
          <va-card-title>Dataset Information</va-card-title>
          <va-card-content>
            <div v-if="track.dataset_file?.dataset" class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Dataset Name:</span>
                <router-link 
                  :to="`/datasets/${track.dataset_file.dataset.id}`"
                  class="va-link"
                >
                  {{ track.dataset_file.dataset.name }}
                </router-link>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Dataset Type:</span>
                <span>{{ track.dataset_file.dataset.type }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Name:</span>
                <span>{{ track.dataset_file.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Size:</span>
                <span>{{ formatFileSize(track.dataset_file.size) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Path:</span>
                <span class="text-sm text-gray-600 font-mono break-all">
                  {{ track.dataset_file.path }}
                </span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Staging Status:</span>
                <va-chip 
                  :color="track.dataset_file.dataset.is_staged ? 'success' : 'warning'"
                  size="small"
                >
                  {{ track.dataset_file.dataset.is_staged ? 'Staged' : 'Not Staged' }}
                </va-chip>
              </div>
            </div>
            <div v-else class="text-center text-gray-500 py-4">
              No dataset information available
            </div>
          </va-card-content>
        </va-card>
      </div>

      <!-- Project Associations -->
      <va-card v-if="track.dataset_file?.dataset?.projects?.length">
        <va-card-title>Project Associations</va-card-title>
        <va-card-content>
          <div class="space-y-2">
            <div
              v-for="projectAssoc in track.dataset_file.dataset.projects"
              :key="projectAssoc.project.id"
              class="flex items-center justify-between p-3 border rounded-lg"
            >
              <div class="flex-1">
                <router-link 
                  :to="`/projects/${projectAssoc.project.slug}`"
                  class="va-link font-medium"
                >
                  {{ projectAssoc.project.name }}
                </router-link>
              </div>
              <div class="flex items-center gap-2">
                <va-chip size="small" outline>Project</va-chip>
              </div>
            </div>
          </div>
        </va-card-content>
      </va-card>

      <!-- Track Usage -->
      <va-card>
        <va-card-title>Track Usage</va-card-title>
        <va-card-content>
          <div v-if="track.session_tracks?.length" class="space-y-3">
            <div class="text-sm text-gray-600 mb-3">
              This track is used in {{ track.session_tracks.length }} session(s)
            </div>
            <div
              v-for="sessionTrack in track.session_tracks"
              :key="sessionTrack.id"
              class="flex items-center justify-between p-3 border rounded-lg"
            >
              <div class="flex-1">
                <router-link 
                  :to="`/sessions/${sessionTrack.session.id}`"
                  class="va-link font-medium"
                >
                  {{ sessionTrack.session.title }}
                </router-link>
                <div class="text-sm text-gray-600">
                  Created by {{ sessionTrack.session.user?.name || sessionTrack.session.user?.username }}
                </div>
                <div class="text-xs text-gray-500">
                  {{ datetime.fromNow(sessionTrack.session.created_at) }}
                </div>
              </div>
              <div class="flex items-center gap-2">
                <span v-if="sessionTrack.color" class="text-sm text-gray-500">
                  Color: {{ sessionTrack.color }}
                </span>
                <span class="text-sm text-gray-500">
                  Order: {{ sessionTrack.order + 1 }}
                </span>
              </div>
            </div>
          </div>
          <div v-else class="text-center text-gray-500 py-8">
            This track is not used in any sessions yet.
          </div>
        </va-card-content>
      </va-card>

      <!-- Track Preview Placeholder -->
      <va-card>
        <va-card-title>Track Preview</va-card-title>
        <va-card-content>
          <div class="text-center py-12 text-gray-500">
            <va-icon name="mdi-chart-gantt" class="text-6xl mb-4" />
            <p class="text-lg">Track Visualization</p>
            <p class="text-sm">This would show a preview of the track data</p>
            <p class="text-sm">File: {{ track.dataset_file?.name }}</p>
          </div>
        </va-card-content>
      </va-card>
    </div>

    <!-- Edit Modal -->
    <edit-track-modal
      v-model="showEditModal"
      :track="track"
      @updated="handleTrackUpdated"
    />
  </div>
</template>

<script setup>
import EditTrackModal from '@/components/tracks/EditTrackModal.vue';
import * as datetime from '@/services/datetime';
import { useAuthStore } from '@/stores/auth';
import { useTracksStore } from '@/stores/tracks';
import { computed, onMounted, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { toast } from 'vue-toastification';

const route = useRoute();
const router = useRouter();
const tracksStore = useTracksStore();
const auth = useAuthStore();

// Reactive state
const editModal = ref();
const showEditModal = ref(false);

// Computed
const track = computed(() => tracksStore.currentTrack);
const loading = computed(() => tracksStore.loading);
const error = computed(() => tracksStore.error);

// Methods
const getFileTypeColor = (fileType) => {
  const colors = {
    bam: 'primary',
    bigwig: 'success',
    bw: 'success',
    vcf: 'warning',
    bed: 'info',
    gtf: 'secondary',
  };
  return colors[fileType] || 'secondary';
};

const formatFileSize = (bytes) => {
  if (!bytes) return 'Unknown';
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
};

const addToSession = () => {
  // Navigate to create session page with this track pre-selected
  router.push({
    path: '/sessions/new',
    query: { track_id: track.value.id }
  });
};

const deleteTrack = async () => {
  if (!confirm('Are you sure you want to delete this track? This action cannot be undone.')) {
    return;
  }

  try {
    await tracksStore.deleteTrack(track.value.id);
    toast.success('Track deleted successfully');
    router.push('/tracks');
  } catch (error) {
    console.error('Failed to delete track:', error);
    toast.error('Failed to delete track');
  }
};

const handleTrackUpdated = (updatedTrack) => {
  toast.success('Track updated successfully');
  // Refresh the track data
  tracksStore.fetchTrack(route.params.id);
};

// Load track data
onMounted(async () => {
  try {
    await tracksStore.fetchTrack(route.params.id);
  } catch (error) {
    console.error('Failed to load track:', error);
    toast.error('Failed to load track');
  }
});
</script>

<route lang="yaml">
meta:
  title: Track Details
  requiresRoles: ["operator", "admin"]
  nav: [
    { label: "Tracks", to: "/tracks" },
    { label: "Track Details" }
  ]
</route>
