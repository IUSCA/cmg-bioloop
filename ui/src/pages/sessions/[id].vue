<template>
  <div class="session-detail p-6">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circular indeterminate />
    </div>

    <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div>

    <div v-else-if="session" class="space-y-6">
      <!-- Breadcrumbs -->
      <div class="flex items-center space-x-2 text-sm">
        <router-link to="/sessions" class="hover:underline">Sessions</router-link>
        <span>/</span>
        <span>{{ session.title }}</span>
      </div>

      <!-- Header -->
      <div class="flex justify-between items-start">
        <div>
          <h1 class="text-3xl font-bold">{{ session.title }}</h1>
          <p class="text-gray-600 mt-2">
            Created by {{ session.user?.name || session.user?.username }} on {{ date(session.created_at) }}
          </p>
        </div>
        
        <!-- Action Buttons -->
        <div class="flex gap-3">
          <va-button
            v-if="canEditSession(session)"
            preset="primary"
            @click="editModal.show()"
          >
            <va-icon name="edit" />
            Edit Session
          </va-button>
          
          <va-button
            preset="secondary"
            @click="exportDataHub"
          >
            <va-icon name="download" />
            Export DataHub
          </va-button>
          
          <va-button
            v-if="canDeleteSession(session)"
            preset="danger"
            @click="deleteSession"
          >
            <va-icon name="delete" />
            Delete Session
          </va-button>
        </div>
      </div>

      <!-- Session info -->
      <va-card>
        <va-card-title>Session Information</va-card-title>
        <va-card-content>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label class="text-sm font-medium text-gray-600">Genome Type</label>
              <div class="mt-1">
                <va-chip size="small" preset="secondary">
                  {{ session.genome_type }}
                </va-chip>
              </div>
            </div>
            
            <div>
              <label class="text-sm font-medium text-gray-600">Genome</label>
              <div class="mt-1">
                <va-chip size="small" preset="primary">
                  {{ session.genome }}
                </va-chip>
              </div>
            </div>
            
            <div>
              <label class="text-sm font-medium text-gray-600">Visibility</label>
              <div class="mt-1">
                <va-chip size="small" :preset="session.is_public ? 'success' : 'warning'">
                  {{ session.is_public ? 'Public' : 'Private' }}
                </va-chip>
              </div>
            </div>
          </div>
        </va-card-content>
      </va-card>

      <!-- Tracks List -->
      <div class="bg-white rounded-lg shadow p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-semibold">Tracks ({{ session.session_tracks?.length || 0 }})</h2>
          <div class="flex gap-2">
            <va-button
              v-if="hasUnstagedTracks"
              preset="warning"
              size="small"
              @click="requestStaging"
              :loading="requestingStaging"
            >
              <va-icon name="info" />
              Check Staging Status
            </va-button>
          </div>
        </div>

        <div v-if="session.session_tracks?.length" class="space-y-3">
          <div
            v-for="sessionTrack in session.session_tracks"
            :key="sessionTrack.id"
            class="flex items-center justify-between p-3 border rounded-lg"
          >
            <div class="flex-1">
              <div class="font-medium">{{ sessionTrack.track.name }}</div>
              <div class="text-sm text-gray-600">
                {{ sessionTrack.track.file_type }} • 
                {{ sessionTrack.track.genomeType }} {{ sessionTrack.track.genomeValue }}
              </div>
              <div class="text-xs text-gray-500">
                Dataset: {{ sessionTrack.track.dataset_file?.dataset?.name }}
              </div>
              <div class="flex items-center gap-2 mt-1">
                <span class="text-xs px-2 py-1 rounded-full" 
                      :class="sessionTrack.track.dataset_file?.dataset?.is_staged 
                             ? 'bg-green-100 text-green-800' 
                             : 'bg-yellow-100 text-yellow-800'">
                  {{ sessionTrack.track.dataset_file?.dataset?.is_staged ? 'Staged' : 'Not Staged' }}
                </span>
                <span v-if="sessionTrack.color" class="text-xs px-2 py-1 rounded-full bg-gray-100">
                  Color: {{ sessionTrack.color }}
                </span>
              </div>
            </div>
            
            <div class="flex items-center gap-2">
              <va-button
                v-if="canEditSession(session)"
                preset="plain"
                size="small"
                @click="editModal.show()"
              >
                <va-icon name="edit" />
              </va-button>
            </div>
          </div>
        </div>

        <div v-else class="text-center text-gray-500 py-8">
          No tracks added to this session yet.
        </div>
      </div>

      <!-- Genome Browser Integration Placeholder -->
      <va-card>
        <va-card-title>Genome Browser View</va-card-title>
        <va-card-content>
          <div class="text-center py-12 text-gray-500">
            <va-icon name="mdi-dna" class="text-6xl mb-4" />
            <p class="text-lg">Genome Browser Integration</p>
            <p class="text-sm">This would integrate with a genome browser like IGV.js or JBrowse</p>
            <p class="text-sm">Session ID: {{ session.id }}</p>
          </div>
        </va-card-content>
      </va-card>
    </div>

    <!-- Edit Modal -->
    <edit-session-modal
      v-model="showEditModal"
      :session="session"
      @updated="handleSessionUpdated"
    />
  </div>
</template>

<script setup>
import EditSessionModal from '@/components/sessions/EditSessionModal.vue';
import api from '@/services/api';
import { date } from '@/services/datetime';
import { useAuthStore } from '@/stores/auth';
import { useSessionsStore } from '@/stores/sessions';
import { computed, onMounted, ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { toast } from 'vue-toastification';

const route = useRoute();
const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();

// Reactive state
const editModal = ref();
const requestingStaging = ref(false);

// Computed
const session = computed(() => sessionsStore.currentSession);
const loading = computed(() => sessionsStore.loading);
const error = computed(() => sessionsStore.error);

const canEditSession = computed(() => {
  return session.value?.user_id === auth.user?.id;
});

const canDeleteSession = computed(() => {
  return session.value?.user_id === auth.user?.id;
});

const hasUnstagedTracks = computed(() => {
  if (!session.value?.session_tracks) return false;
  return session.value.session_tracks.some(st => !st.track.dataset_file?.dataset?.is_staged);
});

// Methods
const deleteSession = async () => {
  if (!session.value) return;
  
  if (confirm('Are you sure you want to delete this session?')) {
    try {
      await sessionsStore.deleteSession(session.value.id);
      toast.success('Session deleted successfully');
      router.push('/sessions');
    } catch (error) {
      console.error('Failed to delete session:', error);
      toast.error('Failed to delete session');
    }
  }
};

const exportDataHub = async () => {
  try {
    const response = await api.get(`/sessions/${session.value.id}/datahub`);
    
    // Create a blob with the DataHub JSON data
    const blob = new Blob([JSON.stringify(response.data, null, 2)], {
      type: 'application/json',
    });
    
    // Create download link
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `session-${session.value.id}-datahub.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
    
    toast.success('DataHub export downloaded successfully');
  } catch (error) {
    console.error('Failed to export DataHub:', error);
    toast.error('Failed to export DataHub');
  }
};

const requestStaging = async () => {
  if (!session.value) return;
  
  requestingStaging.value = true;
  try {
    const response = await api.post(`/sessions/${session.value.id}/stage`);
    
    if (response.data.datasets && response.data.datasets.length > 0) {
      // Show which datasets need staging
      toast.info(`${response.data.datasets.length} datasets need staging. Use the dataset staging workflow to stage them individually.`);
      
      // You could also navigate to a datasets page or show a modal with staging options
      console.log('Datasets that need staging:', response.data.datasets);
    } else {
      toast.success('All datasets are already staged');
    }
  } catch (error) {
    console.error('Failed to check staging status:', error);
    toast.error('Failed to check staging status');
  } finally {
    requestingStaging.value = false;
  }
};

const loadSession = async () => {
  const sessionId = parseInt(route.params.id);
  if (isNaN(sessionId)) {
    router.push('/sessions');
    return;
  }
  
  try {
    await sessionsStore.fetchSession(sessionId);
  } catch (error) {
    // Error is handled by the store
  }
};

// Lifecycle
onMounted(() => {
  loadSession();
});
</script>

<route lang="yaml">
meta:
  title: Session Details
  requiresRoles: ["operator", "admin"]
  nav: [
    { label: "Sessions", to: "/sessions" },
    { label: "Session Details" }
  ]
</route> 