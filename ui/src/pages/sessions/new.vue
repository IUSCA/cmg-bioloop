<template>
  <div class="create-session p-6">
    <!-- Breadcrumbs -->
    <div class="flex items-center space-x-2 text-sm mb-6">
      <router-link to="/sessions" class="hover:underline">Sessions</router-link>
      <span>/</span>
      <span>Create New Session</span>
    </div>

    <div class="max-w-4xl mx-auto">
      <h1 class="text-3xl font-bold mb-6">Create New Genome Browser Session</h1>

      <va-card>
        <va-card-content>
          <form @submit.prevent="handleSubmit" class="space-y-6">
            <!-- Basic Session Info -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <va-input
                v-model="form.session_name"
                label="Session Name"
                placeholder="Enter session name"
                :error="errors.session_name"
                required
              />

              <va-select
                v-model="form.genome_type"
                label="Genome Type"
                placeholder="Select genome type"
                :options="genomeTypeOptions"
                text-by="name"
                value-by="id"
                :error="errors.genome_type"
                required
              />

              <va-select
                v-model="form.genome"
                label="Genome"
                placeholder="Select genome"
                :options="genomeOptions"
                text-by="name"
                value-by="id"
                :error="errors.genome"
                required
                :disabled="!form.genome_type"
              />

              <div class="flex items-center">
                <va-checkbox
                  v-model="form.is_public"
                  label="Make session public"
                />
              </div>
            </div>

            <!-- Track Selection -->
            <div>
              <h3 class="text-lg font-medium mb-4">Select Tracks</h3>
              
              <div class="mb-4">
                <va-input
                  v-model="trackSearch"
                  placeholder="Search tracks..."
                  class="w-full"
                />
              </div>

              <div v-if="availableTracks.length > 0" class="max-h-64 overflow-y-auto border rounded p-4">
                <div
                  v-for="track in availableTracks"
                  :key="track.id"
                  class="flex items-center justify-between p-2 hover:bg-gray-50 rounded"
                >
                  <div class="flex-1">
                    <div class="font-medium">{{ track.name }}</div>
                    <div class="text-sm text-gray-600">
                      {{ track.file_type }} • {{ track.genomeType }} {{ track.genomeValue }}
                    </div>
                    <div class="text-xs text-gray-500">
                      Dataset: {{ track.dataset_file?.dataset?.name }}
                    </div>
                  </div>
                  
                  <div class="flex items-center gap-2">
                    <va-input
                      v-model="trackColors[track.id]"
                      placeholder="Color"
                      class="w-20"
                    />
                    <va-checkbox
                      :model-value="selectedTrackIds.includes(track.id)"
                      @update:model-value="toggleTrack(track.id)"
                    />
                  </div>
                </div>
              </div>

              <div v-else-if="trackSearch" class="text-center text-gray-500 py-8">
                No tracks found matching your search.
              </div>

              <div v-else class="text-center text-gray-500 py-8">
                No tracks available. You need access to tracks through projects.
              </div>
            </div>

            <!-- Selected Tracks Preview -->
            <div v-if="selectedTracks.length > 0">
              <h3 class="text-lg font-medium mb-4">Selected Tracks ({{ selectedTracks.length }})</h3>
              <div class="space-y-2">
                <div
                  v-for="(track, index) in selectedTracks"
                  :key="track.id"
                  class="flex items-center justify-between p-2 bg-gray-50 rounded"
                >
                  <div class="flex-1">
                    <div class="font-medium">{{ track.name }}</div>
                    <div class="text-sm text-gray-600">
                      {{ track.file_type }} • {{ track.genomeType }} {{ track.genomeValue }}
                    </div>
                  </div>
                  
                  <div class="flex items-center gap-2">
                    <span class="text-sm text-gray-500">Order: {{ index + 1 }}</span>
                    <span v-if="trackColors[track.id]" class="text-sm text-gray-500">
                      Color: {{ trackColors[track.id] }}
                    </span>
                    <va-button
                      preset="plain"
                      color="danger"
                      size="small"
                      @click="removeTrack(track.id)"
                    >
                      <va-icon name="delete" />
                    </va-button>
                  </div>
                </div>
              </div>
            </div>

            <!-- Form Actions -->
            <div class="flex justify-end gap-3 pt-6 border-t">
              <va-button
                preset="secondary"
                @click="router.push('/sessions')"
              >
                Cancel
              </va-button>
              
              <va-button
                type="submit"
                preset="primary"
                :loading="loading"
                :disabled="!canSubmit"
              >
                Create Session
              </va-button>
            </div>
          </form>
        </va-card-content>
      </va-card>
    </div>
  </div>
</template>

<script setup>
import constants from '@/constants';
import toast from '@/services/toast';
import { useSessionsStore } from '@/stores/sessions';
import { useTracksStore } from '@/stores/tracks';
import { computed, onMounted, ref, watch } from 'vue';
import { useRouter } from 'vue-router';

const router = useRouter();
const sessionsStore = useSessionsStore();
const tracksStore = useTracksStore();

// Reactive state
const loading = ref(false);
const form = ref({
  session_name: '',
  genome_type: '',
  genome: '',
  is_public: false,
});
const errors = ref({});
const trackSearch = ref('');
const selectedTrackIds = ref([]);
const trackColors = ref({});

// Computed
const genomeTypeOptions = computed(() => {
  return Object.keys(constants.GENOME_TYPES).map(type => ({
    name: type.charAt(0).toUpperCase() + type.slice(1),
    id: type,
  }));
});

const genomeOptions = computed(() => {
  if (!form.value.genome_type) return [];
  
  const genomes = constants.GENOME_TYPES[form.value.genome_type]?.genomes || [];
  return genomes.map(genome => ({
    name: genome,
    id: genome,
  }));
});

const availableTracks = computed(() => {
  return tracksStore.tracks.filter(track => {
    // Filter by search term
    if (trackSearch.value) {
      const searchLower = trackSearch.value.toLowerCase();
      if (!track.name.toLowerCase().includes(searchLower) &&
          !track.file_type?.toLowerCase().includes(searchLower)) {
        return false;
      }
    }
    
    // Filter by genome type if selected
    if (form.value.genome_type && track.genomeType !== form.value.genome_type) {
      return false;
    }
    
    return true;
  });
});

const selectedTracks = computed(() => {
  return availableTracks.value.filter(track => selectedTrackIds.value.includes(track.id));
});

const canSubmit = computed(() => {
  return form.value.session_name.trim() && 
         form.value.genome_type && 
         form.value.genome &&
         selectedTrackIds.value.length > 0;
});

// Watchers
watch(() => form.value.genome_type, () => {
  // Reset genome when genome type changes
  form.value.genome = '';
});

// Methods
const searchTracks = () => {
  // Search is handled by computed property
};

const toggleTrack = (trackId) => {
  const index = selectedTrackIds.value.indexOf(trackId);
  if (index > -1) {
    selectedTrackIds.value.splice(index, 1);
    delete trackColors.value[trackId];
  } else {
    selectedTrackIds.value.push(trackId);
    // Set default color
    trackColors.value[trackId] = '#000000';
  }
};

const removeTrack = (trackId) => {
  const index = selectedTrackIds.value.indexOf(trackId);
  if (index > -1) {
    selectedTrackIds.value.splice(index, 1);
    delete trackColors.value[trackId];
  }
};

const validateForm = () => {
  errors.value = {};
  
  if (!form.value.session_name.trim()) {
    errors.value.session_name = 'Session name is required';
  }
  
  if (!form.value.genome_type) {
    errors.value.genome_type = 'Genome type is required';
  }
  
  if (!form.value.genome) {
    errors.value.genome = 'Genome is required';
  }
  
  if (selectedTrackIds.value.length === 0) {
    errors.value.tracks = 'At least one track is required';
  }
  
  return Object.keys(errors.value).length === 0;
};

const handleSubmit = async () => {
  if (!validateForm()) {
    return;
  }
  
  loading.value = true;
  
  try {
    const sessionData = {
      session_name: form.value.session_name,
      genome_type: form.value.genome_type,
      genome: form.value.genome,
      is_public: form.value.is_public,
      track_ids: selectedTrackIds.value,
      track_colors: Object.entries(trackColors.value).map(([id, color]) => ({
        track_id: id,
        color: color,
      })),
    };
    
    const session = await sessionsStore.createSession(sessionData);
    toast.success('Session created successfully!');
    router.push(`/sessions/${session.id}`);
  } catch (error) {
    console.error('Failed to create session:', error);
    toast.error('Failed to create session');
  } finally {
    loading.value = false;
  }
};

const loadTracks = async () => {
  try {
    await tracksStore.fetchTracks();
  } catch (error) {
    console.error('Failed to load tracks:', error);
    toast.error('Failed to load tracks');
  }
};

// Lifecycle
onMounted(() => {
  loadTracks();
});
</script>

<route lang="yaml">
meta:
  title: Create Session
  requiresRoles: ["operator", "admin"]
  nav: [
    { label: "Sessions", to: "/sessions" },
    { label: "Create Session" }
  ]
</route>

