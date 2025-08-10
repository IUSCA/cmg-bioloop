<template>
  <va-modal
    v-model="visible"
    title="Edit Genome Browser Session"
    fixed-layout
    hide-default-actions
    size="large"
    okText="Save"
    cancelText="Cancel"
    @ok="handleSave"
    @cancel="handleCancel"
  >
    <div class="space-y-6">
      <!-- Basic session info -->
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
        />

        <va-checkbox
          v-model="form.is_public"
          label="Make session public"
        />
      </div>

      <!-- Track Management -->
      <div>
        <h3 class="text-lg font-medium mb-4">Manage Tracks</h3>
        
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
              <va-button
                preset="plain"
                size="small"
                @click="moveTrackUp(track.id)"
                :disabled="getTrackIndex(track.id) === 0"
              >
                <va-icon name="keyboard_arrow_up" />
              </va-button>
              <va-button
                preset="plain"
                size="small"
                @click="moveTrackDown(track.id)"
                :disabled="getTrackIndex(track.id) === selectedTrackIds.length - 1"
              >
                <va-icon name="keyboard_arrow_down" />
              </va-button>
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
    </div>
  </va-modal>
</template>

<script setup>
import constants from '@/constants';
import { useSessionsStore } from '@/stores/sessions';
import { useTracksStore } from '@/stores/tracks';
import { computed, ref, watch } from 'vue';

const props = defineProps({
  modelValue: { type: Boolean, required: true },
  session: { type: Object, default: null },
});

const emit = defineEmits(['update:modelValue', 'updated']);

const sessionsStore = useSessionsStore();
const tracksStore = useTracksStore();

// Reactive state
const visible = ref(false);
const form = ref({
  session_name: '',
  genome_type: '',
  genome: '',
  is_public: false,
});
const errors = ref({});
const trackSearch = ref('');
const selectedTrackIds = ref([]);
const trackColors = ref({}); // New state for track colors

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
watch(() => props.modelValue, (newValue) => {
  visible.value = newValue;
  if (newValue && props.session) {
    initializeForm();
  }
});

watch(visible, (newValue) => {
  emit('update:modelValue', newValue);
});

watch(() => form.value.genome_type, () => {
  // Reset genome when genome type changes
  form.value.genome = '';
});

// Methods
const initializeForm = () => {
  if (!props.session) return;
  
  form.value = {
    session_name: props.session.title || '',
    genome_type: props.session.genome_type || '',
    genome: props.session.genome || '',
    is_public: props.session.is_public || false,
  };
  
  // Set selected tracks from session
  selectedTrackIds.value = props.session.session_tracks?.map(st => st.track_id) || [];
  trackColors.value = props.session.session_tracks?.reduce((acc, st) => ({
    ...acc,
    [st.track_id]: st.color || '#000000', // Initialize with existing colors or default
  }), {});
  
  errors.value = {};
  trackSearch.value = '';
  
  // Load available tracks
  loadTracks();
};

const loadTracks = async () => {
  try {
    await tracksStore.fetchTracks();
  } catch (error) {
    console.error('Failed to load tracks:', error);
  }
};

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
    // Set default color if not already set
    if (!trackColors.value[trackId]) {
      trackColors.value[trackId] = '#000000';
    }
  }
};

const removeTrack = (trackId) => {
  const index = selectedTrackIds.value.indexOf(trackId);
  if (index > -1) {
    selectedTrackIds.value.splice(index, 1);
    delete trackColors.value[trackId];
  }
};

const moveTrackUp = (trackId) => {
  const index = selectedTrackIds.value.indexOf(trackId);
  if (index > 0) {
    const temp = selectedTrackIds.value[index];
    selectedTrackIds.value[index] = selectedTrackIds.value[index - 1];
    selectedTrackIds.value[index - 1] = temp;
  }
};

const moveTrackDown = (trackId) => {
  const index = selectedTrackIds.value.indexOf(trackId);
  if (index < selectedTrackIds.value.length - 1) {
    const temp = selectedTrackIds.value[index];
    selectedTrackIds.value[index] = selectedTrackIds.value[index + 1];
    selectedTrackIds.value[index + 1] = temp;
  }
};

const getTrackIndex = (trackId) => {
  return selectedTrackIds.value.indexOf(trackId);
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
  
  return Object.keys(errors.value).length === 0;
};

const handleSave = async () => {
  if (!validateForm() || !props.session) {
    return;
  }
  
  try {
    const sessionData = {
      session_name: form.value.session_name.trim(),
      genome_type: form.value.genome_type,
      genome: form.value.genome,
      is_public: form.value.is_public,
      track_ids: selectedTrackIds.value,
      track_colors: Object.entries(trackColors.value).map(([id, color]) => ({
        track_id: id,
        color: color,
      })),
    };
    
    const session = await sessionsStore.updateSession(props.session.id, sessionData);
    emit('updated', session);
    visible.value = false;
  } catch (error) {
    console.error('Failed to update session:', error);
  }
};

const handleCancel = () => {
  visible.value = false;
};
</script> 