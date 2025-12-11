<template>
  <div class="create-session p-6">
    <!-- Breadcrumbs -->
    <div class="flex items-center space-x-2 text-sm mb-6">
      <router-link to="/sessions" class="hover:underline">Sessions</router-link>
      <span>/</span>
      <span>Create New Session</span>
    </div>

    <div class="max-w-4xl mx-auto">
      <!-- <h1 class="text-3xl font-bold mb-6">Create New Genome Browser Session</h1> -->

      <va-card>
        <va-card-content>
          <form @submit.prevent="handleSubmit" class="space-y-6">
            <!-- Basic Session Info -->
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
              <va-input
                v-model="form.session_name"
                label="Session Name"
                placeholder="Enter session name"
                :rules="[
                  (value) => !!value || 'Session name is required',
                  debouncedValidateSessionName,
                ]"
              />

              <va-input
                v-model="form.genome_type"
                label="Genome Type"
                placeholder="Auto-populated if tracks have consistent genome info"
              />

              <va-input
                v-model="form.genome"
                label="Genome Assembly"
                placeholder="Auto-populated if tracks have consistent genome info"
              />

              <div class="flex items-center">
                <va-checkbox v-model="form.is_public" label="Make session public" />
              </div>
            </div>

            <!-- File Selection -->
            <div>
              <h3 class="text-lg font-medium mb-4">Select Tracks</h3>

              <p class="text-sm mb-4">
                Select tracks from Data Products. Genome fields are auto-populated if all selected
                tracks have consistent genome information.
              </p>

              <va-form-field
                :model-value="selectedTracks"
                :rules="[() => selectedTracks.length > 0 || 'At least one track must be selected']"
              >
                <TracksAsyncAutoComplete
                  v-model:search-term="trackSearch"
                  label="Search and Add Tracks"
                  placeholder="Search tracks by name"
                  multiple
                  @select="addTrack"
                />
              </va-form-field>

              <!-- Selected tracks table -->
              <div v-if="selectedTracks.length > 0" class="space-y-3 mt-4">
                <div class="flex items-center justify-between">
                  <div>
                    {{ selectedTracks.length }} track{{ selectedTracks.length !== 1 ? 's' : '' }}
                    selected
                  </div>
                </div>

                <va-scroll-container vertical style="max-height: 300px">
                  <va-data-table
                    :items="selectedTracksTableData"
                    :columns="selectedTracksColumns"
                    class="selected-tracks-table"
                  >
                    <template #cell(name)="{ value, rowData }">
                      <div class="text-sm">
                        <router-link
                          v-if="rowData.id"
                          :to="`/tracks/${rowData.id}`"
                          target="_blank"
                          class="text-primary hover:underline"
                        >
                          {{ value }}
                        </router-link>
                        <span v-else>{{ value }}</span>
                      </div>
                    </template>

                    <template #cell(genome)="{ rowData }">
                      <div class="text-sm">
                        {{
                          (rowData.genome_type || '') +
                          (rowData.genome_type && rowData.genome_value ? ' ' : '') +
                          (rowData.genome_value || '')
                        }}
                      </div>
                    </template>

                    <template #cell(dataset)="{ rowData }">
                      <div class="text-sm">
                        <router-link
                          v-if="rowData.dataset?.id"
                          :to="`/datasets/${rowData.dataset.id}`"
                          target="_blank"
                          class="text-primary hover:underline"
                        >
                          {{ rowData.dataset.name || '' }}
                        </router-link>
                        <span v-else>{{ rowData.dataset?.name || '' }}</span>
                      </div>
                    </template>

                    <template #cell(size)="{ rowData }">
                      <div class="text-sm">
                        {{ formatBytes(rowData.size) }}
                      </div>
                    </template>

                    <template #cell(actions)="{ rowData }">
                      <va-button size="small" plain color="danger" @click="removeTrack(rowData.id)">
                        <va-icon name="delete" />
                      </va-button>
                    </template>
                  </va-data-table>
                </va-scroll-container>

                <!-- Track Validation Alert -->
                <div v-if="trackValidationAlert.show" class="mt-4">
                  <va-alert
                    :color="trackValidationAlert.type === 'error' ? 'danger' : 'warning'"
                    class="mb-4"
                  >
                    <template #title>
                      <div class="flex items-center gap-2">
                        <va-icon name="warning" />
                        <span>Track Selection Issue</span>
                      </div>
                    </template>
                    <template #default>
                      {{ trackValidationAlert.message }}
                    </template>
                  </va-alert>
                </div>
              </div>
            </div>

            <!-- Form Actions -->
            <div class="flex justify-end gap-3 pt-6 border-t">
              <va-button preset="secondary" @click="router.push('/sessions')"> Cancel </va-button>

              <va-button type="submit" preset="primary" :loading="loading" :disabled="!canSubmit">
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
import TracksAsyncAutoComplete from '@/components/tracks/TracksAsyncAutoComplete.vue';
import api from '@/services/api';
import toast from '@/services/toast';
import { formatBytes } from '@/services/utils';
import { useSessionsStore } from '@/stores/sessions';
import _ from 'lodash';
import { computed, onMounted, ref } from 'vue';
import { useRouter } from 'vue-router';

const router = useRouter();
const sessionsStore = useSessionsStore();

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
const selectedTrack = ref(null);
const selectedTracks = ref([]);
const trackValidationAlert = ref({ show: false, message: '', type: 'error' });

// Table columns for selected files
const selectedTracksColumns = [
  {
    key: 'name',
    label: 'Track Name',
    sortable: true,
    width: '30%',
  },
  {
    key: 'genome',
    label: 'Genome',
    sortable: true,
    width: '25%',
  },
  {
    key: 'dataset',
    label: 'Dataset',
    sortable: true,
    width: '25%',
  },
  {
    key: 'size',
    label: 'Size',
    sortable: true,
    width: '10%',
  },
  {
    key: 'actions',
    label: 'Actions',
    sortable: false,
    width: '10%',
  },
];

const selectedTracksTableData = computed(() => {
  return selectedTracks.value.map((track) => ({
    ...track,
    name: track.name || '',
    genome_type: track.dataset_file?.dataset?.genomic_details?.genome_type || null,
    genome_value: track.dataset_file?.dataset?.genomic_details?.genome_value || null,
    dataset: track.dataset_file?.dataset || null,
    size: track.dataset_file?.size || null,
  }));
});

const canSubmit = computed(() => {
  return form.value.session_name.trim() && selectedTracks.value.length > 0;
});

// Async validation for session name uniqueness
const validateSessionNameUnique = async (value) => {
  if (!value || !value.trim()) {
    return true; // Let the required validation handle empty values
  }
  try {
    const response = await api.get(`/sessions/check-name/${encodeURIComponent(value.trim())}`);
    if (response.data.exists) {
      return 'A session with this name already exists';
    }
    return true;
  } catch (error) {
    console.error('Error checking session name:', error);
    return true; // Don't block form submission on API errors
  }
};

// Debounced version to avoid too many API calls
const debouncedValidateSessionName = _.debounce(validateSessionNameUnique, 500);

// Methods

const addTrack = (track) => {
  // Check if track is already selected
  const existingIndex = selectedTracks.value.findIndex((t) => t.id === track.id);
  if (existingIndex === -1) {
    // Note: We allow tracks without genome information - they can still be added to sessions

    // Validate track before adding
    const validationResult = validateTrackForSession(track);

    if (validationResult.isValid) {
      // Add track to selected tracks
      selectedTracks.value.push(track);
      // Clear the selected track for next selection
      selectedTrack.value = null;
      // Clear search term
      trackSearch.value = '';

      // Auto-populate genome fields based on candidate datasets logic
      updateGenomeFields();

      // Clear any previous validation alerts
      trackValidationAlert.value = { show: false, message: '', type: 'error' };
    } else {
      // Show validation alert instead of toast
      trackValidationAlert.value = {
        show: true,
        message: validationResult.error,
        type: 'error',
      };
    }
  }
};

// Track validation functions
const validateTrackForSession = (track) => {
  // Get genome details from track's dataset
  const trackGenomeDetails = track.dataset_file?.dataset?.genomic_details;

  // Validation 1: Check genome type consistency (MANDATORY)
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    const firstGenomeDetails = firstTrack.dataset_file?.dataset?.genomic_details;

    // Only validate if both tracks have genome_type defined
    if (
      trackGenomeDetails?.genome_type &&
      firstGenomeDetails?.genome_type &&
      trackGenomeDetails.genome_type !== firstGenomeDetails.genome_type
    ) {
      return {
        isValid: false,
        error: `Cannot mix different genome types. First track has "${firstGenomeDetails.genome_type}", this track has "${trackGenomeDetails.genome_type}". Please create separate sessions.`,
      };
    }
  }

  // Validation 2: Check genome value consistency
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    const firstGenomeDetails = firstTrack.dataset_file?.dataset?.genomic_details;

    // Only validate if both tracks have genome_value defined
    if (
      trackGenomeDetails?.genome_value &&
      firstGenomeDetails?.genome_value &&
      trackGenomeDetails.genome_value !== firstGenomeDetails.genome_value
    ) {
      return {
        isValid: false,
        error: `Cannot mix different genome assemblies. First track has "${firstGenomeDetails.genome_value}", this track has "${trackGenomeDetails.genome_value}". Please create separate sessions or manually select one assembly.`,
      };
    }
  }

  return { isValid: true };
};

const validateAllTracks = () => {
  if (selectedTracks.value.length === 0) {
    return { isValid: false, error: 'No tracks selected' };
  }

  // Check if all tracks have the same genome type and value
  const firstTrack = selectedTracks.value[0];
  const firstGenomeDetails = firstTrack.dataset_file?.dataset?.genomic_details;

  const allConsistent = selectedTracks.value.every((track) => {
    const genomeDetails = track.dataset_file?.dataset?.genomic_details;

    // Allow tracks with missing genome info, only check consistency when both have values
    const genomeTypeMatch =
      !genomeDetails?.genome_type ||
      !firstGenomeDetails?.genome_type ||
      genomeDetails.genome_type === firstGenomeDetails.genome_type;
    const genomeValueMatch =
      !genomeDetails?.genome_value ||
      !firstGenomeDetails?.genome_value ||
      genomeDetails.genome_value === firstGenomeDetails.genome_value;

    return genomeTypeMatch && genomeValueMatch;
  });

  if (!allConsistent) {
    return {
      isValid: false,
      error:
        'All tracks must have the same genome type and assembly. Cannot mix different organisms or genome builds in one session.',
    };
  }

  return { isValid: true };
};

// Update genome fields based on candidate datasets logic
const updateGenomeFields = () => {
  if (selectedTracks.value.length === 0) {
    form.value.genome_type = '';
    form.value.genome = '';
    return;
  }

  // Get all unique (genome_type, genome_value) pairs from candidate datasets
  // Tracks have dataset_file.dataset.genomic_details
  const genomes = new Set();
  selectedTracks.value.forEach((track) => {
    const genomeDetails = track.dataset_file?.dataset?.genomic_details;
    // Only add to set if both genome_type and genome_value exist
    if (genomeDetails?.genome_type && genomeDetails?.genome_value) {
      genomes.add(`${genomeDetails.genome_type}|${genomeDetails.genome_value}`);
    }
  });

  // If exactly one unique genome pair, auto-populate
  if (genomes.size === 1) {
    const [genome_type, genome_value] = Array.from(genomes)[0].split('|');
    form.value.genome_type = genome_type;
    form.value.genome = genome_value;
  } else {
    // Multiple or no genome pairs - leave blank for user input
    form.value.genome_type = '';
    form.value.genome = '';
  }
};

const removeTrack = (trackId) => {
  const index = selectedTracks.value.findIndex((t) => t.id === trackId);
  if (index > -1) {
    selectedTracks.value.splice(index, 1);

    // If this was the last track, clear genome fields
    if (selectedTracks.value.length === 0) {
      form.value.genome_type = '';
      form.value.genome = '';
    }
    // Update genome fields after removing track
    updateGenomeFields();

    // Clear validation alerts when tracks are removed
    trackValidationAlert.value = { show: false, message: '', type: 'error' };
  }
};

const validateForm = () => {
  errors.value = {};

  // Validate tracks (this will ensure genome fields are populated)
  const trackValidation = validateAllTracks();
  if (!trackValidation.isValid) {
    errors.value.tracks = trackValidation.error;
  }

  // Genome fields are optional - no additional validation needed

  // Clear validation alerts if form is valid
  if (Object.keys(errors.value).length === 0) {
    trackValidationAlert.value = { show: false, message: '', type: 'error' };
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
      track_ids: selectedTracks.value.map((track) => track.id),
    };

    const session = await sessionsStore.createSession(sessionData);
    toast.success('Session created successfully!');
    router.push(`/sessions/${session.id}`);
  } catch (error) {
    console.error('Failed to create session:', error);

    // Provide more specific error messages
    if (error.response?.data?.error) {
      toast.error(`Failed to create session: ${error.response.data.error}`);
    } else if (error.message) {
      toast.error(`Failed to create session: ${error.message}`);
    } else {
      toast.error('Failed to create session. Please try again.');
    }
  } finally {
    loading.value = false;
  }
};

// Lifecycle
onMounted(() => {
  // Component will handle track loading automatically
});
</script>

<route lang="yaml">
meta:
  title: Create Session
  requiresRoles: ['operator', 'admin']
  nav: [{ label: 'Sessions', to: '/sessions' }, { label: 'Create Session' }]
</route>
