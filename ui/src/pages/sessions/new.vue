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

              <va-input
                v-model="form.genome_type"
                label="Genome Type"
                placeholder="Will be set automatically from selected tracks"
                :error="errors.genome_type"
                readonly
                class="bg-gray-50"
              />

              <va-input
                v-model="form.genome"
                label="Genome Assembly"
                placeholder="Will be set automatically from selected tracks"
                :error="errors.genome"
                readonly
                class="bg-gray-50"
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

              <p class="text-sm mb-4">
                All tracks must have the same genome type (organism), assembly,
                and file type. Only compatible file types (BAM, VCF, BigWig,
                FASTQ) are shown. Genome fields are automatically populated from
                the first selected track.
              </p>

              <TracksAsyncAutoComplete
                v-model:selected="selectedTrack"
                v-model:search-term="trackSearch"
                label="Search and Add Tracks"
                placeholder="Search tracks by name"
                @select="addTrack"
              />

              <!-- Selected tracks table -->
              <div v-if="selectedTracks.length > 0" class="space-y-3 mt-4">
                <div class="flex items-center justify-between">
                  <div>
                    {{ selectedTracks.length }} track{{
                      selectedTracks.length !== 1 ? "s" : ""
                    }}
                    selected
                  </div>
                </div>

                <va-data-table
                  :items="selectedTracksTableData"
                  :columns="selectedTracksColumns"
                  class="selected-tracks-table"
                >
                  <template #cell(name)="{ value }">
                    <div class="text-sm">{{ value }}</div>
                  </template>

                  <template #cell(file_type)="{ value }">
                    <div class="text-sm">{{ value }}</div>
                  </template>

                  <template #cell(genome)="{ rowData }">
                    <div class="text-sm">
                      {{ rowData.genomeType }} {{ rowData.genomeValue }}
                    </div>
                  </template>

                  <template #cell(dataset)="{ rowData }">
                    <div class="text-sm">
                      {{ rowData.dataset_file?.dataset?.name || "N/A" }}
                    </div>
                  </template>

                  <template #cell(color)="{ rowData }">
                    <va-input
                      v-model="trackColors[rowData.id]"
                      placeholder="Color"
                      class="w-20"
                    />
                  </template>

                  <template #cell(actions)="{ rowData }">
                    <va-button
                      size="small"
                      plain
                      color="danger"
                      @click="removeTrack(rowData.id)"
                    >
                      <va-icon name="delete" />
                    </va-button>
                  </template>
                </va-data-table>

                <!-- Track Validation Alert -->
                <div v-if="trackValidationAlert.show" class="mt-4">
                  <va-alert
                    :color="
                      trackValidationAlert.type === 'error'
                        ? 'danger'
                        : 'warning'
                    "
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
              <va-button preset="secondary" @click="router.push('/sessions')">
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
import TracksAsyncAutoComplete from "@/components/tracks/TracksAsyncAutoComplete.vue";
import toast from "@/services/toast";
import { useSessionsStore } from "@/stores/sessions";
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";

const router = useRouter();
const sessionsStore = useSessionsStore();

// Reactive state
const loading = ref(false);
const form = ref({
  session_name: "",
  genome_type: "",
  genome: "",
  is_public: false,
});
const errors = ref({});
const trackSearch = ref("");
const selectedTrack = ref(null);
const selectedTracks = ref([]);
const trackColors = ref({});
const trackValidationAlert = ref({ show: false, message: "", type: "error" });

// Table columns for selected tracks
const selectedTracksColumns = [
  {
    key: "name",
    label: "Track Name",
    sortable: true,
    width: "25%",
  },
  {
    key: "file_type",
    label: "File Type",
    sortable: true,
    width: "15%",
  },
  {
    key: "genome",
    label: "Genome",
    sortable: true,
    width: "20%",
  },
  {
    key: "dataset",
    label: "Dataset",
    sortable: true,
    width: "25%",
  },
  {
    key: "color",
    label: "Color",
    sortable: false,
    width: "10%",
  },
  {
    key: "actions",
    label: "Actions",
    sortable: false,
    width: "5%",
  },
];

// Computed properties for genome options are no longer needed since fields are auto-populated

const selectedTracksTableData = computed(() => {
  return selectedTracks.value.map((track) => ({
    ...track,
    // Ensure we have the required fields for the table
    name: track.name || "Unknown",
    file_type: track.file_type || "Unknown",
    genomeType: track.genomeType || "Unknown",
    genomeValue: track.genomeValue || "Unknown",
  }));
});

const canSubmit = computed(() => {
  return (
    form.value.session_name.trim() &&
    selectedTracks.value.length > 0 &&
    form.value.genome_type &&
    form.value.genome
  );
});

// Watchers are no longer needed since genome fields are auto-populated

// Methods

const addTrack = (track) => {
  // Check if track is already selected
  const existingIndex = selectedTracks.value.findIndex(
    (t) => t.id === track.id,
  );
  if (existingIndex === -1) {
    // Check if track has required genome information
    if (!track.genomeType || !track.genomeValue) {
      toast.error(
        `Track "${track.name}" is missing genome information and cannot be added to a session.`,
      );
      return;
    }

    // Validate track before adding
    const validationResult = validateTrackForSession(track);

    if (validationResult.isValid) {
      // Add track to selected tracks
      selectedTracks.value.push(track);
      // Set default color
      trackColors.value[track.id] = "#000000";
      // Clear the selected track for next selection
      selectedTrack.value = null;
      // Clear search term
      trackSearch.value = "";

      // Auto-populate genome fields if this is the first track
      if (selectedTracks.value.length === 1) {
        form.value.genome_type = track.genomeType;
        form.value.genome = track.genomeValue;
      }

      // Clear any previous validation alerts
      trackValidationAlert.value = { show: false, message: "", type: "error" };
    } else {
      // Show validation alert instead of toast
      trackValidationAlert.value = {
        show: true,
        message: validationResult.error,
        type: "error",
      };
    }
  }
};

// Track validation functions
const validateTrackForSession = (track) => {
  // Validation 1: Check file type consistency (MANDATORY)
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    if (track.file_type !== firstTrack.file_type) {
      return {
        isValid: false,
        error: `Cannot mix different file types. First track has "${firstTrack.file_type}", this track has "${track.file_type}". Please create separate sessions.`,
      };
    }
  }

  // Validation 2: Check genome type consistency (MANDATORY)
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    if (track.genomeType !== firstTrack.genomeType) {
      return {
        isValid: false,
        error: `Cannot mix different genome types. First track has "${firstTrack.genomeType}", this track has "${track.genomeType}". Please create separate sessions.`,
      };
    }
  }

  // Validation 3: Check genome value consistency
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    if (track.genomeValue !== firstTrack.genomeValue) {
      return {
        isValid: false,
        error: `Cannot mix different genome assemblies. First track has "${firstTrack.genomeValue}", this track has "${track.genomeValue}". Please create separate sessions or manually select one assembly.`,
      };
    }
  }

  return { isValid: true };
};

const validateAllTracks = () => {
  if (selectedTracks.value.length === 0) {
    return { isValid: false, error: "No tracks selected" };
  }

  // Check if all tracks have the same file type, genome type, and value
  const firstTrack = selectedTracks.value[0];
  const allConsistent = selectedTracks.value.every(
    (track) =>
      track.file_type === firstTrack.file_type &&
      track.genomeType === firstTrack.genomeType &&
      track.genomeValue === firstTrack.genomeValue,
  );

  if (!allConsistent) {
    return {
      isValid: false,
      error:
        "All tracks must have the same file type, genome type, and assembly. Cannot mix different file types, organisms, or genome builds in one session.",
    };
  }

  return { isValid: true };
};

const removeTrack = (trackId) => {
  const index = selectedTracks.value.findIndex((t) => t.id === trackId);
  if (index > -1) {
    selectedTracks.value.splice(index, 1);
    delete trackColors.value[trackId];

    // If this was the last track, clear genome fields
    if (selectedTracks.value.length === 0) {
      form.value.genome_type = "";
      form.value.genome = "";
    }
    // If this was the first track, update genome fields to match new first track
    else if (index === 0) {
      const newFirstTrack = selectedTracks.value[0];
      form.value.genome_type = newFirstTrack.genomeType;
      form.value.genome = newFirstTrack.genomeValue;
    }

    // Clear validation alerts when tracks are removed
    trackValidationAlert.value = { show: false, message: "", type: "error" };
  }
};

const validateForm = () => {
  errors.value = {};

  if (!form.value.session_name.trim()) {
    errors.value.session_name = "Session name is required";
  }

  // Validate tracks (this will ensure genome fields are populated)
  const trackValidation = validateAllTracks();
  if (!trackValidation.isValid) {
    errors.value.tracks = trackValidation.error;
  }

  // Additional validation: ensure genome fields are populated from tracks
  if (selectedTracks.value.length > 0) {
    const firstTrack = selectedTracks.value[0];
    if (
      !form.value.genome_type ||
      form.value.genome_type !== firstTrack.genomeType
    ) {
      errors.value.genome_type = `Genome type must match track genome type: ${firstTrack.genomeType}`;
    }
    if (!form.value.genome || form.value.genome !== firstTrack.genomeValue) {
      errors.value.genome = `Genome assembly must match track genome assembly: ${firstTrack.genomeValue}`;
    }
  }

  // Clear validation alerts if form is valid
  if (Object.keys(errors.value).length === 0) {
    trackValidationAlert.value = { show: false, message: "", type: "error" };
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
      track_colors: Object.entries(trackColors.value).map(([id, color]) => ({
        track_id: id,
        color: color,
      })),
    };

    const session = await sessionsStore.createSession(sessionData);
    toast.success("Session created successfully!");
    router.push(`/sessions/${session.id}`);
  } catch (error) {
    console.error("Failed to create session:", error);

    // Provide more specific error messages
    if (error.response?.data?.error) {
      toast.error(`Failed to create session: ${error.response.data.error}`);
    } else if (error.message) {
      toast.error(`Failed to create session: ${error.message}`);
    } else {
      toast.error("Failed to create session. Please try again.");
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
  requiresRoles: ["operator", "admin"]
  nav: [{ label: "Sessions", to: "/sessions" }, { label: "Create Session" }]
</route>
