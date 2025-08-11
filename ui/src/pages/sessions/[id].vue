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
        <router-link to="/sessions" class="hover:underline"
          >Sessions</router-link
        >
        <span>/</span>
        <span>{{ session.title }}</span>
      </div>

      <!-- Header -->
      <div class="flex justify-between items-start">
        <div>
          <h1 class="text-3xl font-bold">{{ session.title }}</h1>
          <p class="text-gray-600 mt-2">
            Created by {{ session.user?.name || session.user?.username }} on
            {{ date(session.created_at) }}
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

          <va-button preset="secondary" @click="exportDataHub">
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
              <span class="text-sm font-medium text-gray-600">Genome Type</span>
              <div class="mt-1">
                <va-chip size="small" preset="secondary">
                  {{ session.genome_type }}
                </va-chip>
              </div>
            </div>

            <div>
              <span class="text-sm font-medium text-gray-600">Genome</span>
              <div class="mt-1">
                <va-chip size="small" preset="primary">
                  {{ session.genome }}
                </va-chip>
              </div>
            </div>

            <div>
              <span class="text-sm font-medium text-gray-600">Visibility</span>
              <div class="mt-1">
                <va-chip
                  size="small"
                  :preset="session.is_public ? 'success' : 'warning'"
                >
                  {{ session.is_public ? "Public" : "Private" }}
                </va-chip>
              </div>
            </div>
          </div>
        </va-card-content>
      </va-card>

      <!-- Tracks List -->
      <div class="bg-white rounded-lg shadow p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-xl font-semibold">
            Tracks ({{ session.session_tracks?.length || 0 }})
          </h2>
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
                {{ sessionTrack.track.genomeType }}
                {{ sessionTrack.track.genomeValue }}
              </div>
              <div class="text-xs text-gray-500">
                Dataset: {{ sessionTrack.track.dataset_file?.dataset?.name }}
              </div>
              <div class="flex items-center gap-2 mt-1">
                <span
                  class="text-xs px-2 py-1 rounded-full"
                  :class="
                    sessionTrack.track.dataset_file?.dataset?.is_staged
                      ? 'bg-green-100 text-green-800'
                      : 'bg-yellow-100 text-yellow-800'
                  "
                >
                  {{
                    sessionTrack.track.dataset_file?.dataset?.is_staged
                      ? "Staged"
                      : "Not Staged"
                  }}
                </span>
                <span
                  v-if="sessionTrack.color"
                  class="text-xs px-2 py-1 rounded-full bg-gray-100"
                >
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

      <!-- Project Associations -->
      <va-card>
        <va-card-title>Project Associations</va-card-title>
        <va-card-content>
          <div v-if="sessionProjects.length" class="space-y-3">
            <div class="text-sm text-gray-600 mb-3">
              This session contains tracks from
              {{ sessionProjects.length }} project(s)
            </div>
            <va-data-table
              :items="sessionProjects"
              :columns="projectColumns"
              :loading="projectsLoading"
              disable-client-side-sorting
            >
              <template #cell(name)="{ rowData }">
                <router-link
                  :to="`/projects/${rowData.slug}`"
                  class="va-link font-medium"
                >
                  {{ rowData.name }}
                </router-link>
              </template>
              <template #cell(description)="{ rowData }">
                <span class="text-sm text-gray-600">
                  {{ rowData.description || "No description" }}
                </span>
              </template>
              <template #cell(created_at)="{ rowData }">
                <span class="text-sm text-gray-500">
                  {{ date(rowData.created_at) }}
                </span>
              </template>
            </va-data-table>
          </div>
          <div
            v-else-if="!projectsLoading"
            class="text-center text-gray-500 py-8"
          >
            This session has no associated projects.
          </div>
        </va-card-content>
      </va-card>

      <!-- Genome Browser Integration -->
      <va-card>
        <va-card-title class="flex items-center justify-between">
          <span>Genome Browser View</span>
          <div class="flex gap-2">
            <va-button size="small" preset="secondary" @click="refreshBrowser">
              <va-icon name="refresh" />
              Refresh
            </va-button>
            <va-button
              size="small"
              preset="secondary"
              @click="openInExternalBrowser"
            >
              <va-icon name="open_in_new" />
              Open External
            </va-button>
          </div>
        </va-card-title>
        <va-card-content>
          <div
            v-if="!session.genome_type || !session.genome"
            class="text-center py-8 text-gray-500"
          >
            <va-icon name="mdi-dna" class="text-4xl mb-2" />
            <p>Please set genome type and version to view tracks</p>
          </div>
          <div
            v-else-if="!session.session_tracks?.length"
            class="text-center py-8 text-gray-500"
          >
            <va-icon name="mdi-chart-gantt" class="text-4xl mb-2" />
            <p>No tracks added to this session yet</p>
          </div>
          <div v-else>
            <IGVBrowser
              :tracks="formattedTracks"
              :genome="`${session.genome_type}_${session.genome}`"
              @browser-ready="onBrowserReady"
              @error="onBrowserError"
            />

            <!-- Track Status -->
            <div
              v-if="unstagedTracks.length > 0"
              class="p-3 bg-yellow-50 border border-yellow-200 rounded-lg mt-4"
            >
              <div class="flex items-center gap-2 mb-2">
                <va-icon name="warning" color="warning" />
                <span class="font-medium text-yellow-800"
                  >Some tracks are not staged</span
                >
              </div>
              <div class="text-sm text-yellow-700">
                <p class="mb-2">
                  The following tracks need to be staged before they can be
                  visualized:
                </p>
                <ul class="list-disc list-inside space-y-1">
                  <li v-for="track in unstagedTracks" :key="track.id">
                    {{ track.track.name }} ({{
                      track.track.dataset_file?.dataset?.name
                    }})
                  </li>
                </ul>
                <p class="mt-2">
                  <va-button
                    size="small"
                    preset="warning"
                    @click="requestStaging"
                    :loading="requestingStaging"
                  >
                    Request Staging
                  </va-button>
                </p>
              </div>
            </div>
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
import IGVBrowser from "@/components/IGVBrowser.vue";
import EditSessionModal from "@/components/sessions/EditSessionModal.vue";
import api from "@/services/api";
import { date } from "@/services/datetime";
import toast from "@/services/toast";
import { useAuthStore } from "@/stores/auth";
import { useSessionsStore } from "@/stores/sessions";
import { computed, onMounted, ref } from "vue";
import { useRoute, useRouter } from "vue-router";

const route = useRoute();
const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();

// Reactive state
const editModal = ref();
const showEditModal = ref(false);
const requestingStaging = ref(false);
const projectsLoading = ref(false);
const sessionProjects = ref([]);

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
  return session.value.session_tracks.some(
    (st) => !st.track.dataset_file?.dataset?.is_staged,
  );
});

const unstagedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];
  return session.value.session_tracks.filter(
    (st) => !st.track.dataset_file?.dataset?.is_staged,
  );
});

const _stagedTracksCount = computed(() => {
  if (!session.value?.session_tracks) return 0;
  return session.value.session_tracks.filter(
    (st) => st.track.dataset_file?.dataset?.is_staged,
  ).length;
});

// Project table columns
const projectColumns = [
  {
    key: "name",
    label: "Project Name",
    sortable: true,
    width: "40%",
  },
  {
    key: "description",
    label: "Description",
    sortable: false,
    width: "40%",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "20%",
  },
];

// Format tracks for IGV browser
const formattedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];

  return session.value.session_tracks.map((sessionTrack) => {
    const track = sessionTrack.track;
    return {
      name: track.name,
      url: `/api/files/${track.dataset_file_id}`,
      type: track.file_type,
      color: sessionTrack.color || "#000000",
      height: 50,
    };
  });
});

// Methods
const deleteSession = async () => {
  if (!session.value) return;

  if (confirm("Are you sure you want to delete this session?")) {
    try {
      await sessionsStore.deleteSession(session.value.id);
      toast.success("Session deleted successfully");
      router.push("/sessions");
    } catch (error) {
      console.error("Failed to delete session:", error);
      toast.error("Failed to delete session");
    }
  }
};

const exportDataHub = async () => {
  try {
    const response = await api.get(`/sessions/${session.value.id}/datahub`);

    // Create a blob with the DataHub JSON data
    const blob = new Blob([JSON.stringify(response.data, null, 2)], {
      type: "application/json",
    });

    // Create download link
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `session-${session.value.id}-datahub.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);

    toast.success("DataHub export downloaded successfully");
  } catch (error) {
    console.error("Failed to export DataHub:", error);
    toast.error("Failed to export DataHub");
  }
};

const requestStaging = async () => {
  if (!session.value) return;

  requestingStaging.value = true;
  try {
    const response = await api.post(`/sessions/${session.value.id}/stage`);

    if (response.data.datasets && response.data.datasets.length > 0) {
      // Show which datasets need staging
      toast.info(
        `${response.data.datasets.length} datasets need staging. Use the dataset staging workflow to stage them individually.`,
      );

      // You could also navigate to a datasets page or show a modal with staging options
      console.log("Datasets that need staging:", response.data.datasets);
    } else {
      toast.success("All datasets are already staged");
    }
  } catch (error) {
    console.error("Failed to check staging status:", error);
    toast.error("Failed to check staging status");
  } finally {
    requestingStaging.value = false;
  }
};

const loadSession = async () => {
  const sessionId = parseInt(route.params.id);
  if (isNaN(sessionId)) {
    router.push("/sessions");
    return;
  }

  try {
    await sessionsStore.fetchSession(sessionId);
    // Load session projects after session is loaded
    await loadSessionProjects();
  } catch (error) {
    // Error is handled by the store
  }
};

const loadSessionProjects = async () => {
  if (!session.value?.id) return;

  projectsLoading.value = true;
  try {
    const response = await api.get(`/sessions/${session.value.id}/projects`);
    sessionProjects.value = response.data.projects;
  } catch (error) {
    console.error("Failed to load session projects:", error);
    sessionProjects.value = [];
  } finally {
    projectsLoading.value = false;
  }
};

const refreshBrowser = () => {
  // Refresh the browser view
  // This could reload tracks or refresh the visualization
  console.log("Refreshing browser view");
};

const handleSessionUpdated = (updatedSession) => {
  toast.success("Session updated successfully");
  // Refresh the session data
  loadSession();
};

const onBrowserReady = () => {
  console.log("Browser is ready");
};

const onBrowserError = (error) => {
  console.error("Browser error:", error);
  toast.error("Failed to load genome browser");
};

const openInExternalBrowser = () => {
  if (!session.value) return;

  // Generate DataHub URL for external browser
  const datahubUrl = `${window.location.origin}/api/sessions/${session.value.id}/datahub`;

  // Open in new tab
  window.open(datahubUrl, "_blank");

  toast.info(
    "DataHub export opened in new tab. Copy the URL to use with external genome browsers.",
  );
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
  nav: [{ label: "Sessions", to: "/sessions" }, { label: "Session Details" }]
</route>
