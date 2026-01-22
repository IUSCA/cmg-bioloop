<template>
  <div class="session-detail">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circular indeterminate />
    </div>

    <!-- <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div> -->

    <div v-else-if="session">
      <!-- Session Info + Tracks list Cards -->
      <div class="flex flex-col gap-6">
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <!-- Session Info -->
          <va-card>
            <va-card-title>
              <span class="text-lg"> Session Details </span>
            </va-card-title>
            <va-card-content class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Session Title:</span>
                <span>{{ session.title }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Type</span>
                <va-chip size="small">{{ session.genome_type }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Value</span>
                <va-chip outline size="small">{{ session.genome }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Visibility</span>
                <div class="flex items-center gap-2">
                  <va-icon
                    :name="session.is_public ? 'public' : 'lock'"
                    :color="session.is_public ? 'success' : 'warning'"
                  />
                  <span>{{ session.is_public ? "Public" : "Private" }}</span>
                </div>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created</span>
                <span>{{ datetime.date(session.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated</span>
                <span>{{ datetime.fromNow(session.updated_at) }}</span>
              </div>
            </va-card-content>
          </va-card>

          <!-- Actions -->
          <va-card>
            <va-card-title>
              <span class="text-lg"> Actions </span>
            </va-card-title>
            <va-card-content class="flex items-center justify-center py-8">
              <div class="flex gap-3">
                <!-- Open in Genome Browser Action Button-->
                <va-button
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                  class="flex-initial"
                  @click="initiateGenomeBrowserSession"
                >
                  <i-mdi-open-in-new class="pr-2 text-2xl" />
                  Open in Genome Browser
                </va-button>

                <!-- Delete Session Action Button-->
                <!-- <va-button
                  color="danger"
                  border-color="danger"
                  class="flex-initial"
                  preset="secondary"
                >
                  <i-mdi-delete class="pr-2 text-2xl" />
                  Delete Session
                </va-button> -->

                <!-- Share Session Action Button-->
                <!-- <va-button
                  class="flex-initial"
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                >
                  <i-mdi-share-variant class="pr-2 text-2xl" />
                  Share Session
                </va-button> -->
              </div>
            </va-card-content>
          </va-card>
        </div>

        <!-- Associated Tracks Information -->
        <div class="grid grid-cols-1 gap-3">
          <va-card>
            <va-card-title>
              <span class="text-lg">Associated Tracks</span>
            </va-card-title>
            <va-card-content>
              <div v-if="associatedTracks?.length" class="space-y-4">
                <va-data-table
                  :items="associatedTracks"
                  :columns="trackColumns"
                  :loading="false"
                  disable-client-side-sorting
                >
                  <template #cell(name)="{ rowData }">
                    <router-link
                      :to="`/tracks/${rowData.id}`"
                      class="va-link font-medium"
                    >
                      {{ rowData.name }}
                    </router-link>
                  </template>

                  <template #cell(file_type)="{ rowData }">
                    <va-chip
                      size="small"
                      :color="trackService._getTrackColor(rowData.file_type)"
                      >{{ rowData.file_type }}</va-chip
                    >
                  </template>

                  <template #cell(genomeType)="{ rowData }">
                    <va-chip size="small">{{ rowData.genomeType }}</va-chip>
                  </template>

                  <template #cell(genomeValue)="{ rowData }">
                    <va-chip size="small" outline>{{
                      rowData.genomeValue
                    }}</va-chip>
                  </template>

                  <template #cell(dataset_name)="{ rowData }">
                    <router-link
                      :to="`/datasets/${rowData.dataset_file.dataset.id}`"
                      class="va-link font-medium"
                    >
                      {{ rowData.dataset_file.dataset.name }}
                    </router-link>
                  </template>

                  <template #cell(created_at)="{ rowData }">
                    <span>
                      {{ datetime.fromNow(rowData.created_at) }}
                    </span>
                  </template>
                </va-data-table>
              </div>
            </va-card-content>
          </va-card>
        </div>
      </div>

      <!-- Action Buttons -->
      <!-- <div class="flex justify-between items-start">
        <div class="flex gap-3">
          <va-button
            v-if="canEditSession"
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
            v-if="canDeleteSession"
            preset="danger"
            @click="deleteSession"
          >
            <va-icon name="delete" />
            Delete Session
          </va-button>
        </div>
      </div> -->

      <!-- Project Associations -->
      <!-- <va-card>
        <va-card-title>
          <span class="text-lg">Project Associations</span>
        </va-card-title>
        <va-card-content>
          <div v-if="sessionProjects.length" class="space-y-3">
            <div class="mb-3">
              This session contains tracks from
              {{ sessionProjects.length }} project(s)
            </div>
            <va-data-table
              :items="Projects"
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
                <span>
                  {{ rowData.description || "No description" }}
                </span>
              </template>
              <template #cell(created_at)="{ rowData }">
                <span>
                  {{ date(rowData.created_at) }}
                </span>
              </template>
            </va-data-table>
          </div>
          <div v-else-if="!projectsLoading" class="text-center py-8">
            This session has no associated projects.
          </div>
        </va-card-content>
      </va-card> -->

      <!-- Genome Browser Integration -->
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
import api from "@/services/api";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import trackService from "@/services/track";
import { useAuthStore } from "@/stores/auth";
import { useNavStore } from "@/stores/nav";
import { useSessionsStore } from "@/stores/sessions";

const route = useRoute();
const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();
const nav = useNavStore();

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


const associatedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];
  return session.value.session_tracks.map((st) => st.track);
});

const trackColumns = [
  {
    key: "name",
    label: "Name",
    sortable: true,
    width: "35%",
    thAlign: "left",
    tdAlign: "left",
  },
  {
    key: "file_type",
    label: "File Type",
    sortable: true,
    width: "25%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "genomeType",
    label: "Genome Type",
    sortable: true,
    width: "25%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "genomeValue",
    label: "Genome Value",
    sortable: true,
    width: "25%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "dataset_name",
    label: "Dataset Name",
    sortable: true,
    width: "25%",
    thAlign: "center",
    tdAlign: "center",
  },
  // {
  //   key: "is_staged",
  //   label: "Staged",
  //   sortable: true,
  //   width: "25%",
  // },
  // {
  //   key: "created_by",
  //   label: "Created By",
  //   sortable: true,
  //   width: "25%",
  // },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "25%",
    thAlign: "right",
    tdAlign: "right",
  },
];

// Project table columns
const projectColumns = [
  {
    key: "name",
    label: "Project Name",
    sortable: true,
    width: "40%",
    thAlign: "left",
    tdAlign: "left",
  },
  {
    key: "description",
    label: "Description",
    sortable: false,
    width: "40%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "20%",
    thAlign: "right",
    tdAlign: "right",
  },
];

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

    if (session.value) {
      nav.setNavItems([
        {
          label: "Sessions",
          to: "/sessions",
        },
        {
          label: session.value.title,
        },
      ]);
    }
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

const handleSessionUpdated = (updatedSession) => {
  toast.success("Session updated successfully");
  // Refresh the session data
  loadSession();
};

const initiateGenomeBrowserSession = async () => {
  if (!session.value) return;



};

onMounted(() => {
  loadSession();
});
</script>

<route lang="yaml">
meta:
  title: Session Details
  requiresRoles: ["operator", "admin"]
</route>
