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
              <div class="flex flex-nowrap items-center w-full">
                <span class="flex-auto text-lg"> Session Details </span>
                <AddEditButton
                  class="flex-none"
                  edit
                  @click.stop="showEditModal = true"
                  v-if="canEditSession"
                />
              </div>
            </va-card-title>
            <va-card-content class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Session Title:</span>
                <span>{{ session.title }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Type</span>
                <va-chip v-if="session.genome_type" size="small">{{ session.genome_type }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Value</span>
                <va-chip v-if="session.genome" outline size="small">{{ session.genome }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Visibility</span>
                <div class="flex items-center gap-2">
                  <va-icon
                    :name="session.is_public ? 'public' : 'lock'"
                    :color="session.is_public ? 'success' : 'warning'"
                  />
                  <span>{{ session.is_public ? 'Public' : 'Private' }}</span>
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
                <!-- View in IGV Browser Action Button-->
                <va-button
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                  class="flex-initial"
                  @click="viewInIGV"
                  :loading="igvLoading"
                >
                  <i-mdi-dna class="pr-2 text-2xl" />
                  View in IGV Browser
                </va-button>

                <!-- Delete Session Action Button-->
                <va-button
                  v-if="canDeleteSession"
                  color="danger"
                  border-color="danger"
                  class="flex-initial"
                  preset="secondary"
                  @click="deleteSession"
                >
                  <i-mdi-delete class="pr-2 text-2xl" />
                  Delete Session
                </va-button>

                <!-- Share Session Action Button-->
                <va-button
                  class="flex-initial"
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                >
                  <i-mdi-share-variant class="pr-2 text-2xl" />
                  Share Session
                </va-button>
              </div>
            </va-card-content>
          </va-card>
        </div>

        <!-- Associated Tracks Information -->
        <div class="grid grid-cols-1 gap-3">
          <va-card>
            <va-card-title>
              <div class="flex flex-nowrap items-center w-full">
                <span class="flex-auto text-lg">Associated Tracks</span>
                <AddEditButton
                  class="flex-none"
                  show-text
                  :edit="associatedTracks?.length > 0"
                  @click="showTracksModal = true"
                  v-if="canEditSession"
                />
              </div>
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
                    <router-link :to="`/tracks/${rowData.id}`" class="va-link font-medium">
                      {{ rowData.name }}
                    </router-link>
                  </template>

                  <template #cell(analysis_type)="{ rowData }">
                    <va-chip
                      v-if="rowData?.analysis_type"
                      size="small"
                      :color="trackService._getTrackColor(rowData.analysis_type)"
                      >{{ rowData?.analysis_type }}</va-chip
                    >
                  </template>

                  <template #cell(genomeType)="{ rowData }">
                    <va-chip v-if="rowData.genomeType" size="small">
                      {{ rowData.genomeType }}
                    </va-chip>
                  </template>

                  <template #cell(genomeValue)="{ rowData }">
                    <va-chip v-if="rowData.genomeValue" size="small" outline>
                      {{ rowData.genomeValue }}
                    </va-chip>
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

    <!-- Edit Session Modal -->
    <va-modal
      v-model="showEditModal"
      title="Edit Session Details"
      size="small"
      no-outside-dismiss
      fixed-layout
      ok-text="Update"
      @ok="updateSession"
      @cancel="showEditModal = false"
    >
      <va-inner-loading :loading="updating">
        <div class="space-y-4">
          <va-input
            v-model="editForm.title"
            label="Session Title"
            class="w-full"
            :rules="[(value) => !!value || 'Session title is required']"
          />

          <va-input v-model="editForm.genome_type" label="Genome Type" class="w-full" clearable />

          <va-input v-model="editForm.genome" label="Genome Value" class="w-full" clearable />

          <va-checkbox v-model="editForm.is_public" label="Make session public" />
        </div>
      </va-inner-loading>
    </va-modal>

    <!-- Edit Tracks Modal -->
    <va-modal
      v-model="showTracksModal"
      title="Manage Session Tracks"
      size="large"
      hide-default-actions
      no-outside-dismiss
    >
      <div class="space-y-4">
        <p class="text-sm text-gray-600">Select tracks from Data Products for this session.</p>

        <TracksAsyncAutoComplete
          v-model:search-term="trackSearch"
          label="Search and Add Tracks"
          placeholder="Search tracks by name"
          multiple
          @select="handleTrackSelect"
        />

        <!-- Selected tracks table -->
        <div v-if="selectedTracks.length > 0" class="space-y-3">
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
              :loading="false"
              disable-client-side-sorting
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
        </div>

        <div class="flex justify-end gap-3 pt-4">
          <va-button preset="secondary" @click="showTracksModal = false"> Cancel </va-button>
          <va-button preset="primary" :loading="updatingTracks" @click="updateSessionTracks">
            Update Tracks
          </va-button>
        </div>
      </div>
    </va-modal>

    <!-- Delete Session Modal -->
    <DeleteSessionModal
      ref="sessionDeleteModal"
      :data="session"
      @update="router.push('/sessions')"
    />

    <!-- IGV Browser Modal -->
    <va-modal
      v-model="showIGV"
      title="IGV Genome Browser"
      fullscreen
      hide-default-actions
      no-padding
      @close="closeIGV"
    >
      <div class="h-full flex flex-col">
        <div id="igv-container" class="flex-1" style="min-height: 600px"></div>
      </div>
    </va-modal>
  </div>
</template>

<script setup>
import DeleteSessionModal from '@/components/sessions/DeleteSessionModal.vue';
import TracksAsyncAutoComplete from '@/components/tracks/TracksAsyncAutoComplete.vue';
import AddEditButton from '@/components/utils/buttons/AddEditButton.vue';
import * as datetime from '@/services/datetime';
import sessionService from '@/services/session';
import toast from '@/services/toast';
import trackService from '@/services/track';
import { formatBytes } from '@/services/utils';
import { useAuthStore } from '@/stores/auth';
import { useNavStore } from '@/stores/nav';
import { useSessionsStore } from '@/stores/sessions';
import { nextTick } from 'vue';

const route = useRoute();
const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();
const nav = useNavStore();

// Reactive state
const _editModal = ref();
const showEditModal = ref(false);
const requestingStaging = ref(false);
const projectsLoading = ref(false);
const sessionProjects = ref([]);
const updating = ref(false);
const editForm = ref({
  title: '',
  genome_type: '',
  genome: '',
  is_public: false,
});
const showTracksModal = ref(false);
const updatingTracks = ref(false);
const selectedTracks = ref([]);
const trackSearch = ref('');

// Computed
const session = computed(() => sessionsStore.currentSession);
const loading = computed(() => sessionsStore.loading);
const _error = computed(() => sessionsStore.error);

const canEditSession = computed(() => {
  return session.value?.user_id === auth.user?.id;
});

const canDeleteSession = computed(() => {
  return session.value?.user_id === auth.user?.id;
});

const _hasUnstagedTracks = computed(() => {
  if (!session.value?.session_tracks) return false;
  return session.value.session_tracks.some((st) => !st.track.dataset_file?.dataset?.is_staged);
});

// Table columns for selected tracks in modal
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

const _unstagedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];
  return session.value.session_tracks.filter((st) => !st.track.dataset_file?.dataset?.is_staged);
});

const _stagedTracksCount = computed(() => {
  if (!session.value?.session_tracks) return 0;
  return session.value.session_tracks.filter((st) => st.track.dataset_file?.dataset?.is_staged)
    .length;
});

// IGV Browser state
const showIGV = ref(false);
const igvLoading = ref(false);
let igvBrowser = null;

const associatedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];
  return session.value.session_tracks.map((st) => st.track);
});

const trackColumns = [
  {
    key: 'name',
    label: 'Name',
    sortable: true,
    width: '25%',
    thAlign: 'left',
    tdAlign: 'left',
  },
  {
    key: 'analysis_type',
    label: 'Analysis Type',
    sortable: true,
    width: '15%',
  },
  {
    key: 'genomeType',
    label: 'Genome Type',
    sortable: true,
    width: '12%',
  },
  {
    key: 'genomeValue',
    label: 'Genome Value',
    sortable: true,
    width: '13%',
  },
  {
    key: 'dataset_name',
    label: 'Dataset Name',
    sortable: true,
    width: '20%',
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
    key: 'created_at',
    label: 'Created',
    sortable: true,
    width: '15%',
    thAlign: 'right',
    tdAlign: 'right',
  },
];

// Project table columns
const _projectColumns = [
  {
    key: 'name',
    label: 'Project Name',
    sortable: true,
    width: '40%',
  },
  {
    key: 'description',
    label: 'Description',
    sortable: false,
    width: '40%',
  },
  {
    key: 'created_at',
    label: 'Created',
    sortable: true,
    width: '20%',
  },
];

// Methods
const sessionDeleteModal = ref(null);

const deleteSession = async () => {
  if (!session.value) return;
  sessionDeleteModal.value.show();
};

const _exportDataHub = async () => {
  try {
    const response = await sessionService.getDatahub(session.value.id);

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

const _requestStaging = async () => {
  if (!session.value) return;

  requestingStaging.value = true;
  try {
    const response = await sessionService.stage(session.value.id);

    if (response.data.datasets && response.data.datasets.length > 0) {
      // Show which datasets need staging
      toast.info(
        `${response.data.datasets.length} datasets need staging. Use the dataset staging workflow to stage them individually.`
      );

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
    // Load session projects after session is loaded
    await loadSessionProjects();

    if (session.value) {
      nav.setNavItems([
        {
          label: 'Sessions',
          to: '/sessions',
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
    const response = await sessionService.getProjects(session.value.id);
    sessionProjects.value = response.data.projects;
  } catch (error) {
    console.error('Failed to load session projects:', error);
    sessionProjects.value = [];
  } finally {
    projectsLoading.value = false;
  }
};

const _handleSessionUpdated = (_updatedSession) => {
  toast.success('Session updated successfully');
  // Refresh the session data
  loadSession();
};

/**
 * Initialize IGV browser for the session
 */
const viewInIGV = async () => {
  if (!session.value) return;

  igvLoading.value = true;

  try {
    // Set the authentication cookie for file access
    await sessionService.setFileCookie(session.value.id);

    // Fetch the datahub configuration (includes genome and tracks)
    const datahubResponse = await sessionService.getDatahub(session.value.id);
    const datahubConfig = datahubResponse.data;

    console.log('[IGV] Datahub response:', datahubConfig);

    // Use tracks from the datahub response (served via /files/expose with cookie auth)
    const igv_tracks = datahubConfig.tracks || [];
    const igv_genome = datahubConfig.genome || 'hg38';

    console.log('[IGV] Tracks:', igv_tracks);
    console.log('[IGV] Genome:', igv_genome);

    if (!igv_tracks || igv_tracks.length === 0) {
      toast.error('No tracks available for this session');
      igvLoading.value = false;
      return;
    }

    // Show IGV container
    showIGV.value = true;

    // Wait for DOM to update
    await nextTick();

    // Dynamically import IGV
    const igvModule = await import('igv');
    const igv = igvModule.default;

    // Configure IGV options
    const igvOptions = {
      genome: igv_genome,
      locus: 'chr8:127,736,588-127,739,371', // Default locus
      tracks: igv_tracks,
    };

    // Create IGV browser instance
    const container = document.getElementById('igv-container');
    if (container && igv_tracks && igv_tracks.length > 0) {
      console.log('igv_tracks');
      console.log(igv_tracks);
      igvBrowser = await igv.createBrowser(container, igvOptions);
      // toast.success('IGV browser loaded successfully');
      console.log('IGV browser loaded successfully');
    } else {
      throw new Error('IGV container not found');
    }
  } catch (error) {
    console.error('Failed to initialize IGV:', error);
    toast.error('Failed to load IGV browser');
    showIGV.value = false;
  } finally {
    igvLoading.value = false;
  }
};

/**
 * Close IGV browser
 */
const closeIGV = () => {
  if (igvBrowser) {
    igvBrowser.remove();
    igvBrowser = null;
  }
  showIGV.value = false;
};

const updateSession = async () => {
  updating.value = true;
  try {
    await sessionsStore.updateSession(session.value.id, editForm.value);
    toast.success('Session updated successfully');
    showEditModal.value = false;
  } catch (error) {
    console.error('Failed to update session:', error);
    toast.error('Failed to update session');
  } finally {
    updating.value = false;
  }
};

const handleTrackSelect = (track) => {
  // Add track to selected tracks if not already present
  const existingIndex = selectedTracks.value.findIndex((t) => t.id === track.id);
  if (existingIndex === -1) {
    selectedTracks.value.push(track);
  }
};

const removeTrack = (trackId) => {
  const index = selectedTracks.value.findIndex((t) => t.id === trackId);
  if (index > -1) {
    selectedTracks.value.splice(index, 1);
  }
};

const updateSessionTracks = async () => {
  updatingTracks.value = true;
  try {
    const trackIds = selectedTracks.value.map((track) => track.id);
    await sessionsStore.updateSession(session.value.id, { track_ids: trackIds });
    toast.success('Session tracks updated successfully');
    showTracksModal.value = false;
    await loadSession(); // Reload session data
  } catch (error) {
    console.error('Failed to update session tracks:', error);
    toast.error('Failed to update session tracks');
  } finally {
    updatingTracks.value = false;
  }
};

// Watch for modal opening to initialize form
watch(showEditModal, (isOpen) => {
  if (isOpen && session.value) {
    editForm.value = {
      title: session.value.title || '',
      genome_type: session.value.genome_type || '',
      genome: session.value.genome || '',
      is_public: session.value.is_public || false,
    };
  }
});

// Watch for tracks modal opening to initialize selected tracks
watch(showTracksModal, (isOpen) => {
  if (isOpen && session.value?.session_tracks) {
    selectedTracks.value = session.value.session_tracks.map((st) => st.track);
  }
});

// Lifecycle
onMounted(async () => {
  await loadSession();
});

onUnmounted(() => {
  // Clean up IGV browser instance
  closeIGV();
});
</script>

<route lang="yaml">
meta:
  title: Session Details
  requiresRoles: ['operator', 'admin']
</route>
