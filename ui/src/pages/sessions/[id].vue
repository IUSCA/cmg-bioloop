<template>
  <div class="session-detail">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circle indeterminate />
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
                <span class="font-medium">Genome</span>
                <va-chip
                  v-if="session.genome_type || session.genome"
                  size="small"
                  outline
                >
                  {{ session.genome_type || ""
                  }}{{ session.genome ? ` (${session.genome})` : "" }}
                </va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created</span>
                <span>{{ datetime.date(session.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated</span>
                <span>{{ datetime.fromNow(session.updated_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Data Status</span>
                <div class="flex items-center gap-2">
                  <va-icon
                    :name="
                      session.data_requested?.all_staged
                        ? 'check_circle'
                        : session.data_requested?.requested
                          ? 'hourglass_empty'
                          : 'warning'
                    "
                    :color="
                      session.data_requested?.all_staged
                        ? 'success'
                        : session.data_requested?.requested
                          ? 'warning'
                          : 'danger'
                    "
                  />
                  <span class="text-sm">
                    {{
                      session.data_requested?.all_staged
                        ? "All Staged"
                        : session.data_requested?.requested
                          ? `Staging ${session.data_requested.request_status || "PENDING"}`
                          : "Not Staged"
                    }}
                  </span>
                </div>
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
                <!-- View in Genome Browser Action Button-->
                <va-button
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                  class="flex-initial"
                  @click="handleViewInBrowser"
                  :loading="genomeBrowserLoading"
                >
                  <i-mdi-dna class="pr-2 text-2xl" />
                  View in Genome Browser
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

                <!-- Retry Staging Button - shown if data requested and staging pending -->
                <va-button
                  v-if="
                    session.data_requested?.requested &&
                    session.data_requested?.request_status === 'PENDING' &&
                    canRetryStaging
                  "
                  color="warning"
                  border-color="warning"
                  class="flex-initial"
                  preset="secondary"
                  :loading="retryingStagingLoading"
                  @click="retryStaging"
                >
                  <i-mdi-refresh class="pr-2 text-2xl" />
                  Retry Staging
                </va-button>

                <!-- Share Session Action Button (commented out)
                <va-button
                  class="flex-initial"
                  color="primary"
                  border-color="primary"
                  preset="secondary"
                >
                  <i-mdi-share-variant class="pr-2 text-2xl" />
                  Share Session
                </va-button>
                -->
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
              <!-- Show message if legacy session not hydrated and no tracks -->
              <div
                v-if="needsHydration && !associatedTracks?.length"
                class="text-center py-8 text-gray-600 dark:text-gray-400"
              >
                Track Information is not yet available for this legacy Session.
              </div>

              <!-- Show tracks table if tracks exist -->
              <div v-else-if="associatedTracks?.length" class="space-y-4">
                <va-data-table
                  :items="paginatedTracks"
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

                  <template #cell(analysis_type)="{ rowData }">
                    <va-chip
                      v-if="rowData?.analysis_type"
                      size="small"
                      outline
                      :color="
                        trackService._getTrackColor(rowData.analysis_type)
                      "
                      >{{ rowData?.analysis_type }}</va-chip
                    >
                  </template>

                  <template #cell(genome)="{ rowData }">
                    <GenomeDisplay
                      :genome-type="
                        rowData.dataset_file?.dataset?.genomic_details
                          ?.genome_type
                      "
                      :genome-value="
                        rowData.dataset_file?.dataset?.genomic_details
                          ?.genome_value
                      "
                    />
                  </template>

                  <template #cell(dataset_name)="{ rowData }">
                    <router-link
                      v-if="auth.canOperate"
                      :to="`/datasets/${rowData.dataset_file?.dataset?.id}`"
                      class="va-link font-medium"
                    >
                      {{ rowData.dataset_file?.dataset?.name }}
                    </router-link>
                    <span v-else>{{
                      rowData.dataset_file?.dataset?.name
                    }}</span>
                  </template>

                  <template #cell(stage)="{ rowData }">
                    <div v-if="rowData.dataset_file?.dataset?.is_staged">
                      <va-button
                        class="shadow"
                        preset="primary"
                        color="info"
                        icon="cloud_sync"
                        disabled
                      />
                    </div>
                    <div v-else class="flex justify-center">
                      <va-popover
                        v-if="
                          getTrackDatasetStagingStatus(rowData)
                            .is_archival_pending
                        "
                        :message="'Dataset is pending archival to SDA'"
                      >
                        <half-circle-spinner
                          class="flex-none"
                          :animation-duration="1000"
                          :size="24"
                          :color="colors.info"
                        />
                      </va-popover>
                      <va-popover
                        v-else-if="
                          getTrackDatasetStagingStatus(rowData)
                            .is_staging_pending
                        "
                        :message="'Dataset is being staged'"
                      >
                        <half-circle-spinner
                          class="flex-none"
                          :animation-duration="1000"
                          :size="24"
                          :color="colors.warning"
                        />
                      </va-popover>
                      <va-button
                        v-else
                        class="shadow flex-none"
                        preset="primary"
                        color="info"
                        icon="cloud_sync"
                        @click="openTrackDatasetStageModal(rowData)"
                      />
                    </div>
                  </template>

                  <template #cell(download_dataset)="{ rowData }">
                    <va-button
                      class="shadow"
                      preset="primary"
                      color="info"
                      icon="cloud_download"
                      @click="openTrackDatasetDownloadModal(rowData)"
                      :disabled="!rowData.dataset_file?.dataset?.is_staged"
                    />
                  </template>

                  <template #cell(download_file)="{ rowData }">
                    <va-button
                      class="shadow"
                      preset="primary"
                      color="info"
                      icon="download"
                      @click="downloadTrackFile(rowData)"
                      :disabled="!rowData.dataset_file?.dataset?.is_staged"
                    />
                  </template>

                  <template #cell(created_at)="{ rowData }">
                    <span>
                      {{ datetime.fromNow(rowData.created_at) }}
                    </span>
                  </template>
                </va-data-table>

                <!-- Pagination for tracks -->
                <Pagination
                  v-model:page="currentTrackPage"
                  v-model:page_size="trackPageSize"
                  :total_results="associatedTracks.length"
                  :curr_items="paginatedTracks.length"
                  :page_size_options="TRACK_PAGE_SIZE_OPTIONS"
                />
              </div>

              <!-- Show empty state for non-legacy or hydrated sessions with no tracks -->
              <div
                v-else
                class="text-center py-8 text-gray-600 dark:text-gray-400"
              >
                No tracks associated with this session.
              </div>
            </va-card-content>
          </va-card>
        </div>

        <!-- Associated Datasets Table -->
        <div class="grid grid-cols-1 gap-3">
          <SessionDatasetsTable
            :session-id="session?.id"
            :data-requested="session?.data_requested?.requested || false"
            @datasets-updated="handleDatasetsUpdated"
          />
        </div>

        <!-- Workflows (legacy sessions only) -->
        <div v-if="isLegacySession" class="grid grid-cols-1 gap-3">
          <div>
            <span class="flex text-xl my-2 font-bold">WORKFLOWS</span>
            <div v-if="sessionWorkflowsDisplay.length > 0" class="space-y-2">
              <collapsible
                v-for="workflow in sessionWorkflowsDisplay"
                :key="workflow.id"
                v-model="workflow.collapse_model"
              >
                <template #header-content>
                  <WorkflowCompact :workflow="workflow" />
                </template>
                <div>
                  <workflow :workflow="workflow" @update="loadSession" />
                </div>
              </collapsible>
            </div>
            <div
              v-else
              class="text-center bg-slate-200 dark:bg-slate-800 py-2 rounded shadow"
            >
              <i-mdi-card-remove-outline class="inline-block text-4xl pr-3" />
              <span class="text-lg">
                No workflows associated with this session.
              </span>
            </div>
          </div>
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

          <va-input
            v-model="editForm.genome_type"
            label="Genome Type"
            class="w-full"
            clearable
          />

          <va-input
            v-model="editForm.genome"
            label="Genome Value"
            class="w-full"
            clearable
          />
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
        <p class="text-sm text-gray-600">
          Select tracks from Data Products for this session.
        </p>

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
              {{ selectedTracks.length }} track{{
                selectedTracks.length !== 1 ? "s" : ""
              }}
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
                <GenomeDisplay
                  :genome-type="rowData.genome_type"
                  :genome-value="rowData.genome_value"
                />
              </template>

              <template #cell(dataset)="{ rowData }">
                <div class="text-sm">
                  <router-link
                    v-if="rowData.dataset?.id"
                    :to="`/datasets/${rowData.dataset.id}`"
                    target="_blank"
                    class="text-primary hover:underline"
                  >
                    {{ rowData.dataset.name || "" }}
                  </router-link>
                  <span v-else>{{ rowData.dataset?.name || "" }}</span>
                </div>
              </template>

              <template #cell(size)="{ rowData }">
                <div class="text-sm">
                  {{ formatBytes(rowData.size) }}
                </div>
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
          </va-scroll-container>
        </div>

        <div class="flex justify-end gap-3 pt-4">
          <va-button preset="secondary" @click="showTracksModal = false">
            Cancel
          </va-button>
          <va-button
            preset="primary"
            :loading="updatingTracks"
            @click="updateSessionTracks"
          >
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

    <!-- Track Dataset Download Modal -->
    <DatasetDownloadModal
      ref="trackDatasetDownloadModal"
      :dataset="trackDatasetToDownload"
    />

    <!-- Track Dataset Stage Modal -->
    <StageDatasetModal
      ref="trackDatasetStageModal"
      :dataset="trackDatasetToStage"
      @update="loadSession"
    />

    <!-- Unstaged Datasets Modal -->
    <UnstagedDatasetsModal
      v-model="showUnstagedModal"
      :session-id="session?.id"
      :datasets="datasetsToStage"
      @close="showUnstagedModal = false"
      @staging-requested="handleStagingRequested"
    />

    <!-- Hydration Modal -->
    <va-modal
      v-model="showHydrationModal"
      title="Session Hydration Required"
      size="small"
      ok-text="Hydrate Session"
      :ok-disabled="hydratingSession"
      @ok="handleHydrateSession"
      @cancel="showHydrationModal = false"
    >
      <va-inner-loading :loading="hydratingSession">
        <p>
          This session needs to be hydrated with track information before it can
          be viewed in the genome browser. Would you like to start the hydration
          workflow?
        </p>
        <p class="mt-2 text-sm text-gray-600">
          Note: Hydration may take a few moments. You will be able to view the
          session once the workflow completes.
        </p>
      </va-inner-loading>
    </va-modal>

    <!-- Browser Selection Modal -->
    <BrowserSelectionModal
      v-model="showBrowserSelectionModal"
      @browser-selected="handleBrowserSelection"
    />

    <!-- Genome Browser Modal (IGV or WashU) -->
    <va-modal
      v-model="showGenomeBrowserModal"
      :title="genomeBrowserTitle"
      :disable-attachment="true"
      fullscreen
      hide-default-actions
      no-padding
      no-outside-dismiss
      @close="closeGenomeBrowser"
    >
      <div class="h-full flex flex-col" @click.stop>
        <!-- IGV Container -->
        <div
          v-if="selectedBrowserType === BROWSER_TYPES.IGV"
          id="igv-container"
          class="flex-1"
          style="min-height: 600px"
        ></div>

        <!-- WashU Container - v-if ensures full destruction on modal close -->
        <WashUBrowser
          v-if="
            showGenomeBrowserModal &&
            selectedBrowserType === BROWSER_TYPES.WASHU
          "
          :key="washuMountKey"
          :genome-name="genomeBrowserGenome"
          :data-hub="genomeBrowserTracks"
          :view-region="genomeBrowserRegion"
          class="h-full flex-1"
        />
      </div>
    </va-modal>
  </div>
</template>

<script setup>
import GenomeDisplay from "@/components/genome/GenomeDisplay.vue";
import BrowserSelectionModal from "@/components/genomeBrowser/BrowserSelectionModal.vue";
import WashUBrowser from "@/components/genomeBrowser/WashUBrowser.vue";
import DatasetDownloadModal from "@/components/project/datasets/DatasetDownloadModal.vue";
import StageDatasetModal from "@/components/project/datasets/StageDatasetModal.vue";
import DeleteSessionModal from "@/components/sessions/DeleteSessionModal.vue";
import UnstagedDatasetsModal from "@/components/sessions/UnstagedDatasetsModal.vue";
import TracksAsyncAutoComplete from "@/components/tracks/TracksAsyncAutoComplete.vue";
import AddEditButton from "@/components/utils/buttons/AddEditButton.vue";
import Pagination from "@/components/utils/Pagination.vue";
import config from "@/config";
import constants from "@/constants";
import datasetService from "@/services/dataset";
import * as datetime from "@/services/datetime";
import legacyMigrationService from "@/services/legacyMigration";
import sessionService from "@/services/session";
import toast from "@/services/toast";
import trackService from "@/services/track";
import { downloadFile, formatBytes } from "@/services/utils";
import { default as wfService, default as workflowService } from "@/services/workflow";
import { useAuthStore } from "@/stores/auth";
import { useNavStore } from "@/stores/nav";
import { useSessionsStore } from "@/stores/sessions";
import { HalfCircleSpinner } from "epic-spinners";
import { nextTick } from "vue";
import { useColors } from "vuestic-ui";
const route = useRoute();
const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();
const nav = useNavStore();
const { colors } = useColors();

// Reactive state
const _editModal = ref();
const showEditModal = ref(false);
const requestingStaging = ref(false);
const projectsLoading = ref(false);
const sessionProjects = ref([]);
const updating = ref(false);
const editForm = ref({
  title: "",
  genome_type: "",
  genome: "",
});
const showTracksModal = ref(false);
const updatingTracks = ref(false);
const selectedTracks = ref([]);
const trackSearch = ref("");
const canRetryStaging = ref(false);
const retryingStagingLoading = ref(false);
const lastStagingStatus = ref(null);

// Computed
const session = computed(() => sessionsStore.currentSession);
const loading = computed(() => sessionsStore.loading);
const _error = computed(() => sessionsStore.error);

const needsHydration = computed(() =>
  sessionService._needsHydration(session.value),
);

const isLegacySession = computed(() =>
  legacyMigrationService.isLegacySession(session.value),
);

// Session workflows display state (with collapse_model tracking)
const sessionWorkflowsDisplay = ref([]);

watch(
  () => session.value?.session_workflows,
  (newWorkflows) => {
    if (!newWorkflows) {
      sessionWorkflowsDisplay.value = [];
      return;
    }
    sessionWorkflowsDisplay.value = newWorkflows.map((w, i) => ({
      ...w,
      collapse_model:
        !workflowService.is_workflow_done(w) ||
        (sessionWorkflowsDisplay.value || [])[i]?.collapse_model ||
        false,
    }));
  },
  { immediate: true },
);

const hasActiveSessionWorkflows = computed(() =>
  sessionWorkflowsDisplay.value.some(
    (wf) => !workflowService.is_workflow_done(wf),
  ),
);

const sessionWorkflowPollingInterval = computed(() =>
  hasActiveSessionWorkflows.value ? config.dataset_polling_interval : null,
);

const { resume: resumeWorkflowPoll, pause: pauseWorkflowPoll } = useIntervalFn(
  () => loadSession(),
  sessionWorkflowPollingInterval,
  { immediate: false },
);

watch(hasActiveSessionWorkflows, (newVal) => {
  if (newVal) {
    resumeWorkflowPoll();
  } else {
    pauseWorkflowPoll();
  }
});

const canEditSession = computed(() => {
  return auth.canOperate || session.value?.user_id === auth.user?.id;
});

const canDeleteSession = computed(() => {
  return auth.canOperate || session.value?.user_id === auth.user?.id;
});

const _hasUnstagedTracks = computed(() => {
  if (!session.value?.session_tracks) return false;
  return session.value.session_tracks.some(
    (st) => !st.track.dataset_file?.dataset?.is_staged,
  );
});

// Table columns for selected tracks in modal
const selectedTracksColumns = [
  {
    key: "name",
    label: "Track Name",
    sortable: true,
    width: "30%",
  },
  {
    key: "genome",
    label: "Genome",
    sortable: true,
    width: "25%",
  },
  {
    key: "dataset",
    label: "Dataset",
    sortable: true,
    width: "25%",
  },
  {
    key: "size",
    label: "Size",
    sortable: true,
    width: "10%",
  },
  {
    key: "actions",
    label: "Actions",
    sortable: false,
    width: "10%",
  },
];

const selectedTracksTableData = computed(() => {
  return selectedTracks.value.map((track) => ({
    ...track,
    name: track.name || "",
    genome_type:
      track.dataset_file?.dataset?.genomic_details?.genome_type || null,
    genome_value:
      track.dataset_file?.dataset?.genomic_details?.genome_value || null,
    dataset: track.dataset_file?.dataset || null,
    size: track.dataset_file?.size || null,
  }));
});

const _unstagedTracks = computed(() => {
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

// Genome Browser constants
const { browserTypes: BROWSER_TYPES, browserTitles: BROWSER_TITLES } =
  constants.genomeBrowser;

// Unstaged datasets modal
const showUnstagedModal = ref(false);
const datasetsToStage = ref([]);

// Hydration modal
const showHydrationModal = ref(false);
const hydratingSession = ref(false);

// Genome Browser state (generic for IGV and WashU)
const showBrowserSelectionModal = ref(false);
const showGenomeBrowserModal = ref(false);
const genomeBrowserLoading = ref(false);
const selectedBrowserType = ref(null); // BROWSER_TYPES.IGV or BROWSER_TYPES.WASHU
const genomeBrowserGenome = ref("");
const genomeBrowserTracks = ref([]);
const genomeBrowserRegion = ref(""); // View region for browser
const washuMountKey = ref(0); // Force WashU remount on open
let igvBrowser = null; // IGV browser instance

const genomeBrowserTitle = computed(() => {
  if (selectedBrowserType.value === BROWSER_TYPES.IGV)
    return BROWSER_TITLES.igv;
  if (selectedBrowserType.value === BROWSER_TYPES.WASHU)
    return BROWSER_TITLES.washu;
  return BROWSER_TITLES.default;
});

const associatedTracks = computed(() => {
  if (!session.value?.session_tracks) return [];
  return session.value.session_tracks.map((st) => st.track);
});

// Track pagination
const currentTrackPage = ref(1);
const trackPageSize = ref(25);
const TRACK_PAGE_SIZE_OPTIONS = [25, 50, 100];

const trackStartIndex = computed(
  () => (currentTrackPage.value - 1) * trackPageSize.value,
);

const trackEndIndex = computed(() =>
  Math.min(
    trackStartIndex.value + trackPageSize.value,
    associatedTracks.value.length,
  ),
);

const paginatedTracks = computed(() => {
  return associatedTracks.value.slice(
    trackStartIndex.value,
    trackEndIndex.value,
  );
});

const trackColumns = [
  {
    key: "name",
    label: "Name",
    sortable: true,
    width: "20%",
    thAlign: "left",
    tdAlign: "left",
  },
  {
    key: "analysis_type",
    label: "Analysis Type",
    sortable: true,
    width: "12%",
  },
  {
    key: "genome",
    label: "Genome",
    sortable: true,
    width: "12%",
  },
  {
    key: "dataset_name",
    label: "Dataset",
    sortable: true,
  },
  {
    key: "stage",
    label: "Stage",
    width: "7%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "download_dataset",
    label: "Download Dataset",
    width: "12%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "download_file",
    label: "Download File",
    width: "10%",
    thAlign: "center",
    tdAlign: "center",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "10%",
    thAlign: "right",
    tdAlign: "right",
  },
];

// Project table columns
const _projectColumns = [
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

// Track dataset stage/download modals
const trackDatasetDownloadModal = ref(null);
const trackDatasetToDownload = ref({});
const trackDatasetStageModal = ref(null);
const trackDatasetToStage = ref({});

function openTrackDatasetDownloadModal(track) {
  trackDatasetToDownload.value = track.dataset_file?.dataset || {};
  trackDatasetDownloadModal.value.show();
}

function openTrackDatasetStageModal(track) {
  trackDatasetToStage.value = track.dataset_file?.dataset || {};
  trackDatasetStageModal.value.show();
}

function getTrackDatasetStagingStatus(track) {
  const workflows = track.dataset_file?.dataset?.workflows;
  return {
    is_staging_pending: wfService.is_staging_workflow_active(workflows),
    is_archival_pending: wfService.is_step_pending("archive", workflows),
  };
}

async function downloadTrackFile(track) {
  const datasetFile = track.dataset_file;
  if (!datasetFile) return;
  const datasetId = datasetFile.dataset?.id;
  if (!datasetId) return;

  try {
    const res = await datasetService.get_file_download_data({
      dataset_id: datasetId,
      file_id: datasetFile.id,
    });
    const url = new URL(res.data.url);
    url.searchParams.set("token", res.data.bearer_token);
    downloadFile({
      url: url.toString(),
      filename: datasetFile.name,
    });
  } catch (err) {
    console.error(err);
    toast.error("Unable to download file");
  }
}

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

const _requestStaging = async () => {
  if (!session.value) return;

  requestingStaging.value = true;
  try {
    const response = await sessionService.stage(session.value.id);

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
    const response = await sessionService.getProjects(session.value.id);
    sessionProjects.value = response.data.projects;
  } catch (error) {
    console.error("Failed to load session projects:", error);
    sessionProjects.value = [];
  } finally {
    projectsLoading.value = false;
  }
};

const _handleSessionUpdated = (_updatedSession) => {
  toast.success("Session updated successfully");
  // Refresh the session data
  loadSession();
};

/**
 * Handle View in Genome Browser button click
 * New flow: Check datasets first, then staging, then hydration (for legacy)
 */
const handleViewInBrowser = async () => {
  if (!session.value) return;

  try {
    // Get all datasets for the session (API uses metadata.datasets for legacy sessions)
    const [stagedResponse, unstagedResponse] = await Promise.all([
      sessionService.getDatasets(session.value.id, { staged: true }),
      sessionService.getDatasets(session.value.id, { staged: false }),
    ]);

    const stagedDatasets = stagedResponse.data.datasets || [];
    const unstagedDatasets = unstagedResponse.data.datasets || [];
    const totalDatasets = stagedDatasets.length + unstagedDatasets.length;

    // If there are no datasets at all, nothing to view
    if (totalDatasets === 0) {
      toast.warning("No datasets associated with this session");
      return;
    }

    // Check if any datasets need staging/migration
    const datasetsNeedingStaging = [];

    for (const dataset of unstagedDatasets) {
      // Check if dataset needs staging or migration
      const needsMigration =
        legacyMigrationService.isLegacyDataset(dataset) &&
        dataset.migration_status &&
        !dataset.migration_status.is_migrated;

      datasetsNeedingStaging.push({
        ...dataset,
        needs_migration: needsMigration,
      });
    }

    // If there are datasets needing staging, show the staging modal
    if (datasetsNeedingStaging.length > 0) {
      // Store datasets that need staging for modal to use
      datasetsToStage.value = datasetsNeedingStaging;
      showUnstagedModal.value = true;
      return;
    }

    // All datasets are staged - now check if legacy session needs hydration
    if (legacyMigrationService.isLegacySession(session.value)) {
      // Check hydration first - if already hydrated, proceed directly to browser selection
      const isHydrated = await legacyMigrationService.isSessionHydrated(
        session.value.id,
      );

      if (!isHydrated) {
        // Session not yet hydrated - check that all legacy datasets have finished migrating
        // before showing the hydration modal (migration is a prerequisite for hydration)
        const allDatasets = [...stagedDatasets, ...unstagedDatasets];
        const legacyDatasets = allDatasets.filter((ds) =>
          legacyMigrationService.isLegacyDataset(ds),
        );

        if (legacyDatasets.length > 0) {
          const allMigrated = legacyDatasets.every(
            (ds) => ds.migration_status && ds.migration_status.is_migrated,
          );

          if (!allMigrated) {
            toast.warning(
              "Some legacy datasets are still being migrated. Please wait for migration to complete.",
            );
            return;
          }
        }

        showHydrationModal.value = true;
        return;
      }
      // Session is already hydrated - proceed to Genome-Browser-selection
    }

    // All datasets are staged, all legacy datasets migrated, session hydrated (if needed)
    // Proceed to browser selection
    showBrowserSelectionModal.value = true;
  } catch (error) {
    console.error("Failed to check dataset status:", error);
    toast.error("Failed to check dataset staging status");
  }
};

/**
 * Handle staging requested
 * After staging workflows are triggered, check if we can proceed to hydration/browser
 */
const handleStagingRequested = async () => {
  toast.info(
    "Staging workflows have been requested. You can proceed once all workflows complete.",
  );
  showUnstagedModal.value = false;
  datasetsToStage.value = [];

  // Note: Don't automatically proceed to next step
  // User needs to click "View in Genome Browser" again after staging completes
  // This allows them to monitor workflow progress first
};

/**
 * Handle hydration confirmation
 */
const handleHydrateSession = async () => {
  if (!session.value?.id) return;

  hydratingSession.value = true;
  try {
    await sessionService.hydrateSession(session.value.id);
    toast.success(
      "Session hydration workflow started. Tracks will be available once hydration completes.",
    );
    showHydrationModal.value = false;
  } catch (error) {
    console.error("Failed to start session hydration:", error);
    toast.error("Failed to start session hydration workflow");
  } finally {
    hydratingSession.value = false;
  }
};

/**
 * Handle datasets updated from table
 */
const handleDatasetsUpdated = (updatedDatasets) => {
  // Check if there are any unstaged datasets
  const hasUnstagedDatasets = updatedDatasets.some((ds) => !ds.is_staged);

  // If data requested and we have unstaged datasets, allow retry
  canRetryStaging.value =
    session.value?.data_requested?.requested && hasUnstagedDatasets;
};

/**
 * Retry staging for unstaged datasets
 */
const retryStaging = async () => {
  if (!session.value?.id) return;

  retryingStagingLoading.value = true;
  try {
    const response = await sessionService.stageDatasets(session.value.id);
    lastStagingStatus.value = response.status;

    if (response.status === 200) {
      // All successful
      toast.success(response.data.message);
      canRetryStaging.value = false;
      // Reload session to update data_requested status
      await loadSession();
    } else if (response.status === 207) {
      // Partial success
      toast.warning(response.data.message);
      canRetryStaging.value = true;
    } else {
      // All failed
      toast.error(response.data.message);
      canRetryStaging.value = true;
    }
  } catch (error) {
    console.error("Failed to retry staging:", error);
    toast.error("Failed to initiate staging workflows");
    canRetryStaging.value = true;
  } finally {
    retryingStagingLoading.value = false;
  }
};

/**
 * Handle browser selection from modal
 */
const handleBrowserSelection = async (browserType) => {
  // Browser selection modal only shows if we have staged datasets
  selectedBrowserType.value = browserType;

  if (browserType === BROWSER_TYPES.IGV) {
    await initializeIGV();
  } else if (browserType === BROWSER_TYPES.WASHU) {
    await initializeWashU();
  }
};

/**
 * Initialize IGV browser
 */
const initializeIGV = async () => {
  if (!session.value) return;

  genomeBrowserLoading.value = true;

  try {
    // Set the authentication cookie for file access
    await sessionService.setFileCookie(session.value.id);

    // Fetch the datahub configuration for IGV
    const datahubResponse = await sessionService.getDatahub(
      session.value.id,
      BROWSER_TYPES.IGV,
    );
    const datahubConfig = datahubResponse?.data;

    console.log("[IGV] Datahub response:", datahubConfig);

    const tracks = datahubConfig?.tracks || [];
    // const tracks ß
    const genome = datahubConfig?.genome;

    console.log("[IGV] Tracks:", tracks);
    console.log("[IGV] Genome:", genome);

    if (!tracks || tracks.length === 0) {
      toast.error("No tracks available for this session");
      genomeBrowserLoading.value = false;
      return;
    }

    // Store for modal display
    genomeBrowserGenome.value = genome;
    genomeBrowserTracks.value = tracks;

    // Show genome browser modal
    showGenomeBrowserModal.value = true;

    // Wait for DOM to update
    await nextTick();

    // Dynamically import IGV
    const igvModule = await import("igv");
    const igv = igvModule.default;

    // Configure IGV options
    const igvOptions = {
      genome,
      tracks,
    };

    // Create IGV browser instance
    const container = document.getElementById("igv-container");
    if (container) {
      igvBrowser = await igv.createBrowser(container, igvOptions);
      console.log("[IGV] Browser loaded successfully");
    } else {
      throw new Error("IGV container not found");
    }
  } catch (error) {
    console.error("[IGV] Failed to initialize:", error);
    toast.error("Failed to load IGV browser");
    showGenomeBrowserModal.value = false;
  } finally {
    genomeBrowserLoading.value = false;
  }
};

/**
 * Initialize WashU browser
 */
const initializeWashU = async () => {
  if (!session.value) return;

  genomeBrowserLoading.value = true;

  try {
    // Set the authentication cookie for file access
    await sessionService.setFileCookie(session.value.id);

    // Fetch the datahub configuration for WashU
    const datahubResponse = await sessionService.getDatahub(
      session.value.id,
      BROWSER_TYPES.WASHU,
    );
    const datahubConfig = datahubResponse.data;

    console.log("[WashU] Datahub response:", datahubConfig);

    const tracks = datahubConfig.tracks || [];
    const genome = datahubConfig.genome;
    // const genome = 'hg38';

    console.log("[WashU] Tracks:", tracks);
    console.log("[WashU] Genome:", genome);

    if (!tracks || tracks.length === 0) {
      toast.error("No tracks available for this session");
      genomeBrowserLoading.value = false;
      return;
    }

    // TEMPORARY TEST: Add a public BigWig file to test if WashU works at all
    // const testTrack = {
    //   type: 'bigwig',
    //   // name: 'Test Public BigWig',
    //   url: 'https://www.encodeproject.org/files/ENCFF356YES/@@download/ENCFF356YES.bigWig',
    //   options: {
    //     backgroundColor: '#ff0000',
    //     height: 50,
    //   },
    // };

    // Store for modal display
    genomeBrowserGenome.value = genome;
    genomeBrowserTracks.value = tracks;
    genomeBrowserRegion.value =
      datahubConfig.locus || "chr1:155000000-155050000";

    // Force fresh WashU mount by incrementing key
    washuMountKey.value++;

    // Show genome browser modal (WashU component will mount automatically)
    showGenomeBrowserModal.value = true;

    console.log(
      "[WashU] Browser will initialize via component with mount key:",
      washuMountKey.value,
    );
  } catch (error) {
    console.error("[WashU] Failed to initialize:", error);
    toast.error("Failed to load WashU browser");
    showGenomeBrowserModal.value = false;
  } finally {
    genomeBrowserLoading.value = false;
  }
};

/**
 * Close genome browser (IGV or WashU)
 */
const closeGenomeBrowser = () => {
  // Clean up IGV browser instance if it exists
  if (igvBrowser) {
    igvBrowser.dispose();
    igvBrowser = null;
  }

  // WashU cleanup is handled by its component's onBeforeUnmount

  showGenomeBrowserModal.value = false;
  selectedBrowserType.value = null;
  genomeBrowserGenome.value = "";
  genomeBrowserTracks.value = [];
  genomeBrowserRegion.value = "";

  console.log("[Genome Browser] Closed and cleaned up");
};

const updateSession = async () => {
  updating.value = true;
  try {
    await sessionsStore.updateSession(session.value.id, editForm.value);
    toast.success("Session updated successfully");
    showEditModal.value = false;
  } catch (error) {
    console.error("Failed to update session:", error);
    toast.error("Failed to update session");
  } finally {
    updating.value = false;
  }
};

const handleTrackSelect = (track) => {
  // Add track to selected tracks if not already present
  const existingIndex = selectedTracks.value.findIndex(
    (t) => t.id === track.id,
  );
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
    await sessionsStore.updateSession(session.value.id, {
      track_ids: trackIds,
    });
    toast.success("Session tracks updated successfully");
    showTracksModal.value = false;
    await loadSession(); // Reload session data
  } catch (error) {
    console.error("Failed to update session tracks:", error);
    toast.error("Failed to update session tracks");
  } finally {
    updatingTracks.value = false;
  }
};

// Watch for modal opening to initialize form
watch(showEditModal, (isOpen) => {
  if (isOpen && session.value) {
    editForm.value = {
      title: session.value.title || "",
      genome_type: session.value.genome_type || "",
      genome: session.value.genome || "",
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
  // Clean up genome browser instances
  closeGenomeBrowser();
});
</script>

<route lang="yaml">
meta:
  title: Session Details
</route>
