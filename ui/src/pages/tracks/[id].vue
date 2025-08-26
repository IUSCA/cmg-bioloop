<template>
  <div class="track-detail">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circular indeterminate />
    </div>

    <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div>

    <div v-else-if="track" class="space-y-6">
      <!-- Track Information Grid -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Basic Information -->
        <va-card>
          <va-card-title>
            <span class="text-lg">Basic Information</span>
          </va-card-title>
          <va-card-content>
            <div class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Track Name:</span>
                <span>{{ track.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Type</span>
                <va-chip
                  :color="getFileTypeColor(track.file_type)"
                  size="small"
                >
                  {{ track.file_type?.toUpperCase() || "Unknown" }}
                </va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Type</span>
                <va-chip outline size="small">{{ track.genomeType }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Version</span>
                <va-chip outline size="small">{{ track.genomeValue }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created</span>
                <span>{{ datetime.date(track.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated</span>
                <span>{{ datetime.fromNow(track.updated_at) }}</span>
              </div>
            </div>
          </va-card-content>
        </va-card>

        <!-- Dataset Information -->
        <va-card>
          <va-card-title>
            <span class="text-lg">Dataset Information</span>
          </va-card-title>
          <va-card-content>
            <div v-if="track.dataset_file?.dataset" class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Dataset Name</span>
                <router-link
                  :to="`/datasets/${track.dataset_file.dataset.id}`"
                  class="va-link"
                >
                  {{ track.dataset_file.dataset.name }}
                </router-link>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Dataset Type</span>
                <span>{{ track.dataset_file.dataset.type }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Name</span>
                <span>{{ track.dataset_file.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Size</span>
                <span>{{ formatFileSize(track.dataset_file.size) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Path</span>
                <div class="w-80">
                  <CopyText :text="track.dataset_file.path" />
                </div>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Staged</span>
                <div
                  v-if="track.dataset_file.dataset.is_staged"
                  class="text-green-700"
                >
                  <va-icon name="check_circle_outline" />
                </div>
                <div v-else class="text-warning">
                  <va-icon name="close_circle_outline" />
                </div>
              </div>
            </div>
            <div v-else class="text-center py-4">
              No dataset information available
            </div>
          </va-card-content>
        </va-card>
      </div>

      <!-- Associated Sessions -->
      <va-card>
        <va-card-title>
          <span class="text-lg">Associated Sessions</span>
        </va-card-title>
        <va-card-content>
          <div v-if="track.session_tracks?.length" class="space-y-3">
            <va-data-table
              :items="track.session_tracks"
              :columns="sessionColumns"
              :loading="false"
              disable-client-side-sorting
            >
              <template #cell(session_title)="{ rowData }">
                <router-link
                  :to="`/sessions/${rowData.session.id}`"
                  class="va-link font-medium"
                >
                  {{ rowData.session.title }}
                </router-link>
              </template>
              <template #cell(created_by)="{ rowData }">
                <span>
                  {{
                    rowData.session.user?.name || rowData.session.user?.username
                  }}
                </span>
              </template>
              <template #cell(created_at)="{ rowData }">
                <span>
                  {{ datetime.fromNow(rowData.session.created_at) }}
                </span>
              </template>
              <template #cell(color)="{ rowData }">
                <div v-if="rowData.color" class="flex items-center gap-2">
                  <div
                    class="w-4 h-4 rounded border"
                    :style="{ backgroundColor: rowData.color }"
                  ></div>
                  <span>{{ rowData.color }}</span>
                </div>
                <span v-else>-</span>
              </template>
              <template #cell(order)="{ rowData }">
                <span>
                  {{ rowData.order + 1 }}
                </span>
              </template>
            </va-data-table>
          </div>
          <div v-else class="text-center py-8">
            This track is not used in any sessions yet.
          </div>
        </va-card-content>
      </va-card>

      <!-- Actions -->
      <va-card>
        <va-card-title>
          <span class="text-lg">Actions</span>
        </va-card-title>
        <va-card-content>
          <div class="flex justify-start gap-3">
            <va-button
              v-if="auth.canOperate"
              color="danger"
              border-color="danger"
              preset="secondary"
              class="flex-initial"
              @click="deleteTrack"
            >
              <va-icon name="delete" class="pr-2 text-2xl" />
              Delete Track
            </va-button>
          </div>
        </va-card-content>
      </va-card>
    </div>

    <!-- Delete Track Modal -->
    <va-modal :model-value="deleteModal.visible" blur hide-default-actions>
      <template #header>
        <div class="flex justify-end">
          <va-button
            class="flex-initial"
            preset="plain"
            @click="deleteModal.visible = false"
          >
            <va-icon name="close" />
          </va-button>
        </div>
      </template>

      <div>
        <h3 class="va-h5">Delete Track?</h3>

        <va-divider class="my-2" />

        <div class="flex flex-col items-center gap-4">
          <div>
            <va-icon name="mdi-dna" class="text-3xl" />
          </div>

          <!-- Track Name and Type -->
          <div class="text-center">
            <span class="text-xl tracking-wide font-medium">
              {{ track?.file_type?.toUpperCase() || "Unknown" }} /
              {{ track?.name || "Unknown Track" }}
            </span>
          </div>

          <!-- Metadata Grid -->
          <div class="grid grid-cols-2 gap-6 w-full max-w-md">
            <!-- File Size -->
            <div class="flex flex-col items-center text-center">
              <div class="flex items-center gap-2 mb-1">
                <va-icon name="mdi-file" class="text-lg text-gray-600" />
                <span class="text-sm font-medium text-gray-700">File Size</span>
              </div>
              <span class="text-sm">
                {{
                  track?.dataset_file?.size
                    ? formatFileSize(track.dataset_file.size)
                    : "Size unknown"
                }}
              </span>
            </div>

            <!-- Dataset -->
            <div class="flex flex-col items-center text-center">
              <div class="flex items-center gap-2 mb-1">
                <va-icon name="mdi-database" class="text-lg text-gray-600" />
                <span class="text-sm font-medium text-gray-700">Dataset</span>
              </div>
              <span class="text-sm">
                {{ track?.dataset_file?.dataset?.name || "Dataset unknown" }}
              </span>
            </div>

            <!-- Genome Type -->
            <div class="flex flex-col items-center text-center">
              <div class="flex items-center gap-2 mb-1">
                <va-icon name="mdi-dna" class="text-lg text-gray-600" />
                <span class="text-sm font-medium text-gray-700"
                  >Genome Type</span
                >
              </div>
              <span class="text-sm">
                {{ track?.genomeType || "Unknown" }}
              </span>
            </div>

            <!-- Genome Version -->
            <div class="flex flex-col items-center text-center">
              <div class="flex items-center gap-2 mb-1">
                <va-icon name="mdi-tag" class="text-lg text-gray-600" />
                <span class="text-sm font-medium text-gray-700"
                  >Genome Version</span
                >
              </div>
              <span class="text-sm">
                {{ track?.genomeValue || "Unknown" }}
              </span>
            </div>
          </div>
        </div>

        <va-divider class="my-4" />

        <div>
          <va-alert color="#fdeae7" text-color="#940909" class="text-center">
            <span> This action cannot be undone! </span>
          </va-alert>

          <ul class="va-unordered va-text-secondary mt-3">
            <li>
              This will permanently delete the track
              <b>{{ track?.name || "Unknown Track" }}</b> and remove it from all
              sessions.
            </li>
            <li>This will not delete the underlying dataset file.</li>
            <li>This will not delete the dataset itself.</li>
            <li>
              This action will affect any sessions that currently use this
              track.
            </li>
          </ul>
        </div>

        <va-divider class="my-4" />

        <div class="flex justify-end gap-3">
          <va-button preset="secondary" @click="deleteModal.visible = false">
            Cancel
          </va-button>
          <va-button color="danger" @click="confirmDeleteTrack">
            Delete Track
          </va-button>
        </div>
      </div>
    </va-modal>
  </div>
</template>

<script setup>
import CopyText from "@/components/utils/CopyText.vue";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import { useAuthStore } from "@/stores/auth";
import { useNavStore } from "@/stores/nav";
import { useTracksStore } from "@/stores/tracks";
import { computed, onMounted } from "vue";
import { useRoute, useRouter } from "vue-router";

const route = useRoute();
const router = useRouter();
const tracksStore = useTracksStore();
const auth = useAuthStore();
const nav = useNavStore();

// Reactive state
const deleteModal = ref({
  visible: false,
});

// Computed
const track = computed(() => tracksStore.currentTrack);
const loading = computed(() => tracksStore.loading);
const error = computed(() => tracksStore.error);

// Session table columns
const sessionColumns = [
  {
    key: "session_title",
    label: "Session Title",
    sortable: true,
    width: "35%",
    thAlign: "left",
    tdAlign: "left",
  },
  {
    key: "created_by",
    label: "Created By",
    sortable: true,
    width: "25%",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "25%",
  },
  // {
  //   key: "color",
  //   label: "Color",
  //   sortable: false,
  //   width: "15%",
  // },
  {
    key: "order",
    label: "Order",
    sortable: true,
    width: "15%",
    thAlign: "right",
    tdAlign: "right",
  },
];

// Methods
const getFileTypeColor = (fileType) => {
  const colors = {
    bam: "primary",
    bigwig: "success",
    bw: "success",
    vcf: "warning",
    bed: "info",
    gtf: "secondary",
  };
  return colors[fileType] || "secondary";
};

const formatFileSize = (bytes) => {
  if (!bytes) return "Unknown";
  const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return Math.round((bytes / Math.pow(1024, i)) * 100) / 100 + " " + sizes[i];
};

const deleteTrack = () => {
  deleteModal.value.visible = true;
};

const confirmDeleteTrack = async () => {
  try {
    await tracksStore.deleteTrack(track.value.id);
    toast.success("Track deleted successfully");
    deleteModal.value.visible = false;
    router.push("/tracks");
  } catch (error) {
    console.error("Failed to delete track:", error);
    toast.error("Failed to delete track");
  }
};

// Load track data
onMounted(async () => {
  try {
    await tracksStore.fetchTrack(route.params.id);

    // Set dynamic breadcrumb navigation
    if (track.value) {
      nav.setNavItems([
        {
          label: "Tracks",
          to: "/tracks",
        },
        {
          label: track.value.name,
        },
      ]);
    }
  } catch (error) {
    console.error("Failed to load track:", error);
    toast.error("Failed to load track");
  }
});
</script>

<route lang="yaml">
meta:
  title: Track Details
  requiresRoles: ["operator", "admin"]
</route>
