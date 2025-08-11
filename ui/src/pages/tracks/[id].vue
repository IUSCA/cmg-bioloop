<template>
  <div class="track-detail p-6">
    <div v-if="loading" class="flex justify-center items-center h-64">
      <va-progress-circular indeterminate />
    </div>

    <div v-else-if="error" class="text-center text-red-600">
      {{ error }}
    </div>

    <div v-else-if="track" class="space-y-6">
      <!-- Breadcrumbs -->
      <va-breadcrumbs class="text-lg breadcrumbs">
        <va-breadcrumbs-item to="/tracks" label="Tracks" />
        <va-breadcrumbs-item :label="track.name" />
      </va-breadcrumbs>

      <!-- Header -->
      <div class="flex justify-between items-start">
        <div>
          <h1 class="text-3xl font-bold">{{ track.name }}</h1>
          <p class="text-gray-600 mt-2">
            Track ID: {{ track.id }} • Created
            {{ datetime.fromNow(track.created_at) }}
          </p>
        </div>
      </div>

      <!-- Track Information Grid -->
      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <!-- Basic Information -->
        <va-card>
          <va-card-title>Basic Information</va-card-title>
          <va-card-content>
            <div class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Track Name:</span>
                <span>{{ track.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Type:</span>
                <va-chip
                  :color="getFileTypeColor(track.file_type)"
                  size="small"
                >
                  {{ track.file_type?.toUpperCase() || "Unknown" }}
                </va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Type:</span>
                <va-chip outline size="small">{{ track.genomeType }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Genome Version:</span>
                <va-chip outline size="small">{{ track.genomeValue }}</va-chip>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Created:</span>
                <span>{{ datetime.date(track.created_at) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Last Updated:</span>
                <span>{{ datetime.fromNow(track.updated_at) }}</span>
              </div>
            </div>
          </va-card-content>
        </va-card>

        <!-- Dataset Information -->
        <va-card>
          <va-card-title>Dataset Information</va-card-title>
          <va-card-content>
            <div v-if="track.dataset_file?.dataset" class="space-y-4">
              <div class="flex justify-between">
                <span class="font-medium">Dataset Name:</span>
                <router-link
                  :to="`/datasets/${track.dataset_file.dataset.id}`"
                  class="va-link"
                >
                  {{ track.dataset_file.dataset.name }}
                </router-link>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Dataset Type:</span>
                <span>{{ track.dataset_file.dataset.type }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Name:</span>
                <span>{{ track.dataset_file.name }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Size:</span>
                <span>{{ formatFileSize(track.dataset_file.size) }}</span>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">File Path:</span>
                <div class="flex-1 ml-4">
                  <CopyText :text="track.dataset_file.path" />
                </div>
              </div>
              <div class="flex justify-between">
                <span class="font-medium">Staging Status:</span>
                <va-chip
                  :color="
                    track.dataset_file.dataset.is_staged ? 'success' : 'warning'
                  "
                  size="small"
                >
                  {{
                    track.dataset_file.dataset.is_staged
                      ? "Staged"
                      : "Not Staged"
                  }}
                </va-chip>
              </div>
            </div>
            <div v-else class="text-center text-gray-500 py-4">
              No dataset information available
            </div>
          </va-card-content>
        </va-card>
      </div>

      <!-- Associated Sessions -->
      <va-card>
        <va-card-title>Associated Sessions</va-card-title>
        <va-card-content>
          <div v-if="track.session_tracks?.length" class="space-y-3">
            <div class="text-sm text-gray-600 mb-3">
              This track is used in {{ track.session_tracks.length }} session(s)
            </div>
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
                <span class="text-sm text-gray-600">
                  {{
                    rowData.session.user?.name || rowData.session.user?.username
                  }}
                </span>
              </template>
              <template #cell(created_at)="{ rowData }">
                <span class="text-sm text-gray-500">
                  {{ datetime.fromNow(rowData.session.created_at) }}
                </span>
              </template>
              <template #cell(color)="{ rowData }">
                <span v-if="rowData.color" class="text-sm text-gray-500">
                  {{ rowData.color }}
                </span>
                <span v-else class="text-sm text-gray-400">-</span>
              </template>
              <template #cell(order)="{ rowData }">
                <span class="text-sm text-gray-500">
                  {{ rowData.order + 1 }}
                </span>
              </template>
            </va-data-table>
          </div>
          <div v-else class="text-center text-gray-500 py-8">
            This track is not used in any sessions yet.
          </div>
        </va-card-content>
      </va-card>

      <!-- Actions -->
      <va-card>
        <va-card-title>Actions</va-card-title>
        <va-card-content>
          <div class="flex gap-3">
            <va-button preset="secondary" @click="addToSession">
              <va-icon name="plus" />
              Add to Session
            </va-button>

            <va-button
              v-if="auth.canOperate"
              preset="danger"
              @click="deleteTrack"
            >
              <va-icon name="delete" />
              Delete Track
            </va-button>
          </div>
        </va-card-content>
      </va-card>
    </div>
  </div>
</template>

<script setup>
import CopyText from "@/components/utils/CopyText.vue";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import { useAuthStore } from "@/stores/auth";
import { useTracksStore } from "@/stores/tracks";
import { computed, onMounted } from "vue";
import { useRoute, useRouter } from "vue-router";

const route = useRoute();
const router = useRouter();
const tracksStore = useTracksStore();
const auth = useAuthStore();

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
    width: "30%",
  },
  {
    key: "created_by",
    label: "Created By",
    sortable: true,
    width: "20%",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "20%",
  },
  {
    key: "color",
    label: "Color",
    sortable: false,
    width: "15%",
  },
  {
    key: "order",
    label: "Order",
    sortable: true,
    width: "15%",
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

const addToSession = () => {
  // Navigate to create session page with this track pre-selected
  router.push({
    path: "/sessions/new",
    query: { track_id: track.value.id },
  });
};

const deleteTrack = async () => {
  if (
    !confirm(
      "Are you sure you want to delete this track? This action cannot be undone.",
    )
  ) {
    return;
  }

  try {
    await tracksStore.deleteTrack(track.value.id);
    toast.success("Track deleted successfully");
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
  nav: [{ label: "Tracks", to: "/tracks" }, { label: "Track Details" }]
</route>
