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
              color="primary"
              border-color="primary"
              preset="secondary"
              class="flex-initial"
              @click="addToSession"
            >
              Add to Session
            </va-button>

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
