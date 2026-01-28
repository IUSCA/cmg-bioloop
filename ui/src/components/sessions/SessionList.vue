<template>
  <div class="session-list">
    <!-- Header with search and create button -->
    <div class="flex justify-between items-center mb-6">
      <div class="flex items-center gap-4">
        <va-input
          v-model="inclusive_query"
          placeholder="Search Sessions by title"
          class="w-80"
          @input="handleSearch"
        />
        <va-button preset="primary" @click="showSearchModal = true">
          <va-icon name="mdi-filter" class="mr-2" />
          Filters
        </va-button>
      </div>

      <va-button preset="primary" @click="showCreateModal = true">
        <va-icon name="mdi-plus" class="mr-2" />
        Create Session
      </va-button>
    </div>

    <!-- Active filters -->
    <session-search-filters
      v-if="hasActiveFilters"
      :filters="filters"
      @remove-filter="removeFilter"
      @clear-all="clearFilters"
    /> 

    <!-- Sessions table -->
    <va-card>
      <va-data-table
        :items="sessions"
        :columns="columns"
        :loading="loading"
        v-model:sort-by="query.sort_by"
        v-model:sort-order="query.sort_order"
      >
        <template #cell(title)="{ item }">
          <router-link :to="`/sessions/${item.id}`" class="va-link">
            {{ item.title }}
          </router-link>
        </template>

        <template #cell(genome)="{ item }">
          <va-chip v-if="item.genome" size="small" preset="primary">
            {{ item.genome }}
          </va-chip>
        </template>

        <template #cell(genome_type)="{ item }">
          <va-chip v-if="item.genome_type" size="small" preset="secondary">
            {{ item.genome_type }}
          </va-chip>
        </template>

        <template #cell(tracks_count)="{ item }">
          <span class="text-sm text-gray-600">
            {{ item._count?.session_tracks || 0 }} tracks
          </span>
        </template>

        <template #cell(owner)="{ item }">
          <div class="text-sm">
            <div class="font-medium">
              {{ item.user?.name || item.user?.username }}
            </div>
            <div class="text-gray-500">{{ item.user?.username }}</div>
          </div>
        </template>

        <template #cell(created_at)="{ item }">
          <span class="text-sm text-gray-600">
            {{ date(item.created_at) }}
          </span>
        </template>

        <template #cell(actions)="{ item }">
          <div class="flex gap-1">

            <va-button
              v-if="canDeleteSession(item)"
              preset="plain"
              color="danger"
              class="flex-auto"
              @click="openDeleteModal(item)"
            >
              <va-icon name="delete" />
            </va-button>
          </div>
        </template>
      </va-data-table>
    </va-card>

    <!-- Pagination -->
    <Pagination
      class="mt-4"
      v-model:page="query.page"
      v-model:page_size="query.page_size"
      :total_results="metadata?.count || 0"
      :curr_items="sessions.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />

    <!-- Search Modal -->
    <session-search-modal
      v-model="showSearchModal"
      :filters="filters"
      @apply="applyFilters"
      @reset="resetFilters"
    />

    <!-- Create Modal -->
    <create-session-modal
      v-model="showCreateModal"
      @created="handleSessionCreated"
    />

    <!-- Delete Modal -->
    <DeleteSessionModal
      ref="deleteModal"
      :data="selectedForDeletion"
      @update="fetchSessions"
    />
  </div>
</template>

<script setup>
import Pagination from "@/components/utils/Pagination.vue";
import { date } from "@/services/datetime";
import { useAuthStore } from "@/stores/auth";
import { useSessionsStore } from "@/stores/sessions";
import { useDebounceFn } from "@vueuse/core";
import { computed, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import DeleteSessionModal from "./DeleteSessionModal.vue";
import SessionSearchFilters from "./SessionSearchFilters.vue";
import SessionSearchModal from "./SessionSearchModal.vue";

const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();

const PAGE_SIZE_OPTIONS = [25, 50, 100];

// Reactive state
const showSearchModal = ref(false);
const showCreateModal = ref(false);
const inclusive_query = ref("");

// Query parameters
const query = ref({
  page: 1,
  page_size: 25,
  sort_by: "created_at",
  sort_order: "desc",
});

// Filters
const filters = ref({
  title: "",
  genome: "",
  genome_type: "",
});

// Default values function for query persistence
const defaultParams = () => ({
  page: 1,
  page_size: 25,
  sort_by: "created_at",
  sort_order: "desc",
});

const defaultFilters = () => ({
  title: "",
  genome: "",
  genome_type: "",
});

// Computed
const sessions = computed(() => {
  console.log('[SessionList] sessions:', sessionsStore.sessions.length);
  return sessionsStore.sessions;
});
const loading = computed(() => sessionsStore.loading);
const metadata = computed(() => {
  console.log('[SessionList] metadata:', sessionsStore.metadata);
  return sessionsStore.metadata;
});

const hasActiveFilters = computed(() => {
  return Object.values(filters.value).some(
    (value) => value && value.trim() !== "",
  );
});

// Table columns configuration
const columns = [
  {
    key: "title",
    label: "Title",
    sortable: true,
    width: "25%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "genome",
    label: "Genome",
    sortable: true,
    width: "15%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "genome_type",
    label: "Genome Type",
    sortable: true,
    width: "15%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "tracks_count",
    label: "Tracks",
    sortable: false,
    width: "10%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "owner",
    label: "Owner",
    sortable: false,
    width: "15%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "created_at",
    label: "Created",
    sortable: true,
    width: "10%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
  {
    key: "actions",
    label: "Actions",
    sortable: false,
    width: "10%",
    thStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
    tdStyle:
      "white-space: pre-wrap; word-wrap: break-word; word-break: break-word;",
  },
];

// Methods

const canDeleteSession = (session) => {
  return session.user_id === auth.user?.id;
};

const fetchSessions = async () => {
  try {
    const params = {
      ...filters.value,
      ...(inclusive_query.value ? { title: inclusive_query.value } : {}),
      limit: query.value.page_size,
      offset: (query.value.page - 1) * query.value.page_size,
      sort_by: query.value.sort_by,
      sort_order: query.value.sort_order,
    };

    console.log('[SessionList] Fetching with params:', params);
    const result = await sessionsStore.fetchSessions(params);
    console.log('[SessionList] API returned:', result);
  } catch (error) {
    console.error("Error fetching sessions:", error);
  }
};

const handleSearch = useDebounceFn(() => {
  query.value.page = 1; // Reset to first page when searching
  fetchSessions();
}, 300);

const applyFilters = (newFilters) => {
  filters.value = { ...newFilters };
  query.value.page = 1; // Reset to first page when filtering
  showSearchModal.value = false;
};

const resetFilters = () => {
  filters.value = { ...defaultFilters() };
  inclusive_query.value = "";
  query.value.page = 1; // Reset to first page when resetting
  showSearchModal.value = false;
};

const removeFilter = (key) => {
  filters.value[key] = "";
  if (key === "title") {
    inclusive_query.value = "";
  }
  query.value.page = 1; // Reset to first page when removing filter
};

const clearFilters = () => {
  resetFilters();
};

const viewSession = (session) => {
  // Navigate to session detail page
  router.push(`/sessions/${session.id}`);
};


const deleteModal = ref(null);
const selectedForDeletion = ref({});

function openDeleteModal(session) {
  selectedForDeletion.value = session;
  deleteModal.value.show();
}

const handleSessionCreated = () => {
  showCreateModal.value = false;
  fetchSessions(); // Refresh the list
};


// Watch for changes in query and filters
watch(
  [query, filters],
  (newVals, oldVals) => {
    // Reset to page 1 when sort changes
    if (oldVals[0] && (
      newVals[0].sort_by !== oldVals[0].sort_by ||
      newVals[0].sort_order !== oldVals[0].sort_order
    )) {
      query.value.page = 1;
    }
    fetchSessions();
  },
  { deep: true },
);

// Initial load
onMounted(() => {
  fetchSessions();
});
</script>

<style scoped>
.session-list {
  padding: 1rem;
}
</style>
