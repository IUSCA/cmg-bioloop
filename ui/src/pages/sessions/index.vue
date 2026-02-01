<template>
  <div>
    <!-- search bar and filter -->
    <div class="flex mb-3 gap-3">
      <!-- search bar -->
      <div class="flex-1" v-if="activeFilters.length === 0">
        <va-input
          :model-value="inclusive_query"
          class="w-full"
          placeholder="Search Sessions by title"
          outline
          clearable
          @update:model-value="handleMainFilter"
        >
          <template #prependInner>
            <Icon icon="material-symbols:search" class="text-xl" />
          </template>
        </va-input>
      </div>

      <!-- Filter button -->
      <va-button @click="showSearchModal = true" preset="primary" class="flex-none">
        <i-mdi-filter />
        <span> Filters </span>
      </va-button>

      <!-- active filter chips -->
      <session-search-filters
        v-if="activeFilters.length > 0"
        class="flex-none"
        :filters="filters"
        @remove-filter="removeFilter"
        @clear-all="clearFilters"
      />

      <!-- Create Session button -->
      <div class="flex-none">
        <va-button icon="add" class="px-1" color="success" @click="router.push('/sessions/new')">
          Create Session
        </va-button>
      </div>
    </div>

    <!-- Sessions table -->
    <va-data-table
      :items="sessions"
      :columns="columns"
      :loading="loading"
      v-model:sort-by="query.sort_by"
      v-model:sort-order="query.sort_order"
    >
      <template #cell(title)="{ rowData }">
        <router-link :to="`/sessions/${rowData.id}`" class="va-link">
          {{ rowData?.title }}
        </router-link>
      </template>

      <template #cell(genome)="{ rowData }">
        <va-chip v-if="rowData.genome_type || rowData.genome" size="small">
          {{ rowData.genome_type || '' }}{{ rowData.genome ? ` (${rowData.genome})` : '' }}
        </va-chip>
      </template>

      <template #cell(tracks_count)="{ rowData }">
        <span class="text-sm">
          {{ rowData.session_tracks.length || 0 }}
        </span>
      </template>

      <template #cell(user)="{ rowData }">
        <div class="">
          <div class="">{{ rowData?.user?.name }}</div>
        </div>
      </template>

      <template #cell(created_at)="{ value }">
        <span class=" ">
          {{ date(value) }}
        </span>
      </template>

      <template #cell(actions)="{ rowData }">
        <div class="flex gap-1 justify-end">
          <va-button
            v-if="canDeleteSession(rowData)"
            preset="plain"
            color="danger"
            @click="openDeleteModal(rowData)"
          >
            <va-icon name="delete" />
          </va-button>
        </div>
      </template>
    </va-data-table>

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

    <!-- Delete Modal -->
    <DeleteSessionModal ref="deleteModal" :data="selectedForDeletion" @update="fetchSessions" />
  </div>
</template>

<script setup>
import DeleteSessionModal from '@/components/sessions/DeleteSessionModal.vue';
import SessionSearchFilters from '@/components/sessions/SessionSearchFilters.vue';
import SessionSearchModal from '@/components/sessions/SessionSearchModal.vue';
import Pagination from '@/components/utils/Pagination.vue';
import { date } from '@/services/datetime';
import { useAuthStore } from '@/stores/auth';
import { useSessionsStore } from '@/stores/sessions';
import { useDebounceFn } from '@vueuse/core';
import { computed, onMounted, ref, watch } from 'vue';
import { useRouter } from 'vue-router';

const router = useRouter();
const sessionsStore = useSessionsStore();
const auth = useAuthStore();

const PAGE_SIZE_OPTIONS = [25, 50, 100];

// Reactive state
const showSearchModal = ref(false);
const inclusive_query = ref('');

// Query parameters
const query = ref({
  page: 1,
  page_size: 25,
  sort_by: 'created_at',
  sort_order: 'desc',
});

// Filters
const filters = ref({
  title: '',
  genome: '',
  genome_type: '',
});

// Default values function for query persistence
const defaultParams = () => ({
  page: 1,
  page_size: 25,
  sort_by: 'created_at',
  sort_order: 'desc',
});

const defaultFilters = () => ({
  title: '',
  genome: '',
  genome_type: '',
});

// Computed
const sessions = computed(() => sessionsStore.sessions);
const loading = computed(() => sessionsStore.loading);
const metadata = computed(() => sessionsStore.metadata);

console.log('sessions', sessions.value);

const activeFilters = computed(() => {
  const active = [];
  Object.entries(filters.value).forEach(([key, value]) => {
    if (value && value.trim() !== '') {
      active.push({ key, value });
    }
  });
  return active;
});

// Table columns configuration
const columns = [
  {
    key: 'title',
    label: 'Title',
    sortable: true,
    width: '29%',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    align: 'left',
  },
  {
    key: 'genome',
    label: 'Genome',
    sortable: true,
    width: '20%',
  },
  {
    key: 'tracks_count',
    label: 'Tracks',
    sortable: false,
    width: '10%',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'user',
    label: 'Created By',
    sortable: false,
    width: '15%',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'created_at',
    label: 'Created',
    sortable: true,
    width: '10%',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'actions',
    label: 'Actions',
    sortable: false,
    thAlign: 'right',
    tdAlign: 'right',
    width: '6%',
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

    await sessionsStore.fetchSessions(params);
    console.log('sessions', sessions.value);
  } catch (error) {
    console.error('Error fetching sessions:', error);
  }
};

const handleMainFilter = useDebounceFn((value) => {
  inclusive_query.value = value;
  query.value.page = 1; // Reset to first page when searching
  fetchSessions();
}, 300);

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
  inclusive_query.value = '';
  query.value.page = 1; // Reset to first page when resetting
  showSearchModal.value = false;
};

const removeFilter = (key) => {
  filters.value[key] = '';
  if (key === 'title') {
    inclusive_query.value = '';
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
  { deep: true }
);

// Initial load
onMounted(() => {
  fetchSessions();
});
</script>

<route lang="yaml">
meta:
  title: Sessions
  requiresRoles: ['operator', 'admin']
  nav: [{ label: 'Sessions' }]
</route>
