<template>
  <div>
    <!-- search bar and filter -->
    <div class="flex mb-3 gap-3">
      <!-- search bar -->
      <div class="flex-1" v-if="activeFilters.length === 0">
        <va-input
          :model-value="query.inclusive_query"
          class="w-full"
          placeholder="Search Tracks by name"
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
      <va-button @click="searchModal.show()" preset="primary" class="flex-none">
        <i-mdi-filter />
        <span> Filters </span>
      </va-button>

      <!-- active filter chips -->
      <TrackSearchFilters
        v-if="activeFilters.length > 0"
        class="flex-none"
        :filters="filters"
        @search="handleSearch"
        @open="searchModal.show()"
        @remove-filter="removeFilter"
        @clear-all="clearFilters"
      />
    </div>

    <!-- table -->
    <va-data-table
      :items="tracks"
      :columns="columns"
      v-model:sort-by="query.sort_by"
      v-model:sorting-order="query.sort_order"
      disable-client-side-sorting
      :loading="data_loading"
    >
      <template #cell(name)="{ rowData }">
        <router-link :to="`/tracks/${rowData.id}`" class="va-link">{{ rowData.name }}</router-link>
      </template>

      <template #cell(file_type)="{ value }">
        <va-chip v-if="value" size="small" :color="trackService._getTrackColor(value)">{{
          value
        }}</va-chip>
      </template>

      <template #cell(genome)="{ rowData }">
        <GenomeDisplay
          :genome-type="rowData.dataset_file?.dataset?.genomic_details?.genome_type"
          :genome-value="rowData.dataset_file?.dataset?.genomic_details?.genome_value"
        />
      </template>

      <template #cell(dataset)="{ rowData }">
        <router-link
          v-if="auth.canOperate"
          :to="`/datasets/${rowData.dataset_file?.dataset?.id}`"
          class="va-link"
        >
          {{ rowData.dataset_file?.dataset?.name }}
        </router-link>
        <span v-else>{{ rowData.dataset_file?.dataset?.name }}</span>
      </template>

      <template #cell(created_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(stage)="{ rowData }">
        <span v-if="rowData.dataset_file?.dataset?.is_staged" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>

      <template #cell(updated_at)="{ value }">
        <span>{{ datetime.fromNow(value) }}</span>
      </template>

    </va-data-table>

    <!-- pagination -->
    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="query.page"
      v-model:page_size="query.page_size"
      :total_results="total_results"
      :curr_items="tracks.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />

    <TrackSearchModal ref="searchModal" @search="handleSearch" />
  </div>
</template>

<script setup>
import useQueryPersistence from '@/composables/useQueryPersistence';
import useSearchKeyShortcut from '@/composables/useSearchKeyShortcut';
import GenomeDisplay from '@/components/genome/GenomeDisplay.vue';
import * as datetime from '@/services/datetime';
import toast from '@/services/toast';
import trackService from '@/services/track';
import { useTracksStore } from '@/stores/tracks';
import { useAuthStore } from '@/stores/auth';

useSearchKeyShortcut();

const store = useTracksStore();
const auth = useAuthStore();

const PAGE_SIZE_OPTIONS = [25, 50, 100];

// Reactive data
const tracks = ref([]);
const data_loading = ref(false);
const total_results = ref(0);
const searchModal = ref(null);
// Query parameters
const query = ref({
  page: 1,
  page_size: 25,
  sort_by: 'created_at',
  sort_order: 'desc',
});

// Filters
const filters = ref({
  name: null,
  project_id: null,
  project: null,
  dataset_id: null,
  dataset: null,
  file_type: null,
  genome_type: null,
  genome_value: null,
});

// Default values function for query persistence
const defaultParams = () => ({
  page: 1,
  page_size: 25,
  sort_by: 'created_at',
  sort_order: 'desc',
});

const defaultFilters = () => ({
  name: null,
  project_id: null,
  project: null,
  dataset_id: null,
  dataset: null,
  file_type: null,
  genome_type: null,
  genome_value: null,
});

// Active filters computed
const activeFilters = computed(() => {
  const active = [];
  const { name, project, dataset, file_type, genome_type, genome_value } = filters.value;
  if (name) active.push({ key: 'name', value: name });
  if (project) active.push({ key: 'project', value: project });
  if (dataset) active.push({ key: 'dataset', value: dataset });
  if (file_type) active.push({ key: 'file_type', value: file_type });
  if (genome_type) active.push({ key: 'genome_type', value: genome_type });
  if (genome_value) active.push({ key: 'genome_value', value: genome_value });
  return active;
});

// Offset for pagination
const offset = computed(() => (query.value.page - 1) * query.value.page_size);

// Query persistence
useQueryPersistence({
  refObject: query,
  defaultValueFn: defaultParams,
  key: 'q',
  history_push: true,
});

const columns = [
  {
    key: 'name',
    sortable: true,
    width: '22%',
    thAlign: 'left',
    tdAlign: 'left',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'file_type',
    label: 'File Type',
    sortable: true,
    width: '10%',
    thAlign: 'center',
    tdAlign: 'center',
  },
  {
    key: 'genome',
    label: 'Genome',
    width: '15%',
    thAlign: 'center',
    tdAlign: 'center',
  },
  {
    key: 'dataset',
    label: 'Dataset',
    thAlign: 'center',
    tdAlign: 'center',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'stage',
    label: 'staged',
    width: '7%',
    thAlign: 'center',
    tdAlign: 'center',
  },
  {
    key: 'created_at',
    label: 'Created',
    sortable: true,
    width: '8%',
    thAlign: 'center',
    tdAlign: 'center',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'updated_at',
    label: 'Updated',
    sortable: true,
    width: '7%',
    thAlign: 'center',
    tdAlign: 'center',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
];

async function fetch_items() {
  data_loading.value = true;

  try {
    const { name, project_id, dataset_id, file_type, genome_type, genome_value } = filters.value;

    // If dataset_id is set, omit project_id (dataset is more specific)
    const apiParams = {
      ...(name ? { name } : {}),
      ...(dataset_id ? { dataset_id } : (project_id ? { project_id } : {})),
      ...(file_type ? { file_type } : {}),
      ...(genome_type ? { genome_type } : {}),
      ...(genome_value ? { genome_value } : {}),
      ...(query.value.inclusive_query ? { name: query.value.inclusive_query } : {}),
      limit: query.value.page_size,
      offset: offset.value,
      sort_by: query.value.sort_by,
      sort_order: query.value.sort_order,
    };

    const response = await store.fetchTracks(apiParams);
    tracks.value = response.tracks;
    total_results.value = response.metadata.count;
  } catch (error) {
    console.error('Error fetching tracks:', error);
    toast.error('Failed to fetch tracks');
  } finally {
    data_loading.value = false;
  }
}

function handleMainFilter(value) {
  query.value.inclusive_query = value;
  query.value.page = 1; // Reset to first page when searching
}

function handleSearch(searchFilters) {
  filters.value = { ...defaultFilters(), ...searchFilters };
  query.value.page = 1;
}

function removeFilter(key) {
  if (key === 'project') {
    filters.value.project = null;
    filters.value.project_id = null;
  } else if (key === 'dataset') {
    filters.value.dataset = null;
    filters.value.dataset_id = null;
  } else {
    filters.value[key] = null;
  }
  query.value.page = 1;
}

function clearFilters() {
  filters.value = defaultFilters();
  query.value.page = 1;
}

// Watch for changes in query and filters
watch(
  [query, filters],
  () => {
    fetch_items();
  },
  { deep: true }
);

// Initial load
onMounted(() => {
  fetch_items();
});

</script>
