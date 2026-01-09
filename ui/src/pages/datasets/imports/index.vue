<template>
  <va-alert color="warning" icon="warning" v-if="!auth.isFeatureEnabled('import')">
    This feature is currently disabled
  </va-alert>

  <div v-else>
    <div class="flex mb-3 gap-3">
      <!-- search bar -->
      <div class="flex-1">
        <va-input
          v-model="filterInput"
          class="w-full"
          placeholder="Type / to search Dataset Imports"
          outline
          clearable
          input-class="search-input"
        >
          <template #prependInner>
            <Icon icon="material-symbols:search" class="text-xl" />
          </template>
        </va-input>
      </div>

      <!-- create button -->
      <div class="flex-none">
        <va-button
          icon="add"
          class="px-1"
          color="success"
          @click="router.push('/datasets/imports/new')"
        >
          Import Dataset
        </va-button>
      </div>
    </div>

    <!-- table -->
    <va-data-table :items="pastImports" :columns="columns">
      <template #cell(imported_dataset)="{ rowData }">
        <div v-if="!auth.canOperate">
          {{ rowData.imported_dataset.name }}
        </div>
        <router-link v-else :to="`/datasets/${rowData.imported_dataset.id}`" class="va-link">
          {{ rowData.imported_dataset.name }}
        </router-link>
      </template>

      <template #cell(imported_dataset_type)="{ value }">
        <va-chip size="small" outline>
          {{ value }}
        </va-chip>
      </template>

      <template #cell(file_type)="{ value }">
        <va-chip size="small" outline v-if="value">
          {{ value.toUpperCase() }}
        </va-chip>
        <span v-else>-</span>
      </template>

      <template #cell(genome)="{ rowData }">
        <span v-if="rowData.genome_type || rowData.genome_value">
          {{ rowData.genome_type || '' }}
          {{ rowData.genome_value ? `(${rowData.genome_value})` : '' }}
        </span>
        <span v-else>-</span>
      </template>

      <template #cell(source_dataset)="{ rowData }">
        <div v-if="rowData.source_dataset">
          <div v-if="!auth.canOperate">
            {{ rowData.source_dataset.name }}
          </div>
          <router-link v-else :to="`/datasets/${rowData.source_dataset.id}`" class="va-link">
            {{ rowData.source_dataset.name }}
          </router-link>
        </div>
        <span v-else>-</span>
      </template>

      <template #cell(user)="{ rowData }">
        <span>{{ rowData.user.name }} ({{ rowData.user.username }})</span>
      </template>

      <template #cell(initiated_at)="{ value }">
        <span class="text-sm lg:text-base">
          {{ datetime.date(value) }}
        </span>
      </template>

      <template #cell(notes)="{ rowData }">
        <va-popover v-if="rowData.notes" placement="top">
          <template #body>
            <div class="max-w-xs">{{ rowData.notes }}</div>
          </template>
          <va-icon name="mdi-note-text" class="cursor-pointer" />
        </va-popover>
        <span v-else>-</span>
      </template>
    </va-data-table>

    <Pagination
      v-model:page="currentPageIndex"
      v-model:page_size="pageSize"
      :total_results="total_results"
      :curr_items="pastImports.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />
  </div>
</template>

<script setup>
import useSearchKeyShortcut from '@/composables/useSearchKeyShortcut';
import datasetService from '@/services/dataset';
import * as datetime from '@/services/datetime';
import toast from '@/services/toast';
import { useAuthStore } from '@/stores/auth';
import { useNavStore } from '@/stores/nav';
import _ from 'lodash';

const nav = useNavStore();
const router = useRouter();
const auth = useAuthStore();

nav.setNavItems([{ label: 'Dataset Imports' }]);

useSearchKeyShortcut();

const PAGE_SIZE_OPTIONS = [10, 20, 50];

const filterInput = ref('');
const pastImports = ref([]);

const currentPageIndex = ref(1);
const pageSize = ref(10);
const total_results = ref(0);

const offset = computed(() => (currentPageIndex.value - 1) * pageSize.value);

const search_query = computed(() => {
  return filterInput.value?.length > 0 && { dataset_name: filterInput.value };
});

const imports_batching_query = computed(() => {
  return { offset: offset.value, limit: pageSize.value };
});

const filter_query = computed(() => {
  return {
    ...imports_batching_query.value,
    ...(!!search_query.value && { ...search_query.value }),
    username: auth.user?.username,
    forSelf: !auth.canOperate,
  };
});

const columns = [
  {
    key: 'imported_dataset',
    label: 'Imported Dataset',
    width: '15%',
    thAlign: 'center',
    tdAlign: 'center',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'imported_dataset_type',
    label: 'Dataset Type',
    width: '12%',
    thAlign: 'center',
    tdAlign: 'center',
  },
  {
    key: 'file_type',
    label: 'File Type',
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
    key: 'source_dataset',
    label: 'Source Raw Data',
    width: '15%',
    thAlign: 'center',
    tdAlign: 'center',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'user',
    label: 'Imported By',
    width: '15%',
    thAlign: 'center',
    tdAlign: 'center',
    tdStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'initiated_at',
    label: 'Imported On',
    width: '10%',
    thAlign: 'right',
    tdAlign: 'right',
    thStyle: 'white-space: pre-wrap; word-wrap: break-word; word-break: break-word;',
  },
  {
    key: 'notes',
    label: 'Notes',
    width: '8%',
    thAlign: 'center',
    tdAlign: 'center',
  },
];

const getImportLogs = async () => {
  return datasetService
    .getDatasetImportLogs(filter_query.value)
    .then((res) => {
      pastImports.value = res.data.imports.map((e) => {
        let imported_dataset = e.audit_log.dataset;
        return {
          ...e,
          initiated_at: e.audit_log.timestamp,
          user: e.audit_log.user,
          imported_dataset,
          source_dataset:
            imported_dataset.source_datasets.length > 0
              ? imported_dataset.source_datasets[0].source_dataset
              : null,
          imported_dataset_type: imported_dataset.type,
        };
      });
      total_results.value = res.data.metadata.count;
    })
    .catch((err) => {
      toast.error('Could not retrieve past imports');
      console.error('Error fetching import logs:', err);
    });
};

onMounted(() => {
  getImportLogs();
});

watch(filterInput, () => {
  currentPageIndex.value = 1;
});

watch(filter_query, (newQuery, oldQuery) => {
  if (!_.isEqual(newQuery, oldQuery)) {
    getImportLogs();
  }
});
</script>

<route lang="yaml">
meta:
  title: Dataset Imports
</route>
