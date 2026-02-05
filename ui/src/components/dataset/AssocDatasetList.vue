<template>
  <VaInnerLoading :loading="data_loading">
    <va-data-table
      :items="datasets"
      :columns="columns"
      v-model:sort-by="sort_by"
      v-model:sorting-order="sort_order"
      disable-client-side-sorting
    >
      <template #cell(name)="{ rowData }">
        <router-link :to="`/datasets/${rowData.id}`" class="va-link">
          {{ rowData.name }}
        </router-link>
      </template>

      <template #cell(derivation_method)="{ rowData }">
        <va-chip 
          v-if="derivation_method_map.get(rowData.id)"
          :color="derivation_method_map.get(rowData.id) === 'conversion' ? 'info' : 'secondary'"
          size="small"
        >
          {{ derivation_method_map.get(rowData.id) === 'conversion' ? 'Conversion' : 'Manual Assignment' }}
        </va-chip>
      </template>

      <template #cell(du_size)="{ source }">
        <span>{{ source != null ? formatBytes(source) : "" }}</span>
      </template>

      <template #cell(created_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(updated_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(archive_path)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>

      <template #cell(is_staged)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>
      <template #cell(is_deleted)="{ source }">
        <span v-if="source" class="flex justify-center">
          <i-mdi-check-circle-outline class="text-green-700" />
        </span>
      </template>
    </va-data-table>
    <!-- pagination -->
    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="page"
      v-model:page_size="page_size"
      :total_results="total_results"
      :curr_items="datasets.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />
  </VaInnerLoading>
</template>

<script setup>
import DatasetService from "@/services/dataset";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import { formatBytes } from "@/services/utils";

const props = defineProps({
  datasets_meta: {
    type: Array,
    default: () => [],
  },
  show_derivation_method: {
    type: Boolean,
    default: false,
  },
  // 'derived' = show child datasets (use derived_id from dataset_hierarchy)
  // 'source' = show parent datasets (use source_id from dataset_hierarchy)
  relationship_type: {
    type: String,
    default: 'derived',
    validator: (value) => ['derived', 'source'].includes(value),
  },
});

const datasets = ref([]);
const data_loading = ref(false);

// Extract dataset IDs based on relationship type
// - For derived datasets: extract derived_id (child datasets)
// - For source datasets: extract source_id (parent datasets)
const dataset_ids = computed(() => 
  props.datasets_meta.map((meta) => {
    if (props.relationship_type === 'source') {
      // For source datasets, we want to display the parent (source_id)
      return meta.source_id;
    }
    // For derived datasets, we want to display the child (derived_id)
    return meta.derived_id;
  })
);

const derivation_method_map = computed(() => {
  const map = new Map();
  props.datasets_meta.forEach((meta) => {
    const id = props.relationship_type === 'source' ? meta.source_id : meta.derived_id;
    map.set(id, meta.derivation_method);
  });
  return map;
});

// pagination
const page = ref(1);
const page_size = ref(10);
const total_results = computed(() => dataset_ids.value.length);
const PAGE_SIZE_OPTIONS = [5, 10, 20, 50, 100];
const offset = computed(() => (page.value - 1) * page_size.value);

// sorting
const sort_by = ref("updated_at");
const sort_order = ref("desc");

const baseColumns = [
  // { key: "id", sortable: true,  },
  { key: "name", sortable: true },
  { key: "type", sortable: true },
  {
    key: "du_size",
    label: "size",
    sortable: true,
    width: 80,
  },
  { key: "created_at", label: "created on", sortable: true, width: "100px" },
  {
    key: "updated_at",
    label: "last updated",
    sortable: true,
    width: "100px",
  },
  {
    key: "archive_path",
    label: "archived",
    thAlign: "center",
    tdAlign: "center",
    width: "80px",
  },
  {
    key: "is_staged",
    label: "staged",
    thAlign: "center",
    tdAlign: "center",
    width: "80px",
  },
  {
    key: "is_deleted",
    label: "deleted",
    thAlign: "center",
    tdAlign: "center",
    width: "80px",
  },
];

const columns = computed(() => {
  const cols = [...baseColumns];
  if (props.show_derivation_method) {
    // Insert derivation method column after "type"
    cols.splice(2, 0, {
      key: "derivation_method",
      label: "Derivation Method",
      sortable: false,
      thAlign: "left",
      tdAlign: "left",
      width: "150px",
    });
  }
  return cols;
});

watch([() => props.datasets_meta], fetchDatasets, { immediate: true });
watch(page, fetchDatasets);
watch([page_size, sort_by, sort_order], () => {
  if (page.value !== 1) {
    page.value = 1;
  } else {
    fetchDatasets();
  }
});

function fetchDatasets() {
  if (!dataset_ids.value.length) {
    return;
  }
  data_loading.value = true;
  const ids_to_fetch = dataset_ids.value.slice(
    offset.value,
    offset.value + page_size.value,
  );
  DatasetService.getAll({
    id: ids_to_fetch,
    sort_by: sort_by.value,
    sort_order: sort_order.value,
  })
    .then((res) => {
      datasets.value = res.data.datasets;
    })
    .catch((err) => {
      console.error("Unable to fetch datasets", err);
      toast.error("Unable to fetch datasets");
    })
    .finally(() => {
      data_loading.value = false;
    });
}
</script>
