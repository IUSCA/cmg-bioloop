<template>
  <div>
    <!-- search bar and filter -->
    <div class="flex mb-3 gap-3">
      <!-- search bar -->
      <div class="flex-1" v-if="activeFilters.length === 0">
        <va-input
          :model-value="params.inclusive_query"
          class="w-full"
          :placeholder="`Search ${props.label.toLowerCase()}`"
          outline
          clearable
          @update:model-value="handleMainFilter"
        >
          <template #prependInner>
            <Icon icon="material-symbols:search" class="text-xl" />
          </template>
        </va-input>
      </div>

      <!-- Convert button -->
      <VaButtonGroup>
        <va-button
          @click="handleConvertClick"
          preset="primary"
          color="info"
          class="flex-none"
        >
          <i-mdi-orbit-variant class="mr-1" />
          <span> Convert </span>
        </va-button>
      </VaButtonGroup>

      <!-- Filter button -->
      <va-button @click="searchModal.show()" preset="primary" class="flex-none">
        <i-mdi-filter />
        <span> Filters </span>
      </va-button>

      <!-- active filter chips -->
      <ConversionSearchFilters
        v-if="activeFilters.length > 0"
        class="flex-none"
        @search="handleSearch"
        @open="searchModal.show()"
      />
    </div>

    <!-- table -->
    <div class="overflow-x-auto" style="max-width: calc(100vw - 250px)">
      <va-data-table
        items-track-by="id"
        :items="conversions"
        :columns="columns"
        v-model:sort-by="query.sort_by"
        v-model:sorting-order="query.sort_order"
        disable-client-side-sorting
        hoverable
        :loading="data_loading"
      >
        <template #cell(status)="{ rowData }">
          <div class="flex justify-center">
            <div v-if="getConversionStatus(rowData) === 'ACTIVE'">
              <va-popover message="Conversion in progress">
                <half-circle-spinner
                  class="flex-none"
                  :animation-duration="1000"
                  :size="24"
                  :color="colors.warning"
                />
              </va-popover>
            </div>
            <div v-else-if="getConversionStatus(rowData) === 'SUCCESS'">
              <va-popover message="Conversion completed successfully">
                <va-icon name="check_circle" color="success" />
              </va-popover>
            </div>
            <div v-else-if="getConversionStatus(rowData) === 'FAILURE'">
              <va-popover message="Conversion failed">
                <va-icon name="warning" color="warning" />
              </va-popover>
            </div>
          </div>
        </template>

        <template #cell(dataset)="{ rowData }">
          <router-link :to="`/conversions/${rowData.id}`" class="va-link">{{
            rowData.dataset?.name
          }}</router-link>
        </template>

        <template #cell(program_name)="{ rowData }">
          <span>{{ rowData.definition?.program?.name }}</span>
        </template>

        <template #cell(initiated_at)="{ value }">
          <span>{{ datetime.date(value) }}</span>
        </template>

        <template #cell(initiator)="{ rowData }">
          <div class="flex items-center gap-2">
            <!-- <UserAvatar v-bind="rowData.initiator" /> -->
            <span>{{ rowData.initiator?.username }}</span>
          </div>
        </template>
      </va-data-table>
    </div>

    <!-- pagination -->
    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="query.page"
      v-model:page_size="query.page_size"
      :total_results="total_results"
      :curr_items="conversions.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />

    <ConversionSearchModal ref="searchModal" @search="handleSearch" />

    <BulkConversionModal
      ref="conversionModal"
      :dataset-ids="[]"
      :allow-raw-data-selection="true"
    />
  </div>
</template>

<script setup>
import useQueryPersistence from "@/composables/useQueryPersistence";
import conversionApiService from "@/services/conversion/api";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import { useConversionStore } from "@/stores/conversion";
import { storeToRefs } from "pinia";
import { HalfCircleSpinner } from "epic-spinners";
import { useColors } from "vuestic-ui";
import config from "@/config";

useSearchKeyShortcut();

const props = defineProps({
  label: String,
});

const { colors } = useColors();
const store = useConversionStore();
const { filters, query, params, activeFilters } = storeToRefs(store);

const PAGE_SIZE_OPTIONS = [25, 50, 100];

const conversions = ref([]);
const data_loading = ref(false);
const searchModal = ref(null);
const conversionModal = ref(null);
const total_results = ref(0);

// used for OFFSET clause in the SQL used to retrieve the next paginated batch
// of results
const offset = computed(() => (query.value.page - 1) * query.value.page_size);

useQueryPersistence({
  refObject: params,
  defaultValueFn: store.defaultParams,
  key: "cq",
  history_push: true,
});

const ACTIVE_WF_STATUSES = ["PENDING", "STARTED"];

function getConversionStatus(conversion) {
  if (conversion?.metadata?.origin === "legacy") return "SUCCESS";
  const wfStatus = conversion?.workflow_status;
  if (!wfStatus) return null;
  if (ACTIVE_WF_STATUSES.includes(wfStatus)) return "ACTIVE";
  if (wfStatus === "SUCCESS") return "SUCCESS";
  return "FAILURE";
}

const columns = [
  {
    key: "status",
    label: "Status",
    width: "8%",
    tdAlign: "center",
    thAlign: "center",
  },
  {
    key: "dataset",
    label: "dataset",
    sortable: true,
    tdAlign: "left",
    thAlign: "left",
  },
  {
    key: "program_name",
    label: "program",
    sortable: false,
    width: "20%",
    tdAlign: "center",
    thAlign: "center",
  },
  {
    key: "initiated_at",
    label: "initiated on",
    sortable: true,
    width: "15%",
    tdAlign: "center",
    thAlign: "center",
  },
  {
    key: "initiator",
    label: "initiator",
    sortable: false,
    width: "15%",
    tdAlign: "right",
    thAlign: "right",
  },
];

function fetch_items() {
  data_loading.value = true;
  const filters_api = {
    ...filters.value,
    ...(params.value.inclusive_query
      ? { dataset_name: params.value.inclusive_query }
      : null),
  };
  if (filters_api.initiated_at) {
    filters_api.initiated_at_start = filters_api.initiated_at.start;
    filters_api.initiated_at_end = filters_api.initiated_at.end;
    delete filters_api.initiated_at;
  }
  let sort_by = query.value.sort_by;
  let sort_order = query.value.sort_order;
  if (!sort_order) {
    sort_by = null;
  }
  conversionApiService
    .getAll({
      limit: query.value.page_size,
      offset: offset.value,
      sort_by,
      sort_order,
      include_workflow_status: true,
      ...filters_api,
    })
    .then((res) => {
      conversions.value = res.data?.conversions || [];
      total_results.value = res.data?.metadata?.count || 0;
    })
    .catch((err) => {
      console.error(err);
      toast.error("Unable to fetch conversions");
    })
    .finally(() => {
      data_loading.value = false;
    });
}

const tracking = computed(() =>
  conversions.value.filter((c) => getConversionStatus(c) === "ACTIVE"),
);

const poll = useIntervalFn(
  () => {
    fetch_items();
  },
  config.dataset_polling_interval,
  { immediate: false },
);

watch(tracking, () => {
  if (tracking.value.length > 0) {
    poll.resume();
  } else {
    poll.pause();
  }
});

onMounted(() => {
  fetch_items();
});

// when sort by or sort order changes, set current page to 1 and fetch items
// when page size changes, set current page to 1 and fetch items
watch(
  [
    () => query.value.sort_by,
    () => query.value.sort_order,
    () => query.value.page_size,
  ],
  () => {
    if (query.value.page === 1) {
      fetch_items();
    } else {
      // change current page to 1 triggers the watch on currPage and fetches
      // items
      query.value.page = 1;
    }
  },
);

// when page changes, fetch items
watch(() => query.value.page, fetch_items);

// inclusive_query is changed from multiple locations, do not watch it directly
// instead rely on VaInput's update:model-value event to fetch items
const handleMainFilter = useDebounceFn((value) => {
  params.value.inclusive_query = value;
  if (query.value.page === 1) {
    fetch_items();
  } else {
    // change current page to 1 triggers the watch on currPage and fetches items
    query.value.page = 1;
  }
}, 300);

function handleSearch() {
  // clear the search input when search is emitted either from filter chips or
  // from search modal
  params.value.inclusive_query = null;
  if (query.value.page === 1) {
    fetch_items();
  } else {
    // change current page to 1 triggers the watch on currPage and fetches items
    query.value.page = 1;
  }
}

function handleConvertClick() {
  conversionModal.value.show();
}
</script>
