<template>
  <AutoComplete
    v-model:search-text="searchTerm"
    :async="true"
    :paginated="true"
    :paginated-total-results-count="totalResultsCount"
    :data="datasets"
    :display-by="'name'"
    @clear="onClear"
    @load-more="loadNextPage"
    placeholder="Search Datasets by name"
    :loading="loading"
    @select="onSelect"
    @open="onOpen"
    @close="onClose"
    :disabled="props.disabled"
    :label="props.label"
    :messages="props.messages"
    :data-test-id="props.dataTestId"
  />
</template>

<script setup>
import datasetService from "@/services/dataset";
import projectService from "@/services/projects";
import toast from "@/services/toast";
import _ from "lodash";

const PAGE_SIZE = 10;

const props = defineProps({
  selected: {
    type: [String, Object],
  },
  searchTerm: {
    type: String,
    default: "",
  },
  datasetType: {
    type: String,
  },
  projectId: {
    type: [String, Number],
    default: null,
  },
  disabled: {
    type: Boolean,
    default: false,
  },
  label: {
    type: String,
  },
  messages: {
    type: Array,
    default: () => [],
  },
  dataTestId: {
    type: String,
    default: "dataset-autocomplete",
  },
});

const emit = defineEmits([
  "clear",
  "open",
  "close",
  "update:selected",
  "update:searchTerm",
]);

const loading = ref(false);
const datasets = ref([]);
const totalResultsCount = ref(0);
const page = ref(1);
const skip = computed(() => {
  return PAGE_SIZE * (page.value - 1);
});
const searchTerm = computed({
  get: () => {
    return props.searchTerm;
  },
  set: (val) => {
    emit("update:searchTerm", val);
  },
});

const debouncedSearch = ref(null);
const searchIndex = ref(0);
const searches = ref([]);
const latestQuery = ref(null);

const onSelect = (item) => {
  emit("update:searchTerm", item.name);
  emit("update:selected", item);
};

const loadNextPage = () => {
  page.value += 1;
  return searchDatasets({ appendToCurrentResults: true });
};

const batchingQuery = computed(() => {
  return {
    offset: skip.value,
    limit: PAGE_SIZE,
  };
});

const fetchQuery = computed(() => {
  const base = {
    ...(searchTerm.value && { name: searchTerm.value }),
    ...batchingQuery.value,
  };
  if (!props.projectId && props.datasetType) {
    base.type = props.datasetType;
  }
  return base;
});

const queryDatasets = ({ queryIndex = null, query = null } = {}) => {
  let request;

  if (props.projectId) {
    // Fetch datasets scoped to the specified project (uses role-aware endpoint via projectService)
    request = projectService
      .getDatasets({
        id: props.projectId,
        params: {
          ...query,
          ...(props.datasetType && { type: props.datasetType }),
        },
      })
      .then((res) => ({
        data: {
          datasets: res.data.datasets,
          metadata: res.data.metadata,
        },
        ...(queryIndex && { queryIndex }),
      }));
  } else {
    request = datasetService.getAll(query).then((res) => {
      return { data: res.data, ...(queryIndex && { queryIndex }) };
    });
  }

  return request;
};

const searchDatasets = ({
  searchIndex = null,
  appendToCurrentResults = false,
  logQuery = false,
} = {}) => {
  if (
    _.isEqual(latestQuery.value, {
      query: fetchQuery.value,
      projectId: props.projectId,
    })
  ) {
    resolveSearch(searchIndex);
  } else {
    if (logQuery) {
      latestQuery.value = {
        query: fetchQuery.value,
        projectId: props.projectId,
      };
    }

    return queryDatasets({
      ...(searchIndex && { queryIndex: searchIndex }),
      query: fetchQuery.value,
    })
      .then((res) => {
        datasets.value = appendToCurrentResults
          ? datasets.value.concat(res.data.datasets)
          : res.data.datasets || [];
        totalResultsCount.value = res.data?.metadata?.count || 0;
        resolveSearch(res.queryIndex);
      })
      .catch((e) => {
        console.error(e);
        toast.error("Failed to load datasets");
      });
  }
};

const resolveSearch = (searchIndex) => {
  searches.value.splice(searches.value.indexOf(searchIndex), 1);
  if (searches.value.length === 0) {
    loading.value = false;
  }
};

const performSearch = (searchIndex) => {
  page.value = 1;
  searchDatasets({
    searchIndex,
    appendToCurrentResults: false,
    logQuery: true,
  });
};

const onOpen = () => {
  emit("open");
};

const onClose = () => {
  emit("close");
};

const onClear = () => {
  emit("clear");
};

watch([searchTerm, () => props.projectId, () => props.datasetType], () => {
  searchIndex.value += 1;
  searches.value.push(searchIndex.value);

  loading.value = true;

  debouncedSearch.value = _.debounce(performSearch, 300);
  debouncedSearch.value(searchIndex.value);
});

onMounted(() => {
  loading.value = true;
  searchDatasets();
});

onBeforeUnmount(() => {
  if (debouncedSearch.value) {
    debouncedSearch.value.cancel();
  }
});
</script>

<style scoped></style>
