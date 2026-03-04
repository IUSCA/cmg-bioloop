<template>
  <div>
    <AutoComplete
      v-model:search-text="searchTerm"
      :async="true"
      :paginated="true"
      :paginated-total-results-count="totalResultsCount"
      :data="tracks"
      :display-by="'name'"
      @clear="onClear"
      @load-more="loadNextPage"
      placeholder="Search Tracks by name"
      :loading="loading"
      @select="onSelect"
      @open="onOpen"
      @close="onClose"
      :disabled="props.disabled"
      :label="props.label"
    />

    <!-- Selected tracks list (if multiple selection is enabled) -->
    <div v-if="props.multiple && selectedTracks.length > 0" class="mt-3">
      <div class="flex flex-row justify-between px-1 mb-2">
        <span class="text-sm font-medium">Selected Tracks</span>
        <span class="text-sm text-gray-500"
          >{{ selectedTracks.length }} selected</span
        >
      </div>
      <div
        class="max-h-32 overflow-y-auto border border-gray-200 dark:border-gray-700 rounded p-2 space-y-1"
      >
        <div
          v-for="track in selectedTracks"
          :key="track.id"
          class="flex items-center justify-between p-2 bg-gray-50 dark:bg-gray-800 rounded text-sm"
        >
          <span class="truncate flex-1">{{ track.name }}</span>
          <button
            @click="removeTrack(track)"
            class="ml-2 text-red-500 hover:text-red-700 flex-shrink-0"
          >
            <i-mdi-close class="text-lg" />
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import toast from "@/services/toast";
import { useTracksStore } from "@/stores/tracks";
import _ from "lodash";
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

const PAGE_SIZE = 10;

const props = defineProps({
  selected: {
    type: [String, Object, Array],
  },
  searchTerm: {
    type: String,
    default: "",
  },
  disabled: {
    type: Boolean,
    default: false,
  },
  label: {
    type: String,
  },
  error: {
    type: String,
  },
  errorMessages: {
    type: Array,
    default: () => [],
  },
  genomeType: {
    type: String,
    default: null,
  },
  genomeValue: {
    type: String,
    default: null,
  },
  multiple: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits([
  "clear",
  "open",
  "close",
  "update:selected",
  "update:searchTerm",
  "select",
]);

const tracksStore = useTracksStore();

const loading = ref(false);
const tracks = ref([]);
const totalResultsCount = ref(0);
const page = ref(1);
const selectedTracks = ref(
  props.multiple ? (Array.isArray(props.selected) ? props.selected : []) : [],
);

// Watch for changes in props.selected to sync with selectedTracks
watch(
  () => props.selected,
  (newSelected) => {
    if (props.multiple && Array.isArray(newSelected)) {
      selectedTracks.value = [...newSelected];
    }
  },
  { immediate: true },
);
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
  if (props.multiple) {
    // For multiple selection, emit select event and clear search term
    emit("select", item);
    emit("update:searchTerm", "");
  } else {
    // For single selection, update selected and search term
    emit("update:searchTerm", item.name);
    emit("update:selected", item);
  }
};

const removeTrack = (track) => {
  const index = selectedTracks.value.findIndex((t) => t.id === track.id);
  if (index > -1) {
    selectedTracks.value.splice(index, 1);
    emit("update:selected", [...selectedTracks.value]);
  }
};

const loadNextPage = () => {
  page.value += 1; // increase page value for offset recalculation
  return searchTracks({ appendToCurrentResults: true });
};

const batchingQuery = computed(() => {
  return {
    offset: skip.value,
    limit: PAGE_SIZE,
  };
});

const fetchQuery = computed(() => {
  return {
    ...(searchTerm.value && { name: searchTerm.value }),
    // Add genome filtering if provided
    ...(props.genomeType && { genome_type: props.genomeType }),
    ...(props.genomeValue && { genome_value: props.genomeValue }),
    ...batchingQuery.value,
  };
});

const queryTracks = ({ queryIndex = null, query = null } = {}) => {
  return tracksStore.fetchTracks(query).then((res) => {
    return { data: res, ...(queryIndex && { queryIndex }) };
  });
};

const searchTracks = ({
  searchIndex = null,
  appendToCurrentResults = false,
  logQuery = false,
} = {}) => {
  // Debug: log the query being sent
  console.log("Search query:", fetchQuery.value);

  // Ensure that the same query is not being run a second time (which
  // is possible due to debounced searches). If it is, the search
  // can be resolved immediately.
  if (_.isEqual(latestQuery.value, fetchQuery.value)) {
    resolveSearch(searchIndex);
  } else {
    if (logQuery) {
      latestQuery.value = fetchQuery.value;
    }

    return queryTracks({
      ...(searchIndex && { queryIndex: searchIndex }),
      query: fetchQuery.value,
    })
      .then((res) => {
        tracks.value = appendToCurrentResults
          ? tracks.value.concat(res.data.tracks)
          : res.data.tracks;
        totalResultsCount.value = res.data.metadata.count;

        // Show message if no tracks found
        if (res.data.tracks.length === 0 && !appendToCurrentResults) {
          tracks.value = [];
          // Don't show error for empty results, just log it
          console.log("No tracks found matching the criteria");
        }

        resolveSearch(res.queryIndex);
      })
      .catch((e) => {
        console.error(e);

        // Provide more specific error messages
        if (e.response?.data?.error) {
          toast.error(`Failed to load tracks: ${e.response.data.error}`);
        } else if (e.message) {
          toast.error(`Failed to load tracks: ${e.message}`);
        } else {
          toast.error("Failed to load tracks. Please try again.");
        }

        // Reset tracks on error
        if (!appendToCurrentResults) {
          tracks.value = [];
          totalResultsCount.value = 0;
        }
        resolveSearch(searchIndex);
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
  // reset page value
  page.value = 1;
  // load search results
  searchTracks({
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

watch([searchTerm, () => props.genomeType, () => props.genomeValue], () => {
  searchIndex.value += 1;
  searches.value.push(searchIndex.value);

  loading.value = true;

  debouncedSearch.value = _.debounce(performSearch, 300);
  debouncedSearch.value(searchIndex.value);
});

onMounted(() => {
  loading.value = true;
  searchTracks();
});

onBeforeUnmount(() => {
  if (debouncedSearch.value) {
    debouncedSearch.value.cancel();
  }
});
</script>

<style scoped></style>
