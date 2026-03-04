<template>
  <VaInnerLoading :loading="loading">
    <va-data-table
      :items="sessions"
      :columns="columns"
      v-model:sort-by="sort_by"
      v-model:sorting-order="sort_order"
      disable-client-side-sorting
    >
      <template #cell(title)="{ rowData }">
        <router-link :to="`/sessions/${rowData.id}`" class="va-link">
          {{ rowData.title || `Session #${rowData.id}` }}
        </router-link>
      </template>

      <template #cell(genome)="{ rowData }">
        <GenomeDisplay
          :genome-type="rowData.genome_type"
          :genome-value="rowData.genome"
        />
      </template>

      <template #cell(owner)="{ rowData }">
        <span>{{ rowData.user?.username }}</span>
      </template>

      <template #cell(created_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>
    </va-data-table>

    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="page"
      v-model:page_size="page_size"
      :total_results="total_results"
      :curr_items="sessions.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />
  </VaInnerLoading>
</template>

<script setup>
import * as datetime from "@/services/datetime";
import sessionService from "@/services/session";
import { useAuthStore } from "@/stores/auth";
import toast from "@/services/toast";
import GenomeDisplay from "@/components/genome/GenomeDisplay.vue";

const props = defineProps({
  datasetId: {
    type: [String, Number],
    required: true,
  },
});

const auth = useAuthStore();

const PAGE_SIZE_OPTIONS = [5, 10, 20, 50];

const sessions = ref([]);
const loading = ref(false);
const total_results = ref(0);

const page = ref(1);
const page_size = ref(10);
const sort_by = ref("created_at");
const sort_order = ref("desc");

const offset = computed(() => (page.value - 1) * page_size.value);

const columns = [
  {
    key: "title",
    label: "Title",
    sortable: true,
  },
  {
    key: "genome",
    label: "Genome",
    sortable: false,
  },
  {
    key: "owner",
    label: "Owner",
    sortable: false,
  },
  {
    key: "created_at",
    label: "Created On",
    sortable: true,
    width: "110px",
  },
];

function fetchSessions() {
  if (!props.datasetId) return;
  loading.value = true;

  const params = {
    dataset_id: props.datasetId,
    limit: page_size.value,
    offset: offset.value,
    sort_by: sort_by.value,
    sort_order: sort_order.value,
  };

  const request = auth.canOperate
    ? sessionService.getAll(params)
    : sessionService.getByUsername(auth.user?.username, params);

  request
    .then((res) => {
      sessions.value = res.data?.sessions || [];
      total_results.value = res.data?.metadata?.count || 0;
    })
    .catch((err) => {
      console.error(err);
      toast.error("Unable to fetch associated sessions");
    })
    .finally(() => {
      loading.value = false;
    });
}

watch([sort_by, sort_order, page_size], () => {
  if (page.value !== 1) {
    page.value = 1;
  } else {
    fetchSessions();
  }
});

watch(page, fetchSessions);

watch(
  () => props.datasetId,
  () => {
    fetchSessions();
  },
  { immediate: true },
);
</script>
