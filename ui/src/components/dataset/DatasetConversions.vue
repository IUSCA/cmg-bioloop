<template>
  <VaInnerLoading :loading="loading">
    <va-data-table
      :items="rows"
      :columns="columns"
      v-model:sort-by="sort_by"
      v-model:sorting-order="sort_order"
      disable-client-side-sorting
    >
      <template #cell(id)="{ rowData }">
        <router-link :to="`/conversions/${rowData.id}`" class="va-link">
          #{{ rowData.id }}
        </router-link>
      </template>

      <template #cell(definition)="{ rowData }">
        <span>{{ rowData.definition?.name }}</span>
      </template>

      <template #cell(program)="{ rowData }">
        <span>{{ rowData.definition?.program?.name }}</span>
      </template>

      <template #cell(initiated_at)="{ value }">
        <span>{{ datetime.date(value) }}</span>
      </template>

      <template #cell(initiator)="{ rowData }">
        <span>{{ rowData.initiator?.username }}</span>
      </template>

      <template #cell(status)="{ rowData }">
        <span class="flex justify-center">
          <va-popover v-if="isLegacy(rowData)" message="Complete">
            <i-mdi-check-circle-outline class="text-green-700" />
          </va-popover>
          <WorkflowStatusIcon
            v-else
            :status="workflowStatusMap[rowData.workflow_id] ?? undefined"
          />
        </span>
      </template>
    </va-data-table>

    <Pagination
      class="mt-4 px-1 lg:px-3"
      v-model:page="page"
      v-model:page_size="page_size"
      :total_results="total_results"
      :curr_items="rows.length"
      :page_size_options="PAGE_SIZE_OPTIONS"
    />
  </VaInnerLoading>
</template>

<script setup>
import conversionApiService from "@/services/conversion/api";
import * as datetime from "@/services/datetime";
import toast from "@/services/toast";
import workflowService from "@/services/workflow";

const props = defineProps({
  datasetId: {
    type: [String, Number],
    required: true,
  },
});

const PAGE_SIZE_OPTIONS = [5, 10, 20, 50];

const conversions = ref([]);
const loading = ref(false);
const total_results = ref(0);
const workflowStatusMap = ref({});

const page = ref(1);
const page_size = ref(10);
const sort_by = ref("initiated_at");
const sort_order = ref("desc");

const offset = computed(() => (page.value - 1) * page_size.value);

const columns = [
  {
    key: "id",
    label: "Conversion",
    sortable: false,
    width: "90px",
  },
  {
    key: "definition",
    label: "Definition",
    sortable: false,
  },
  {
    key: "program",
    label: "Program",
    sortable: false,
  },
  {
    key: "initiated_at",
    label: "Initiated On",
    sortable: true,
    width: "110px",
  },
  {
    key: "initiator",
    label: "Initiator",
    sortable: false,
  },
  {
    key: "status",
    label: "Status",
    sortable: false,
    thAlign: "center",
    tdAlign: "center",
    width: "80px",
  },
];

const rows = computed(() => conversions.value);

function isLegacy(conversion) {
  return !!conversion.metadata?.origin;
}

async function fetchWorkflowStatuses(fetchedConversions) {
  const nonLegacyWithWorkflow = fetchedConversions.filter(
    (c) => !isLegacy(c) && c.workflow_id,
  );
  if (nonLegacyWithWorkflow.length === 0) return;

  const workflowIds = nonLegacyWithWorkflow.map((c) => c.workflow_id);
  try {
    const res = await workflowService.getAll({ workflow_id: workflowIds });
    const statusMap = (res.data?.results || []).reduce((acc, wf) => {
      acc[wf.id] = wf.status;
      return acc;
    }, {});
    workflowStatusMap.value = statusMap;
  } catch (err) {
    console.error("Unable to fetch workflow statuses for conversions", err);
  }
}

function fetchConversions() {
  if (!props.datasetId) return;
  loading.value = true;
  conversionApiService
    .getAll({
      dataset_id: props.datasetId,
      limit: page_size.value,
      offset: offset.value,
      sort_by: sort_by.value,
      sort_order: sort_order.value,
    })
    .then((res) => {
      conversions.value = res.data?.conversions || [];
      total_results.value = res.data?.metadata?.count || 0;
      fetchWorkflowStatuses(conversions.value);
    })
    .catch((err) => {
      console.error(err);
      toast.error("Unable to fetch associated conversions");
    })
    .finally(() => {
      loading.value = false;
    });
}

watch([sort_by, sort_order, page_size], () => {
  if (page.value !== 1) {
    page.value = 1;
  } else {
    fetchConversions();
  }
});

watch(page, fetchConversions);

watch(
  () => props.datasetId,
  () => {
    fetchConversions();
  },
  { immediate: true },
);
</script>
