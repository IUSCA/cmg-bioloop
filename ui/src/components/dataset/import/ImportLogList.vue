<template>
  <div>
    <va-card>
      <va-card-content>
        <div v-if="loading" class="flex justify-center p-4">
          <va-progress-circle indeterminate />
        </div>

        <div v-else>
          <va-data-table
            :items="importLogs"
            :columns="columns"
            :loading="loading"
          >
            <template #cell(created_at)="{ rowData }">
              {{ formatDate(rowData.created_at) }}
            </template>

            <template #cell(user)="{ rowData }">
              {{ rowData.audit_log?.user?.username || "N/A" }}
            </template>

            <template #cell(dataset)="{ rowData }">
              <router-link
                v-if="rowData.audit_log?.dataset"
                :to="`/datasets/${rowData.audit_log.dataset.id}`"
              >
                {{ rowData.audit_log.dataset.name }}
              </router-link>
              <span v-else>N/A</span>
            </template>

            <template #cell(file_type)="{ rowData }">
              {{ rowData.file_type || "N/A" }}
            </template>

            <template #cell(genome)="{ rowData }">
              <span v-if="rowData.genome_type || rowData.genome_value">
                {{ rowData.genome_type || "" }}
                {{ rowData.genome_value ? `(${rowData.genome_value})` : "" }}
              </span>
              <span v-else>N/A</span>
            </template>

            <template #cell(source_run)="{ rowData }">
              {{ rowData.source_run || "N/A" }}
            </template>

            <template #cell(notes)="{ rowData }">
              <va-popover
                v-if="rowData.notes"
                :message="rowData.notes"
                placement="top"
              >
                <va-icon name="mdi-note-text" />
              </va-popover>
              <span v-else>-</span>
            </template>
          </va-data-table>

          <div class="flex justify-center mt-4" v-if="totalCount > limit">
            <va-pagination
              v-model="currentPage"
              :pages="totalPages"
              @update:modelValue="onPageChange"
            />
          </div>
        </div>
      </va-card-content>
    </va-card>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from "vue";
import axios from "axios";
import config from "@/config";
import toast from "@/services/toast";
import * as datetime from "@/services/datetime";

const importLogs = ref([]);
const loading = ref(false);
const currentPage = ref(1);
const limit = ref(50);
const totalCount = ref(0);

const columns = [
  { key: "created_at", label: "Date", sortable: true },
  { key: "user", label: "User", sortable: false },
  { key: "dataset", label: "Dataset", sortable: false },
  { key: "file_type", label: "File Type", sortable: false },
  { key: "genome", label: "Genome", sortable: false },
  { key: "source_run", label: "Source Run", sortable: false },
  { key: "notes", label: "Notes", sortable: false },
];

const totalPages = computed(() => Math.ceil(totalCount.value / limit.value));

const formatDate = (dateString) => {
  return datetime.date(dateString);
};

const fetchImportLogs = async () => {
  loading.value = true;
  try {
    const offset = (currentPage.value - 1) * limit.value;
    const response = await axios.get(
      `${config.api}/datasets/imports/history`,
      {
        params: {
          limit: limit.value,
          offset,
          sort_by: "created_at",
          sort_order: "desc",
        },
      }
    );

    importLogs.value = response.data.import_logs || [];
    totalCount.value = response.data.metadata.count || 0;
  } catch (error) {
    console.error("Failed to fetch import logs:", error);
    toast.error("Failed to load import history");
  } finally {
    loading.value = false;
  }
};

const onPageChange = () => {
  fetchImportLogs();
};

onMounted(() => {
  fetchImportLogs();
});
</script>


