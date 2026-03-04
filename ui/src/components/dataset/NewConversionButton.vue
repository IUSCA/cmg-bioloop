<template>
  <div>
    <va-button
      :disabled="disabled"
      class="w-full"
      color="primary"
      border-color="primary"
      preset="secondary"
      @click="visible = !visible"
    >
      <i-mdi-orbit-variant class="pr-2 text-2xl" /> New Conversion
    </va-button>

    <va-modal
      :model-value="visible"
      title="New Conversion"
      @ok="convert_dataset"
      @cancel="close"
      ok-text="Convert"
      fixed-layout
    >
      <div class="min-h-[calc(100vh-15rem)]">
        <ConversionForm
          v-model:definition="definition"
          v-model:argValues="argValues"
          v-model:execution-metadata="execution_metadata"
        />
      </div>
    </va-modal>
  </div>
</template>

<script setup>
import conversionApiService from "@/services/conversion/api";
import toast from "@/services/toast";
const props = defineProps({
  dataset: Object,
});

const emit = defineEmits(["update"]);

const visible = ref(false);
const loading = ref(false);
const definition = ref();
const argValues = ref([]);
const execution_metadata = ref({});

function convert_dataset() {
  loading.value = true;
  conversionApiService
    .create({
      definition_id: definition.value.id,
      dataset_id: props.dataset.id,
      argument_values: argValues.value.argument_values,
      user_argument_values: argValues.value.user_argument_values,
      execution_platform: execution_metadata.value.platform,
      execution_metadata: execution_metadata.value.metadata,
    })
    .then(() => {
      toast.success("Conversion initiated successfully");
      emit("update");
    })
    .catch((err) => {
      console.error(err);
      toast.error("Failed to convert dataset");
      if (err.response.data) {
        toast.error(err.response.data.message);
      }
    })
    .finally(() => {
      loading.value = false;
      visible.value = false;
    });
}

function close() {
  visible.value = false;
  definition.value = null;
  argValues.value = [];
  execution_metadata.value = {};
}

const disabled = computed(() => {
  return !props.dataset.is_staged;
});
</script>
