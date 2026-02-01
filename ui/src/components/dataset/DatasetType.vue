<template>
  <div class="flex items-center">
    <Icon
      :icon="config.dataset.types[props.type]?.icon"
      class="text-2xl flex-none mr-2 va-text-secondary"
      v-if="props.showIcon"
    />
    <va-chip v-if="props.showType" size="small" outline>
      {{ formatDatasetType(props.type) }}
    </va-chip>
  </div>
</template>

<script setup>
import config from "@/config";
import { formatDatasetType } from "@/services/sessionUtils";

const props = defineProps({
  type: {
    type: String,
    required: true,
  },
  showIcon: {
    type: Boolean,
    default: false,
  },
  showType: {
    type: Boolean,
    default: true,
  },
});
</script>
