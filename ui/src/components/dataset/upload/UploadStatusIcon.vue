<template>
  <div
    v-if="props.submissionStatus === constants.UPLOAD_STATES.UPLOADING"
    class="flex items-center space-x-2"
  >
    <Icon
      v-if="props.showIcon"
      icon="mdi:arrow-up-bold"
      class="animate-bounce va-text-primary text-2xl"
    />
    <va-chip size="small">Uploading</va-chip>
  </div>

  <div
    v-else-if="props.submissionStatus === constants.UPLOAD_STATES.PROCESSING"
    class="flex items-center space-x-2"
  >
    <va-icon
      v-if="props.showIcon"
      class="text-2xl"
      name="loop"
      spin="clockwise"
      color="primary"
    />
    <va-chip size="small">Processing</va-chip>
  </div>

  <div
    v-else-if="
      props.submissionStatus === constants.UPLOAD_STATES.COMPUTING_CHECKSUMS
    "
    class="flex items-center space-x-2"
  >
    <i-mdi-progress-helper
      v-if="props.showIcon"
      style="color: var(--va-primary)"
      class="animate-spin text-2xl"
    />
    <va-chip size="small">Computing Checksums</va-chip>
  </div>

  <div
    v-else-if="
      props.submissionStatus ===
      constants.UPLOAD_STATES.CHECKSUM_COMPUTATION_FAILED
    "
    class="flex items-center space-x-2"
  >
    <i-mdi-alert-circle-outline
      class="text-2xl"
      v-if="props.showIcon"
      style="color: var(--va-warning)"
    />
    <va-chip size="small" color="warning">Checksum Computation Failed</va-chip>
  </div>

  <div
    v-else-if="props.submissionStatus === constants.UPLOAD_STATES.UPLOAD_FAILED"
    class="flex items-center space-x-2"
  >
    <i-mdi-alert-circle-outline
      class="text-2xl"
      v-if="props.showIcon"
      style="color: var(--va-danger)"
    />
    <va-chip size="small" color="danger">Upload Failed</va-chip>
  </div>

  <div
    v-else-if="props.submissionStatus === constants.UPLOAD_STATES.UPLOADED"
    class="flex items-center space-x-2"
  >
    <i-mdi-check-circle
      class="text-2xl"
      v-if="props.showIcon"
      style="color: var(--va-success)"
    />
    <va-chip size="small" color="success">Uploaded</va-chip>
  </div>
</template>

<script setup>
import constants from "@/constants";

const props = defineProps({
  submissionStatus: {
    type: String,
    required: true,
  },
  showIcon: {
    type: Boolean,
    default: true,
  },
});
</script>

<style scoped>
.animate-spin {
  animation: spin 2s linear infinite;
}
</style>
