<template>
  <va-modal
    class="delete-track-modal"
    v-model="visible"
    title="Delete Track?"
    no-outside-dismiss
    fixed-layout
    ok-text="Delete"
    size="small"
    @ok="handleOk"
    @cancel="hide"
  >
    <va-inner-loading :loading="loading">
      <div>Are you sure you want to delete the track "{{ props.data.name }}"?</div>
    </va-inner-loading>
  </va-modal>
</template>

<script setup>
import { useTracksStore } from '@/stores/tracks';
import toast from '@/services/toast';

const props = defineProps(['data']);
const emit = defineEmits(['update']);

const store = useTracksStore();

// parent component can invoke these methods through the template ref
defineExpose({
  show,
  hide,
});

const visible = ref(false);
const loading = ref(false);

function hide() {
  loading.value = false;
  visible.value = false;
}

function show() {
  visible.value = true;
}

async function handleOk() {
  loading.value = true;
  try {
    await store.deleteTrack(props.data.id);
    toast.success('Track deleted successfully');
    emit('update');
  } catch (error) {
    console.error('Failed to delete track:', error);
    toast.error('Failed to delete track');
  } finally {
    loading.value = false;
    hide();
  }
}
</script>
