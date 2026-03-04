<template>
  <va-modal
    class="delete-session-modal"
    v-model="visible"
    title="Delete Session?"
    no-outside-dismiss
    fixed-layout
    ok-text="Delete"
    size="small"
    @ok="handleOk"
    @cancel="hide"
  >
    <va-inner-loading :loading="loading">
      <div>
        Are you sure you want to delete the session "{{ props.data.title }}"?
      </div>
    </va-inner-loading>
  </va-modal>
</template>

<script setup>
import { useSessionsStore } from "@/stores/sessions";
import toast from "@/services/toast";

const props = defineProps(["data"]);
const emit = defineEmits(["update"]);

const store = useSessionsStore();

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
    await store.deleteSession(props.data.id);
    toast.success("Session deleted successfully");
    emit("update");
  } catch (error) {
    console.error("Failed to delete session:", error);
    toast.error("Failed to delete session");
  } finally {
    loading.value = false;
    hide();
  }
}
</script>
