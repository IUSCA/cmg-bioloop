<template>
  <div>
    <h1 class="va-h1">Import History</h1>
    <ImportLogList />
  </div>
</template>

<script setup>
import { useNavStore } from "@/stores/nav";
import ImportLogList from "@/components/dataset/import/ImportLogList.vue";

const nav = useNavStore();
nav.setNavItems([
  {
    label: "Import History",
    to: "/datasets/import-history",
  },
]);
</script>

<route lang="yaml">
meta:
  title: Import History
</route>


