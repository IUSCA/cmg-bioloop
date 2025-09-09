<template>
  <ConversionView :conversion-id="props.id" />
</template>

<script setup>
import ConversionApiService from "@/services/conversion/api";
import { useNavStore } from "@/stores/nav";
import { useUIStore } from "@/stores/ui";

const nav = useNavStore();
const ui = useUIStore();

const props = defineProps({ id: String });
console.log("props.id", props.id);

ConversionApiService.get(props.id).then((res) => {
  const conversion = res.data;
  nav.setNavItems([
    {
      label: "Conversions",
      to: "/conversions",
    },
    {
      label: `Conversion #${conversion.id}`,
    },
  ]);
  ui.setTitle(`Conversion #${conversion.id}`);
});
</script>

<route lang="yaml">
meta:
  title: Conversion Details
  requiresRoles: ["operator", "admin"]
</route>
