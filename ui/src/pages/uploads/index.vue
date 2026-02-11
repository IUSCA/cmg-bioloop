<template>
  <div>
    This is a test page for the testroute route.
  </div>
</template>

<route lang="yaml">
meta:
  title: Test Route
  requiresRoles: ["operator", "admin"]
  nav: [{ label: "Test Route" }]
</route>
