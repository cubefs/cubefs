<template>
  <router-view
    :key="uniqueKey"
    class="sub-app-content"
    :class="['sub-app-dashboard-content' ]"
  />
</template>
<script>
export default {
  name: 'MainContent',
  computed: {
    uniqueKey() {
      return 'DashboardLayout'
    },
  },
}
</script>
