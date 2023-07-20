<template>
  <el-container class="main-container">
    <CloudContent v-bind="$props"></CloudContent>
  </el-container>
</template>
<script>
import CloudContent from './CloudContent'
export default {
  name: 'DashboardLayout',
  components: {
    CloudContent,
  },
  props: {
    hideSidebar: {
      type: Boolean,
      default: false,
    },
    appName: {
      type: String,
      default: 'Protal',
    },
  },
}
</script>
<style lang="scss" scoped>

.main-container {
  height: 100%;
  flex-direction: column;
}
</style>
