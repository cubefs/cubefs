<template>
  <div id="app">
    <Noop v-if="layout === 'Noop'"></Noop>
    <Common v-else app-name="CubeFS控制台" :hide-psa-side-bar="false"></Common>
  </div>
</template>
<script>
import { Common } from '@/layouts/Common'
import Noop from '@/layouts/Noop.vue'
export default {
  name: 'App',
  components: {
    Noop,
    Common,
  },
  computed: {
    layout() {
      return this.$route.meta.layout || 'Common'
    },
  },
}
</script>

<style lang="scss" src="@/assets/styles/index.scss"></style>
