<template>
  <div>
    <div class="mg-bt-s">
      <div>
        <el-radio-group v-model="activeName" style="margin-bottom: 10px;">
          <el-radio-button label="blobEvent" :disabled="!ebsClusterList || !ebsClusterList.length">纠删码后台任务</el-radio-button>
        </el-radio-group>
      </div>
    </div>
    <el-card>
      <blobEvent v-if="activeName === 'blobEvent'" />
    </el-card>
  </div>
</template>
<script>
import blobEvent from './components/blobStoreEvent.vue'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  name: '',
  components: { blobEvent },
  mixins: [mixin],
  data () {
    return {
      activeName: '',
    }
  },
  computed: {},
  watch: {},
  created() {
    if (this.ebsClusterList && this.ebsClusterList.length) {
      this.activeName = 'blobEvent'
    }
  },
  beforeMount() {},
  mounted() {},
  methods: {},
}
</script>
<style lang='scss' scoped>
</style>
