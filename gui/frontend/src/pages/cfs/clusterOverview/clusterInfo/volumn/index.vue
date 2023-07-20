<template>
  <div>
    <el-radio-group v-model="activeName" style="margin-bottom: 10px;">
      <el-radio-button label="multiple">多副本卷</el-radio-button>
      <el-radio-button label="cold" :disabled="!ebsClusterList || !ebsClusterList.length">纠删码卷</el-radio-button>
    </el-radio-group>
    <Multiple v-if="activeName === 'multiple'" />
    <Cold v-if="activeName === 'cold'" />
  </div>
</template>

<script>
import Cold from './coldVolume/index.vue'
import Multiple from './multipleVolume/index.vue'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  name: '',
  components: { Cold, Multiple },
  mixins: [mixin],
  data () {
    return {
      activeName: 'multiple',
    }
  },
  computed: {},
  watch: {},
  created() {},
  beforeMount() {},
  mounted() {},
  methods: {},
}
</script>
<style scoped lang='scss'>
</style>
