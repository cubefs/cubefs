<template>
  <div class="inline-block dash-sel-b">
    <el-select v-model="selVal" multiple collapse-tags @change="onChange">
      <el-option
        v-for="item in options"
        :key="item"
        :label="item"
        :value="item">
      </el-option>
    </el-select>
  </div>
</template>

<script>
import dashboardParam from '../../config/dashboardParam'
export default {
  name: 'fuseOpSel',
  data () {
    return {
      selVal: [],
      options: dashboardParam.fuseOpParam
    }
  },
  methods: {
    onChange () {
      this.$parent.fuseOpChange(this.selVal)
    }
  },
  mounted () {
    this.selVal.push(dashboardParam.fuseOpParam[9])
  }
}
</script>

<style scoped>
</style>
