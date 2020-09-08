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
  name: 'dataNodeSel',
  data () {
    return {
      selVal: [],
      options: dashboardParam.dataNodeParam
    }
  },
  methods: {
    onChange () {
      this.$parent.dataNodeChange(this.selVal)
    }
  },
  mounted () {
    this.selVal.push(dashboardParam.dataNodeParam[0])
  }
}
</script>

<style scoped>
</style>
