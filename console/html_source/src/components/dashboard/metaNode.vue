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
  name: 'metaNodeSel',
  data () {
    return {
      selVal: [],
      options: dashboardParam.metaNodeParam
    }
  },
  methods: {
    onChange () {
      this.$parent.metaNodeChange(this.selVal)
    }
  },
  mounted () {
    this.selVal.push(dashboardParam.metaNodeParam[0])
  }
}
</script>
