<template>
  <div>
    <el-select v-model="selVal" @change="onChange">
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
export default {
  name: 'clusterSel',
  data () {
    return {
      selVal: 'master',
      options: [
        'fuseClient',
        'metaNode',
        'dataNode',
        'objectNode',
        'master'
      ]
    }
  },
  methods: {
    onChange () {
      this.$parent.roleChange(this.selVal)
    }
  },
  mounted () {
    this.$parent.roleChange(this.selVal)
  }
}
</script>
