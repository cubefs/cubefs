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
      selVal: 5,
      options: [5, 10, 20, 30, 50, 100]
    }
  },
  methods: {
    onChange () {
      this.$parent.topkChange(this.selVal)
    }
  },
  mounted () {
    this.$parent.topkChange(this.selVal)
  }
}
</script>
