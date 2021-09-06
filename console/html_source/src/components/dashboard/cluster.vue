<template>
  <div>
    <el-select v-model="selVal" disabled>
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
import baseGql from '../../graphql/dashboard'

export default {
  name: 'clusterSel',
  data () {
    return {
      selVal: null,
      options: []
    }
  },
  methods: {
    onChange () {
      this.$parent.clusterChange(this.selVal)
    },
    queryList () {
      const that = this
      this.apollo.query(this.url.cluster, baseGql.clusterView, null).then((res) => {
        if (res) {
          const data = res.data
          this.options = data.clusterView
          this.selVal = this.options.name
          this.$parent.clusterChange(this.selVal)
        } else {
        }
      }).catch((error) => {
        that.options = []
        console.log(error)
      })
    }
  },
  mounted () {
    this.queryList()
  }
}
</script>

<style scoped>
</style>
