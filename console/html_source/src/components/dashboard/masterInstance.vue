<template>
  <div>
    <el-select v-model="selVal" @change="onChange">
      <el-option
        v-for="item in options"
        :key="item.addr"
        :label="item.addr"
        :value="item.addr">
      </el-option>
    </el-select>
  </div>
</template>

<script>
import baseGql from '../../graphql/dashboard'

export default {
  name: 'masterInstanceSel',
  data () {
    return {
      selVal: [],
      options: []
    }
  },
  methods: {
    onChange () {
      this.$parent.masterInstanceChange(this.selVal)
    },
    queryList () {
      const that = this
      this.apollo.query(this.url.cluster, baseGql.masterList, null).then((res) => {
        if (res) {
          const data = res.data
          this.options = data.masterList
          this.selVal = this.options[0].addr
          this.onChange()
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
