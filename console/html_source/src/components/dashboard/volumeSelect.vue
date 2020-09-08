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
      this.$parent.volChange(this.selVal)
    },
    queryList () {
      const variables = {
        keyword: '',
        userID: sessionStorage.getItem('access_userID'),
        num: 10000
      }
      this.apollo.query(this.url.volume, baseGql.queryVolumeList, variables).then((res) => {
        if (res) {
          res.data.listVolume.forEach(item => {
            this.options.push(item.name)
          })
          this.selVal = this.options[0]
          this.$parent.volChange(this.selVal)
        }
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
