<template>
  <div class="cluster health">
    <h3>Version Info</h3>
    <div class="data-block" v-loading="resData.loading">
      <el-table :data="resData.resLists" style="width: 100%">
        <el-table-column prop="iP" label="IP" ></el-table-column>
        <el-table-column prop="versionValue.model" label="model" :sortable="true" ></el-table-column>
        <el-table-column prop="versionValue.branchName" label="branch" ></el-table-column>
        <el-table-column prop="versionValue.commitID" label="commitID" width="300" :sortable="true"></el-table-column>
        <el-table-column prop="message" label="message"></el-table-column>
      </el-table>
    </div>
  </div>
</template>

<script>
import baseGql from '../graphql/health'
export default {
  name: 'health',
  data () {
    return {
      clusterName: null,
      storageLists: [],
      resData: {
        loading: true,
        resLists: []
      },
      signUrlDialog: false,
      detailDialog: false
    }
  },
  methods: {
    queryList () {
      const that = this
      that.resData.loading = true
      this.apollo.query(this.url.monitor, baseGql.versionCheckList, {}).then((res) => {
        that.resData.loading = false
        if (!res.code) {
          const data = res.data.VersionCheck
          console.log(data)
          that.resData.resLists = data
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        that.resData.loading = false
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
.health h3{
  line-height: 16px;
  font-family: Helvetica;
  font-weight: normal;
  font-size: 13px;
  color: rgba(51,51,51,1);
  margin-bottom: 20px;
}
  .health .new-icon{
    width: 7px;
    height: 7px;
    background: rgba(0,158,255,1);
    border-radius: 7px;
  }
  .health-key{
    font-weight: bold;
  }
</style>
