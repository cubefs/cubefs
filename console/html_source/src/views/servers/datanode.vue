<template>
  <div class="alarm">
    <div class="fr mb20">
      <el-input v-model="searchVal" style="width: 255px;"></el-input>
      <el-button type="primary" class="ml5" @click="queryList">{{ $t('chubaoFS.tools.Search') }}</el-button>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        class="mt20"
        style="width: 100%">
        <!--<el-table-column type="index" label="#"></el-table-column>-->
        <el-table-column prop="address" :label="$t('chubaoFS.servers.Address')">
          <template slot-scope="scope">
            <div class="volume-name" @click="goServerDetail(scope.row)">{{scope.row.addr}}</div>
          </template>
        </el-table-column>
        <el-table-column prop="toDataNode.zoneName" :label="$t('chubaoFS.servers.Zone')">
          <!--<template slot-scope="scope">-->
          <!--<div>{{scope.row.addr.split(":")[1]}}</div>-->
          <!--</template>-->
        </el-table-column>
        <el-table-column prop="iD" :label="$t('chubaoFS.servers.identity')"></el-table-column>
        <el-table-column prop="toDataNode.dataPartitionCount" :label="$t('chubaoFS.servers.PartitionCount')" width="150"></el-table-column>
        <el-table-column prop="toDataNode.used" :label="$t('chubaoFS.servers.Used')"></el-table-column>
        <el-table-column prop="toDataNode.total" :label="$t('chubaoFS.servers.Total')"></el-table-column>
        <el-table-column prop="toDataNode.available" :label="$t('chubaoFS.servers.Available')"></el-table-column>
        <el-table-column prop="toDataNode.usageRatio" :label="$t('chubaoFS.servers.UsedRate')"></el-table-column>
        <el-table-column prop="toDataNode.reportTime" :label="$t('chubaoFS.servers.ReportTime')" width="150"></el-table-column>
        <el-table-column prop="toDataNode.isActive" :label="$t('chubaoFS.servers.IsActive')">
          <template slot-scope="scope">
            <div>{{scope.row.toDataNode.isActive}}</div>
          </template>
        </el-table-column>
      </el-table>
      <div class="clearfix mt20">
        <el-pagination
          class="fr"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
          :page-sizes="resData.page.pageSizes"
          :page-size="resData.page.pageSize"
          layout="sizes, prev, pager, next"
          :total="resData.page.totalRecord">
        </el-pagination>
        <span class="fr page-tips pr10">{{ $t('chubaoFS.commonTxt.eachPageShows') }}</span>
      </div>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/server'
import { formatSize } from '../../utils/string.js'
export default {
  name: 'alarm',
  data () {
    return {
      resData: {
        loading: true,
        page: {
          pageSizes: [10, 20, 30, 40],
          pageNo: 1,
          pageSize: 10,
          totalRecord: 0,
          totalPage: 1
        },
        resLists: []
      },
      typeVal: '',
      searchVal: null
    }
  },
  methods: {
    handleSizeChange (val) {
      this.resData.page.pageSize = val
      this.resData.page.pageNo = 1
      this.handleCurrentChange(1)
    },
    handleCurrentChange (val) {
      this.resData.page.pageNo = val
      const start = (val - 1) * this.resData.page.pageSize
      const end = val * this.resData.page.pageSize
      this.resData.resLists = this.resetData(this.storageLists.slice(start, end))
    },
    resetData (data) {
      data.forEach(eachItem => {
        eachItem.toDataNode.available = formatSize(((eachItem.toDataNode.total - eachItem.toDataNode.used)), 10)
        eachItem.toDataNode.used = formatSize((eachItem.toDataNode.used), 10)
        eachItem.toDataNode.total = formatSize((eachItem.toDataNode.total), 10)
        eachItem.toDataNode.usageRatio = parseFloat((eachItem.toDataNode.usageRatio * 100).toFixed(2))
        const reportTime = eachItem.toDataNode.reportTime
        eachItem.toDataNode.reportTime = reportTime.slice(0, reportTime.indexOf('.')).replace('T', ' ')
      })
      return data
    },
    queryList () {
      const that = this
      that.resData.loading = true
      const variables = {
        num: 10000
      }
      this.apollo.query(this.url.cluster, baseGql.queryDataList, variables).then((res) => {
        that.resData.loading = false
        if (res) {
          const data = res.data.clusterView.dataNodes
          let finalLists = []
          if (this.searchVal && this.searchVal.trim() !== '') {
            data.forEach(eachItem => {
              if (eachItem.addr.indexOf(this.searchVal.trim()) > -1) {
                finalLists.push(eachItem)
              }
            })
          } else {
            finalLists = data
          }
          this.storageLists = finalLists
          this.resData.page.totalRecord = finalLists.length
          this.handleCurrentChange(1)
        } else {
        }
      }).catch((error) => {
        that.resData.loading = false
        console.log(error)
      })
    },
    goServerDetail (row) {
      this.$router.push({
        name: 'serverDetail',
        query: {
          ip: row.addr,
          type: 'data'
        }
      })
    }
  },
  mounted () {
    this.queryList()
    // this.resData.resLists = this.testData
    // this.resData.loading = false
  }
}
</script>

<style scoped>
  .alarm h3{
    line-height: 16px;
    font-family: Helvetica;
    font-weight: normal;
    font-size: 13px;
    color: rgba(51,51,51,1);
    margin-bottom: 20px;
  }
  .volume-name{
    cursor: pointer;
    color: #466BE4;
  }
</style>
