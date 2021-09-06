<template>
  <div>
    <crumb :crumbInfo="crumbInfo"></crumb>
    <div class="cluster alarm">
      <span v-if="h3Txt === 'data'">{{$t('chubaoFS.servers.Datanode')}}</span>
      <span v-else>{{$t('chubaoFS.servers.Matanode')}}</span>
      {{$t('chubaoFS.servers.PartitionList')}}
      <div class="data-block" v-loading="resData.loading">

        <el-table
          :data="resData.resLists"
          class="mt20"
          style="width: 100%"
          v-if="h3Txt === 'data'"
        >
          <el-table-column type="index" label="#"></el-table-column>
          <el-table-column prop="partitionID" :label="$t('chubaoFS.servers.PartitionID')"></el-table-column>
          <el-table-column prop="volName" :label="$t('chubaoFS.servers.Volume')"></el-table-column>
          <el-table-column prop="total" :label="$t('chubaoFS.servers.Total')"></el-table-column>
          <el-table-column prop="used" :label="$t('chubaoFS.servers.Used')"></el-table-column>
          <el-table-column prop="status" :label="$t('chubaoFS.servers.Status')"></el-table-column>
          <el-table-column prop="diskPath" :label="$t('chubaoFS.servers.DiskPath')"></el-table-column>
          <el-table-column prop="isLeader" :label="$t('chubaoFS.servers.Leader')">
            <template slot-scope="scope">
              <div>{{scope.row.isLeader}}</div>
            </template>
          </el-table-column>
          <el-table-column prop="extentCount" :label="$t('chubaoFS.servers.ExtentCount')"></el-table-column>
          <el-table-column prop="needCompare" :label="$t('chubaoFS.servers.NeedCompare')">
            <template slot-scope="scope">
              <div>{{scope.row.needCompare}}</div>
            </template>
          </el-table-column>
        </el-table>

        <el-table
          :data="resData.resLists"
          class="mt20"
          style="width: 100%"
          v-else
        >
          <el-table-column type="index" label="#"></el-table-column>
          <el-table-column prop="partitionID" :label="$t('chubaoFS.servers.PartitionID')"></el-table-column>
          <el-table-column prop="volName" :label="$t('chubaoFS.servers.Volume') "></el-table-column>
          <el-table-column prop="start" :label="$t('chubaoFS.servers.Start')"></el-table-column>
          <el-table-column prop="end" :label="$t('chubaoFS.servers.End')"></el-table-column>
          <el-table-column prop="status" :label="$t('chubaoFS.servers.Status')"></el-table-column>
          <el-table-column prop="isLeader" :label="$t('chubaoFS.servers.Leader')">
            <template slot-scope="scope">
              <div>{{scope.row.isLeader}}</div>
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
  </div>
</template>

<script>
import baseGql from '../../graphql/server'

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
      typeList: ['Meta Partition List'],
      ip: '',
      h3Txt: ''
    }
  },
  computed: {
    crumbInfo () {
      return this.ip
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
      this.resData.resLists = this.storageLists.slice(start, end)
    },
    queryList () {
      const that = this
      that.resData.loading = true
      const variables = {
        addr: this.ip
      }
      const infoFun = this.h3Txt === 'data' ? 'dataNodeGet' : 'metaNodeGet'
      this.apollo.query(this.url.cluster, baseGql[infoFun], variables).then((res) => {
        that.resData.loading = false
        if (res) {
          const data = res.data
          if (this.h3Txt === 'data') {
            this.storageLists = data.dataNodeGet.dataPartitionReports
            this.resData.page.totalRecord = data.dataNodeGet.dataPartitionReports.length
            data.dataNodeGet.dataPartitionReports.forEach(item => {
              item.status = item.partitionStatus==1?"ReadOnly":item.partitionStatus==2?"ReadWrite":"Unavailable"
              console.log(item.partitionStatus==1?"ReadOnly":item.partitionStatus==2?"ReadWrite":"Unavailable")
            });
          } else {
            this.storageLists = data.metaNodeGet.metaPartitionInfos
            this.resData.page.totalRecord = data.metaNodeGet.metaPartitionInfos.length
            data.metaNodeGet.metaPartitionInfos.forEach(item => {
              item.status = item.status==1?"ReadOnly":item.status==2?"ReadWrite":"Unavailable"
            });
          }

          this.handleCurrentChange(1)
        } else {
        }
      }).catch((error) => {
        that.resData.loading = false
        console.log(error)
      })
    }
  },
  mounted () {
    let query = this.$route.query
    this.ip = query.ip
    this.h3Txt = query.type
    this.queryList()
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
