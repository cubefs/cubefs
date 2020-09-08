<template>
  <div class="cluster alarm">
    <h3>{{ $t('chubaoFS.alarm.name') }}</h3>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        style="width: 100%">
        <!--<el-table-column-->
          <!--label=""-->
          <!--width="180">-->
          <!--<template slot-scope="scope">-->
            <!--&lt;!&ndash;<i class="new-icon"></i>&ndash;&gt;-->
            <!--<i v-if="scope.row.type === 1" class="new-icon"></i>-->
          <!--</template>-->
        <!--</el-table-column>-->
        <el-table-column
          prop=""
          :label="$t('chubaoFS.alarm.detail.ClusterID')"
          width="180">
          <template slot-scope="scope">
            {{clusterName}}
          </template>
        </el-table-column>
        <el-table-column
          prop="detail"
          :label="$t('chubaoFS.alarm.detail.Alarm')"
        >
          <template slot-scope="scope">
          <!--<i class="new-icon"></i>-->
            <a @click="showDetail(scope.row)">{{scope.row.detail}}</a>
          </template>
        </el-table-column>
        <el-table-column
          prop="hostname"
          :label="$t('chubaoFS.alarm.detail.Host')">
        </el-table-column>
        <el-table-column
          prop="time"
          :label="$t('chubaoFS.alarm.detail.ClusterID')">
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
    <el-dialog
      :title="$t('chubaoFS.alarm.name')"
      :visible.sync="detailDialog"
      width="35%">
      <div v-if="alarmInfo">
        <el-row :gutter="20" class="mb20">
          <el-col :span="12">
            <span class="alarm-key mr10">{{ $t('chubaoFS.alarm.name') }}:</span><span>{{clusterName}}</span>
          </el-col>
          <el-col :span="12">
            <span class="alarm-key mr10">{{ $t('chubaoFS.alarm.detail.Host') }}:</span><span>{{alarmInfo.hostname}}</span>
          </el-col>
        </el-row>
        <el-row class="mb20">
          <span class="alarm-key mr10">{{ $t('chubaoFS.alarm.detail.Time') }}:</span><span>{{alarmInfo.time}}</span>
        </el-row>
        <p class="mb20">{{alarmInfo.detail}}</p>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from '../graphql/alarm'
export default {
  name: 'alarm',
  data () {
    return {
      clusterName: null,
      storageLists: [],
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
      signUrlDialog: false,
      alarmInfo: null,
      detailDialog: false
    }
  },
  methods: {
    showDetail (val) {
      this.alarmInfo = val
      this.detailDialog = true
    },
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
        size: 1000
      }
      this.apollo.query(this.url.cluster, baseGql.queryAlarmList, variables).then((res) => {
        that.resData.loading = false
        if (!res.code) {
          const data = res.data.alarmList
          this.storageLists = data
          this.resData.page.totalRecord = data.length
          this.handleCurrentChange(1)
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        that.resData.loading = false
        console.log(error)
      })
    },
    queryClusterView () {
      const that = this
      const variables = {}
      this.apollo.query(this.url.cluster, baseGql.clusterView, variables).then((res) => {
        that.resData.loading = false
        if (!res.code) {
          const data = res.data.clusterView
          this.clusterName = data.name
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        console.log(error)
      })
    }
  },
  mounted () {
    this.queryClusterView()
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
  .alarm .new-icon{
    width: 7px;
    height: 7px;
    background: rgba(0,158,255,1);
    border-radius: 7px;
  }
  .alarm-key{
    font-weight: bold;
  }
</style>
