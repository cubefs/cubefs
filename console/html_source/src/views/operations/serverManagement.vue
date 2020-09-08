<template>
  <div class="operation-server">
    <div v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        style="width: 100%">
        <el-table-column prop="name" :label="$t('chubaoFS.operations.ServerManagement.Cluster')">
        </el-table-column>
        <el-table-column prop="allDataNode.zoneName" :label="$t('chubaoFS.operations.ServerManagement.IDC')">
        </el-table-column>
        <el-table-column prop="addr" :label="$t('chubaoFS.operations.ServerManagement.ServerIP')">
        </el-table-column>
        <el-table-column prop="serverType" :label="$t('chubaoFS.operations.ServerManagement.ServiceType')">
        </el-table-column>
        <el-table-column prop="statusStr" :label="$t('chubaoFS.operations.ServerManagement.Status')">
        </el-table-column>
        <el-table-column prop="isWritableStr" :label="$t('chubaoFS.operations.ServerManagement.IsWritable')">
        </el-table-column>
        <el-table-column prop="allDataNode.used" :label="$t('chubaoFS.operations.ServerManagement.UsedCapacity')">
        </el-table-column>
        <el-table-column prop="allDataNode.total" :label="$t('chubaoFS.operations.ServerManagement.TotalCapacity')">
        </el-table-column>
        <el-table-column prop="allDataNode.capacityRatio" :label="$t('chubaoFS.operations.ServerManagement.Used')">
        </el-table-column>
        <el-table-column :label="$t('chubaoFS.tools.Actions')" width="230">
          <template slot-scope="scope">
            <el-button type="text" class="text-btn" v-show="scope.row.serverType === 'dataNodes'" @click="openDialog('editDisk', scope.row)">{{ $t('chubaoFS.operations.ServerManagement.OffLineDisk') }}</el-button>
            <el-button type="text" class="text-btn" @click="editServer(scope.row)">{{ $t('chubaoFS.operations.ServerManagement.OffLineServer') }}</el-button>
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
    <!-- Disk -->
    <el-dialog
      :title="$t('chubaoFS.operations.ServerManagement.OffLineDisk')"
      :visible.sync="diskDialog"
      width="30%"
      @close="closeDialog()">
      <p class="mb20">{{$t('chubaoFS.operations.ServerManagement.diskTip')}}</p>
      <el-checkbox-group v-model="checkList">
        <el-checkbox v-for="item in checkListVal" :label="item" :key="item">{{item}}</el-checkbox>
      </el-checkbox-group>
      <span slot="footer" class="dialog-footer">
        <el-button @click="closeDialog">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary"  @click="editDisk">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { formatSize } from '../../utils/string.js'
import baseGql from '../../graphql/operations'
export default {
  name: 'OperationServer',
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
      serverList: [],
      dataNodesList: [],
      metaNodesList: [],
      diskDialog: false,
      diskData: {},
      checkList: [],
      checkListVal: [],
      diskTip: this.$t('chubaoFS.operations.ServerManagement.diskTip')
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
      this.resData.resLists = this.serverList.slice(start, end)
    },
    queryServerList () {
      const that = this
      that.resData.loading = true
      const variables = {
        num: 10000
      }
      this.apollo.query(this.url.cluster, baseGql.serverManagementList, variables).then((res) => {
        that.resData.loading = false
        if (res.data) {
          const data = res.data
          this.dataNodesList = data.clusterView.dataNodes
          this.metaNodesList = data.clusterView.metaNodes
          this.dataNodesList.forEach(item => {
            item.name = data.clusterView.name
            item.serverType = 'dataNodes'
            item.statusStr = item.status.toString()
            item.isWritableStr = item.isWritable.toString()
            item.allDataNode = item.toDataNode
            item.allDataNode.used = formatSize(item.toDataNode.used,10)
            item.allDataNode.total = formatSize(item.toDataNode.total,10)
            item.allDataNode.capacityRatio = ((item.toDataNode.usageRatio) * 100).toFixed(2) + '%'
            this.checkListVal = item.reportDisks
          })
          this.metaNodesList.forEach(item => {
            item.name = data.clusterView.name
            item.serverType = 'metaNodes'
            item.statusStr = item.status.toString()
            item.isWritableStr = item.isWritable.toString()
            item.allDataNode = item.toMetaNode
            item.allDataNode.used = formatSize(item.toMetaNode.used,10)
            item.allDataNode.total = formatSize(item.toMetaNode.total,10)
            item.allDataNode.capacityRatio = ((item.toMetaNode.ratio) * 100).toFixed(2) + '%'
          })
          this.serverList = this.dataNodesList.concat(this.metaNodesList)
          // 按状态倒序排列
          this.serverList.sort(function (a, b) {
            if (a.status === b.status) {
              return 0
            } else {
              return b.status ? -1 : 1
            }
          })
          this.resData.page.totalRecord = this.serverList.length
          this.handleCurrentChange(1)
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        that.resData.loading = false
        console.log(error)
      })
    },
    openDialog (tag, row) {
      if (tag === 'editDisk') {
        this.diskDialog = true
        this.diskData = Object.assign({}, row)
        if (this.checkListVal.length === 0) {
          this.diskTip = this.$t('chubaoFS.operations.ServerManagement.NoDisk')
        }
      }
    },
    editDisk () {
      if (this.checkList.length === 0) {
        this.$message.error(this.$t('chubaoFS.operations.ServerManagement.NoDisk'))
      }
      for (let i = 0; i < this.checkList.length; i++) {
        const variables = {
          offLineAddr: this.diskData.addr,
          diskPath: this.checkList[i]
        }
        this.apollo.mutation(this.url.cluster, baseGql.decommissionDisk, variables).then((res) => {
          if (res.code === 200) {
            this.queryServerList()
            this.diskDialog = false
            this.$message({
              message: this.$t('chubaoFS.message.Success'),
              type: 'success'
            })
          } else {
            this.$message.error(res.message)
          }
        })
      }
    },
    editServer (row) {
      const variables = {
        offLineAddr: row.addr
      }
      this.$confirm(this.$t('chubaoFS.operations.ServerManagement.TakeOffServerTip') + ' (' + row.addr + ')', this.$t('chubaoFS.operations.ServerManagement.OffLineServer'), {
        confirmButtonText: this.$t('chubaoFS.tools.Yes'),
        cancelButtonText: this.$t('chubaoFS.tools.No'),
        type: 'warning'
      }).then(() => {
        if (row.serverType === 'dataNodes') {
          this.apollo.mutation(this.url.cluster, baseGql.decommissionDataNode, variables).then((res) => {
            if (res.code === 200) {
              this.queryServerList()
              this.$message({
                message: res.data.decommissionDataNode.message,
                type: 'success'
              })
            } else {
              this.$message.error(res.message)
            }
          })
        } else if (row.serverType === 'metaNodes') {
          this.apollo.mutation(this.url.cluster, baseGql.decommissionMetaNode, variables).then((res) => {
            if (res.code === 200) {
              this.queryServerList()
              this.$message({
                message: res.data.decommissionDataNode.message,
                type: 'success'
              })
            } else {
              this.$message.error(res.message)
            }
          })
        }
      }).catch(() => {
      })
    },
    closeDialog () {
      this.diskDialog = false
      this.checkList = []
    }
  },
  mounted () {
    this.queryServerList()
  }
}
</script>

<style scoped>

</style>
