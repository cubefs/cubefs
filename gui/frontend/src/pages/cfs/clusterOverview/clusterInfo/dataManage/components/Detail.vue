<template>
  <el-dialog
    v-if="dialogFormVisible"
    top="3vh"
    :append-to-body="true"
    :visible.sync="dialogFormVisible"
    width="1200px"
    @closed="onClose"
  >
    <div slot="title" style="display: flex;">
      <div class="dialog-title">dp详情</div>
      <el-tooltip effect="dark" content="导出Excel" placement="right">
        <i class="el-icon-download icon" @click="onExportClick"></i>
      </el-tooltip>
    </div>
    <el-row class="m-b info-text">
      <el-col :span="2">ID: {{ data.PartitionID }}</el-col>
      <el-col :span="2">副本数: {{ data.ReplicaNum }}</el-col>
      <el-col :span="4">卷名: {{ data.VolName }}</el-col>
      <el-col :span="4">状态: {{ data.Status }}</el-col>
      <el-col :span="4">Peers是否匹配:
        <el-tooltip :disabled="comparePeers()" effect="light" content="" placement="bottom">
          <span>{{ comparePeers() ? '是' : '否' }}</span>
          <div slot="content">
            <el-table
              :data="data.Peers"
              style="width: 100%"
            >
              <el-table-column
                prop="id"
                label="ID"
                width="180">
              </el-table-column>
              <el-table-column
                prop="addr"
                label="addr"
                width="180">
              </el-table-column>
            </el-table>
          </div>
        </el-tooltip>
      </el-col>
    </el-row>
    <div class="replica-row m-t-20">
      <ReplicasCard :replica="data.Replicas[0]" :host="data.Hosts[0]" :zone="data.Zones[0]" :index="0" />
    </div>
    <div class="replica-row m-t-20">
      <div style="display: flex;">
        <ReplicasCard
          v-for="(item, index) in data.Replicas.slice(1)"
          :key="index"
          :replica="item"
          :host="data.Hosts[index+1]"
          :zone="data.Zones[index+1]"
          :index="index+1"
          :used-error="compareSizes(data.Replicas[0].UsedSize, item.UsedSize)"
          style="margin: 0px 10px;"
        />
      </div>
    </div>
  </el-dialog>
</template>
<script>
import { renderSize, formatDate } from '@/utils'
import ReplicasCard from './ReplicasCard.vue'
export default {
  components: {
    ReplicasCard,
  },
  filters: {
    renderSize(val) {
      return renderSize(val)
    },
    formatDate(val) {
      return formatDate(val)
    },
    filterStatus(val) {
      switch (val) {
        case 1:
          return 'ReadOnly'
        case 2:
          return 'ReadWrite'
        case -1:
          return 'Unavailable'
        default:
          return 'Unavailable'
      }
    },
  },
  data() {
    return {
      dialogFormVisible: false,
      data: {},
    }
  },
  methods: {
    init(data) {
      this.data = data
      this.open()
    },
    open() {
      this.dialogFormVisible = true
    },
    onClose() {
      this.dialogFormVisible = false
    },
    compareSizes(a, b) {
      return (a / 1024 / 1024 / 1024 - b / 1024 / 1024 / 1024) > 1
    },
    comparePeers() {
      const arr1 = this.data.Peers.map(item => item.addr)
      const arr2 = this.data.Replicas.map(item => item.Addr)
      if (arr1.length !== arr2.length) {
        return false
      }

      const tempArr = [...arr2]

      for (let i = 0; i < arr1.length; i++) {
        const index = tempArr.findIndex(e => e === arr1[i])
        if (index === -1) {
          return false
        }
        tempArr.splice(index, 1)
      }

      return true
    },
    onExportClick() {
      const XLSX = require('xlsx')
      const exportData = this.data.Replicas.map((item, index) => ({
        files: item.FileCount,
        addr: item.Addr,
        Leader: item.IsLeader,
        disk: item.DiskPath,
        status: this.filterStatus(item.Status),
        report: this.formatDate(item.ReportTime * 1000),
        hostID: this.data.Hosts[index],
        zone: this.data.Zones[index],
        used: this.renderSize(item.UsedSize),
        total: this.renderSize(item.TotalSize),
      }))
      const wb = XLSX.utils.book_new()
      const ws = XLSX.utils.json_to_sheet(exportData)
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1')
      XLSX.writeFile(wb, 'data.xlsx')
    },
    renderSize(val) {
      return renderSize(val)
    },
    formatDate(val) {
      return formatDate(val)
    },
    filterStatus(val) {
      switch (val) {
        case 1:
          return 'ReadOnly'
        case 2:
          return 'ReadWrite'
        case -1:
          return 'Unavailable'
        default:
          return 'Unavailable'
      }
    },
  },
}
</script>
<style lang="scss" scoped>
.m-b {
  margin-bottom: 10px;
}
.m-r {
  padding-right: 20px;
}

.replica-row {
  display: flex;
  justify-content: center;
}
.dialog-title {
  line-height: 24px;
  font-size: 18px;
  color: #303133;
  margin-right: 5px;
}
.icon {
  line-height: 24px;
  font-size: 18px;
  color: #303133;
  cursor: pointer;
}
</style>
