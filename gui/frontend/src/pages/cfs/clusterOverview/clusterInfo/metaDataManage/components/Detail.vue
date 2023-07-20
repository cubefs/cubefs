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
      <div class="dialog-title">mp详情</div>
      <el-tooltip effect="dark" content="导出Excel" placement="right">
        <i class="el-icon-download icon" @click="onExportClick"></i>
      </el-tooltip>
    </div>
    <el-row class="m-b">
      <el-col :span="3">ID: {{ data.PartitionID }}</el-col>
      <el-col :span="3">副本数: {{ data.ReplicaNum }}</el-col>
      <el-col :span="7">
        <el-row>
          <el-col :span="3"> 卷名: </el-col>
          <el-col :span="21" class="m-r">
            {{ data.VolName }}
          </el-col>
        </el-row></el-col>
      <el-col :span="3">状态: {{ data.Status }}</el-col>
      <el-col :span="4">peers是否匹配:
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
    <el-row class="m-b">
      <el-col :span="3">inodeCount: {{ data.InodeCount }}</el-col>
      <el-col :span="3">dentryCount: {{ data.DentryCount }}</el-col>
      <el-col :span="6">isRecovering: {{ data.IsRecover }}</el-col>
    </el-row>
    <el-row class="m-b">
      <el-col :span="3">start: {{ data.Start }}</el-col>
      <el-col :span="3">end: {{ data.End }}</el-col>
      <el-col :span="6">maxInode: {{ data.MaxInodeID }}</el-col>
    </el-row>
    <div class="replica-row m-t-20">
      <ReplicasCard :replica="data.Response[0]" :host="data.Hosts[0]" :zone="data.Zones[0]" :index="0" />
    </div>
    <div class="replica-row m-t-20">
      <div style="display: flex;">
        <ReplicasCard
          v-for="(item, index) in data.Response.slice(1)"
          :key="index"
          :replica="item"
          :host="data.Hosts[index+1]"
          :zone="data.Zones[index+1]"
          :index="index+1"
          style="margin: 0px 10px;"
        />
      </div>
    </div>
  </el-dialog>
</template>
<script>
import ReplicasCard from './ReplicasCard.vue'
export default {
  components: {
    ReplicasCard,
  },

  data() {
    return {
      dialogFormVisible: false,
      data: {},
    }
  },
  methods: {
    init(data) {
      this.data = {
        ...data,
        Response: data?.Response?.map((item) => {
          return {
            ...item,
            Status: data?.Status,
          }
        }),
      }
      this.open()
    },
    open() {
      this.dialogFormVisible = true
    },
    onClose() {
      this.dialogFormVisible = false
    },
    comparePeers() {
      const arr1 = this.data.Peers?.map(item => item.addr)
      const arr2 = this.data.Response?.map(item => item.Addr)
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
      const exportData = this.data.Response.map((item, index) => ({
        applyId: item.ApplyID,
        addr: item.Addr,
        inodeCount: item.InodeCount,
        maxInode: item.MaxInode,
        状态: item.Status,
        目录数: item.DentryCount,
        hostID: this.data.Hosts[index],
        zone: this.data.Zones[index],
      }))
      const wb = XLSX.utils.book_new()
      const ws = XLSX.utils.json_to_sheet(exportData)
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1')
      XLSX.writeFile(wb, 'data.xlsx')
    },
  },
}
</script>
<style lang="scss" scoped>
.m-b {
  margin-bottom: 20px;
}

.m-r {
  padding-right: 5px;
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
