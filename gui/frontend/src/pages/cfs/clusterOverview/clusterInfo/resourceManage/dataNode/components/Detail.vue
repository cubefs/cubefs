<template>
  <el-dialog
    top="3vh"
    :append-to-body="true"
    title="dp详情"
    :visible.sync="dialogFormVisible"
    width="1000px"
    @closed="onClose"
  >
    <el-row class="m-b">
      <el-col :span="6">ID: {{ data.PartitionID }}</el-col>
      <el-col :span="6">副本数: {{ data.ReplicaNum }}</el-col>
      <el-col :span="6">
        <el-row>
          <el-col :span="4"> 卷名: </el-col>
          <el-col :span="20" class="m-r">
            {{ data.VolName }}
          </el-col>
        </el-row>
      </el-col>
      <el-col :span="6">status: {{ data.Status }}</el-col>
    </el-row>
    <el-row class="m-b">
      <div>Peers:</div>
      <u-page-table :data="data.Peers" :has-page="false">
        <el-table-column label="id" prop="id" :width="100"></el-table-column>
        <el-table-column label="addr" prop="addr"></el-table-column></u-page-table>
    </el-row>
    <el-row class="m-b">
      <div>Replicas:11</div>
      <u-page-table :data="data.Replicas" :has-page="false">
        <el-table-column
          label="fileCount"
          prop="FileCount"
          :width="100"
        ></el-table-column>
        <el-table-column label="isLeader" prop="IsLeader">
          <template slot-scope="scope">
            {{ scope.row.IsLeader ? 'true' : false }}
          </template>
        </el-table-column>
        <el-table-column label="status" prop="Status">
          <template slot-scope="scope">
            {{ scope.row.Status | filterStatus }}
          </template>
        </el-table-column>
        <el-table-column label="total" prop="TotalSize">
          <template slot-scope="scope">
            {{ scope.row.TotalSize | renderSize }}
          </template>
        </el-table-column>
        <el-table-column label="used" prop="UsedSize">
          <template slot-scope="scope">
            {{ scope.row.UsedSize | renderSize }}
          </template>
        </el-table-column>
        <el-table-column label="addr" prop="Addr"></el-table-column>
        <el-table-column label="disk" prop="DiskPath"></el-table-column>
        <el-table-column label="report" prop="ReportTime">
          <template slot-scope="scope">
            {{ (scope.row.ReportTime * 1000) | formatDate }}
          </template>
        </el-table-column>
      </u-page-table>
    </el-row>
    <el-row class="m-b">
      <el-col :span="2"> zones: </el-col>
      <el-col :span="20"> {{ data.Zones }} </el-col>
    </el-row>
    <el-row class="m-b">
      <el-col :span="2"> hosts: </el-col>
      <el-col :span="20">
        <div v-for="item in data.Hosts" :key="item">{{ item }}</div>
      </el-col>
    </el-row>
  </el-dialog>
</template>
<script>
import UPageTable from '@/pages/components/uPageTable'
import { renderSize, formatDate } from '@/utils'
export default {
  components: {
    UPageTable,
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
      this.data = {
        ...data,
        Zones: data?.Zones?.join(',') || '',
      }
      this.open()
    },
    open() {
      this.dialogFormVisible = true
    },
    onClose() {
      this.dialogFormVisible = false
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
</style>
