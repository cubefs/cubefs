<template>
  <el-dialog
    top="3vh"
    :append-to-body="true"
    title="mp详情"
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
        </el-row></el-col>
      <el-col :span="6">status: {{ data.Status }}</el-col>
    </el-row>
    <el-row class="m-b">
      <el-col :span="6">inodeCount: {{ data.InodeCount }}</el-col>
      <el-col :span="6">dentryCount: {{ data.DentryCount }}</el-col>
      <el-col :span="6">isRecovering: {{ data.IsRecover }}</el-col>
    </el-row>
    <el-row class="m-b">
      <el-col :span="6">start: {{ data.Start }}</el-col>
      <el-col :span="6">end: {{ data.End }}</el-col>
      <el-col :span="6">maxInode: {{ data.MaxInodeID }}</el-col>
    </el-row>
    <el-row class="m-b">
      <div>Peers:</div>
      <u-page-table :data="data.Peers" :has-page="false">
        <el-table-column label="id" prop="id" :width="100"></el-table-column>
        <el-table-column label="addr" prop="addr"></el-table-column></u-page-table>
    </el-row>
    <el-row class="m-b">
      <div>Response:</div>
      <u-page-table :data="data.Response" :has-page="false">
        <el-table-column label="applyId" prop="ApplyID" :width="100"></el-table-column>
        <el-table-column label="目录数" prop="DentryCount"></el-table-column>
        <el-table-column label="status" prop="Status" :width="100">
        </el-table-column>
        <el-table-column label="inodeCount" prop="InodeCount"></el-table-column>
        <el-table-column label="maxInode" prop="MaxInode"></el-table-column>
        <el-table-column label="addr" prop="Addr"></el-table-column>
        <el-table-column label="id" prop="PartitionID" :width="100"></el-table-column>
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
export default {
  components: {
    UPageTable,
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
