<template>
  <div>
    <el-card class="height mg-lf-s">
      <div class="justify-content-around">
        <CircleCharts
          v-for="(item, index) in chartData"
          :key="index"
          :e-data="item.data"
          :title="item.title"
          :color="['#00DAB3', '#ff0000']"
          @showDialog="showDialog"
        ></CircleCharts>
      </div>
      <div class="justify-content-around" style="margin-top: 20px;">
        <CircleCharts
          v-for="(item, index) in mpDpchartData"
          :key="index"
          :e-data="item.data"
          :title="item.title"
          :value-format="mpDpChartValueFormat"
          @showDialog="showDialog"
        >
        </CircleCharts>
      </div>
    </el-card>
    <el-dialog
      v-if="DiskDialogVisible"
      title="坏磁盘"
      width="65%"
      top="10vh"
      :visible.sync="DiskDialogVisible"
      center
    >
      <div style="margin-bottom: 10px; text-align: right;">
        <el-button type="primary" @click="batchOfflineDisk">批量下线</el-button>
      </div>
      <el-table
        :data="diskTableData"
        style="width: 100%"
        @selection-change="handleSelectionDiskData"
      >
        <el-table-column type="selection" width="80"></el-table-column>
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="Path"
          prop="Path"
        ></el-table-column>
        <el-table-column label="IDs">
          <template slot-scope="scope">
            <span v-for="item in scope.row.PartitionIDs" :key="item">{{ item }}
            </span></template>
        </el-table-column>
      </el-table>
    </el-dialog>
    <el-dialog
      v-if="DataPartitionDetailDialogVisible"
      title="坏DP详情"
      width="65%"
      :visible.sync="DataPartitionDetailDialogVisible"
      center
    >
      <el-table
        :data="PartitionTableData"
        style="width: 100%"
      >
        <el-table-column
          label="分区ID"
          prop="PartitionID"
          :width="100"
        ></el-table-column>
        <el-table-column label="卷名" prop="VolName"></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column label="isRecovering" :width="100">
          <template slot-scope="scope">
            <span>{{ scope.row.IsRecover }}</span>
          </template>
        </el-table-column>
        <el-table-column label="Leader" prop="Leader" width="180"></el-table-column>
        <el-table-column label="Members" width="180">
          <template slot-scope="scope">
            <div v-for="item in scope.row.Members" :key="item">{{ item }}</div>
          </template>
        </el-table-column>
        <el-table-column
          label="状态"
          prop="Status"
          :width="150"
        ></el-table-column>
      </el-table>
    </el-dialog>
    <el-dialog
      v-if="DataPartitionDialogVisible"
      title="坏DP"
      width="65%"
      :visible.sync="DataPartitionDialogVisible"
      center
      top="5vh"
    >
      <div>缺少副本的分区</div>
      <el-table
        max-height="350"
        :data="LackReplicaDataPartitionIDs"
        style="width: 100%"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 2)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
      <div>缺少leader的分区</div>
      <el-table
        max-height="300"
        :data="CorruptDataPartitionIDs"
        style="margin-top:5px"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="副本数"
          prop="ReplicaNum"
        ></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 2)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
    </el-dialog>
    <el-dialog
      v-if="MetaPartitionDialogVisible"
      title="坏MP"
      width="65%"
      :visible.sync="MetaPartitionDialogVisible"
      center
      top="5vh"
    >
      <div>缺少副本的分区</div>
      <el-table
        max-height="350"
        :data="LackReplicaMetaPartitionIDs"
        style="width: 100%"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 1)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
      <div>缺少leader的分区</div>
      <el-table
        max-height="300"
        :data="CorruptMetaPartitionIDs"
        style="margin-top:5px"
      >
        <el-table-column
          label="序号"
          type="index"
        >
        </el-table-column>
        <el-table-column
          label="分区ID"
          prop="id"
        >
          <template slot-scope="scope">
            <div>{{ scope.row }}</div>
          </template></el-table-column>
        <el-table-column
          label="操作"
        >
          <template slot-scope="scope">
            <el-button
              size="medium"
              type="text"
              @click="showDetail(scope.row, 1)"
            >详情</el-button>
          </template>
          ></el-table-column>
      </el-table>
      <!-- <span>点击查看详情</span>
      <div><a v-for="item in badDataPartition" :key="item" style="margin-left:5px" @click="showDetail(item)">{{ item }}</a></div> -->
      <!-- <o-page-table
        :columns="tableColumns"
        :data="tableData"
        :has-page="false"
      >
      </o-page-table> -->
    </el-dialog>
    <el-dialog
      v-if="MetaPartitionDetailDialogVisible"
      title="坏MP详情"
      width="65%"
      :visible.sync="MetaPartitionDetailDialogVisible"
      center
    >
      <el-table
        :data="metaPartitionTableData"
        style="width: 100%"
      >
        <el-table-column
          label="分区ID"
          prop="PartitionID"
          :width="80"
        ></el-table-column>
        <el-table-column label="卷名" prop="VolName"></el-table-column>
        <el-table-column label="Start" prop="Start"></el-table-column>
        <el-table-column label="End" prop="End"></el-table-column>
        <el-table-column
          :width="120"
          label="DentryCount"
          prop="DentryCount"
        ></el-table-column>
        <el-table-column
          :width="120"
          label="InodeCount"
          prop="InodeCount"
        ></el-table-column>
        <el-table-column
          :width="120"
          label="MaxInodeID"
          prop="MaxInodeID"
        ></el-table-column>
        <el-table-column label="isRecovering" :width="80">
          <template slot-scope="scope">
            <span>{{ scope.row.IsRecover }}</span>
          </template>
        </el-table-column>
        <el-table-column
          label="Leader"
          prop="Leader"
          :width="150"
        ></el-table-column>
        <el-table-column label="Members" prop="Members" :width="150">
          <template slot-scope="scope">
            <div v-for="item in scope.row.Members" :key="item">{{ item }}</div>
          </template>
        </el-table-column>
        <el-table-column label="状态" prop="Status" :width="90"></el-table-column>
      </el-table>
    </el-dialog>
  </div>
</template>
<script>
import { getDataNodeList, getBadDataP, getBadMetaP, getMetaNodeList, getDataNodeDiskList, getDataPartitionList, getMetaPartitionList, batchOfflineDisk } from '@/api/cfs/cluster'
import { mapGetters } from 'vuex'
import CircleCharts from '@/components/circleCharts.vue'
import { toByte, renderSize } from '@/utils'
export default {
  name: '',
  components: { CircleCharts },
  props: {
    clusterInfo: {
      type: Object,
      default() {
        return {}
      },
    },
  },
  data() {
    return {
      chartData: [],
      mpDpchartData: [],
      dataPartition: 0,
      metaPartition: 0,
      badDataPartitionNum: 0,
      badMetaPartitionNum: 0,
      diskNum: 0,
      badDiskNum: 0,
      badDataPartition: [],
      badMetaPartition: [],
      DiskDialogVisible: false,
      DataPartitionDialogVisible: false,
      DataPartitionDetailDialogVisible: false,
      MetaPartitionDialogVisible: false,
      MetaPartitionDetailDialogVisible: false,
      PartitionTableData: [],
      MetaPartitionTableData: [],
      LackReplicaDataPartitionIDs: [],
      CorruptDataPartitionIDs: [],
      LackReplicaMetaPartitionIDs: [],
      CorruptMetaPartitionIDs: [],
      diskTableData: [
        {
          PartitionIDs: [
            7569,
            7567,
          ],
          Path: '10.52.136.208:17310:/home/service/var/data1',
        },
      ],
      seletedDisk: [],
    }
  },
  computed: {
    ...mapGetters('clusterInfoModule', {
      curClusterInfo: 'clusterInfog',
    }),
    tableColumns() {
      return [
        {
          title: 'Path',
          key: 'diskPath',
        },
        {
          title: 'id',
          key: 'id',
        },
      ]
    },
  },
  watch: {},
  async created() {
    await this.getDataPartition()
    await this.getMetaPartition()
    this.calcMpDpData()
    this.getMpDpStatus()
  },
  beforeMount() { },
  mounted() { },
  methods: {
    mpDpChartValueFormat(value) {
      return renderSize(value)
    },
    newArrFn(arr) {
      const newArr = []
      for (let i = 0; i < arr.length; i++) {
        newArr.indexOf(arr[i]) === -1 ? newArr.push(arr[i]) : newArr
      }
      return newArr
    },
    async getDataPartition() {
      const res = await getDataNodeList({
        cluster_name: this.curClusterInfo.clusterName,
      })
      const tempData = res.data
      let normalNode = 0
      let badNode = 0
      for (let i = 0; i < tempData.length; i++) {
        if (tempData[i].status === 'Active') {
          normalNode++
        } else {
          badNode++
        }
        const res = await getDataNodeDiskList({ addr: tempData[i].addr, cluster_name: this.curClusterInfo.clusterName })
        this.diskNum += res.data.length
        this.badDiskNum += tempData[i].bad_disks?.length || 0
        this.dataPartition += tempData[i].partition_count
      }
      this.chartData.push({ data: [{ name: '正常', value: this.diskNum - this.badDiskNum }, { name: '损坏', value: this.badDiskNum }], title: '磁盘状态' })
      this.chartData.push({ data: [{ name: '正常', value: normalNode }, { name: '异常', value: badNode }], title: '数据节点数量' })
    },
    async getMetaPartition() {
      const res = await getMetaNodeList({
        cluster_name: this.curClusterInfo.clusterName,
      })
      const tempData = res.data
      let normalNode = 0
      let badNode = 0
      tempData.forEach(item => {
        if (item.status === 'Active') {
          normalNode++
        } else {
          badNode++
        }
        this.metaPartition += item.partition_count
      })
      this.chartData.push({ data: [{ name: '正常', value: normalNode }, { name: '异常', value: badNode }], title: '元数据节点数量' })
    },
    async getMpDpStatus() {
      const res = await getBadDataP({ cluster_name: this.curClusterInfo.clusterName })
      this.LackReplicaDataPartitionIDs = res.data.LackReplicaDataPartitionIDs
      this.CorruptDataPartitionIDs = res.data.CorruptDataPartitionIDs
      this.badDataPartition = this.newArrFn(res.data.LackReplicaDataPartitionIDs.concat(res.data.CorruptDataPartitionIDs))
      this.badDataPartitionNum = this.badDataPartition.length
      this.diskTableData = res.data.BadDataPartitionIDs
      const res1 = await getBadMetaP({ cluster_name: this.curClusterInfo.clusterName })
      // this.LackReplicaMetaPartitionIDs = ['265', '266']
      this.LackReplicaMetaPartitionIDs = res1.data.LackReplicaMetaPartitionIDs
      this.CorruptMetaPartitionIDs = res1.data.CorruptMetaPartitionIDs
      this.badMetaPartition = this.newArrFn(res1.data.LackReplicaMetaPartitionIDs.concat(res1.data.CorruptMetaPartitionIDs))
      this.badMetaPartitionNum = this.badMetaPartition.length
      // this.chartData.push({ data: [{ name: '正常', value: this.dataPartition - this.badDataPartitionNum }, { name: '损坏', value: this.badDataPartitionNum }], title: 'DP状态' })
      // this.chartData.push({ data: [{ name: '正常', value: this.metaPartition - this.badMetaPartitionNum }, { name: '损坏', value: this.badMetaPartitionNum }], title: 'MP状态' })
    },
    calcMpDpData() {
      const { data_total: dataTotal, data_used: dataUsed, meta_total: metaTotal, meta_used: metaUsed } = this.curClusterInfo.clusterInfo

      const calcData = (total, used) => {
        const unitTotal = total.match(/[a-z|A-Z]+/gi)[0]
        const unitUsed = used.match(/[a-z|A-Z]+/gi)[0]
        const newTotal = toByte(Number(total.match(/\d+(\.\d+)?/gi)[0]), unitTotal)
        const newUsed = toByte(Number(used.match(/\d+(\.\d+)?/gi)[0]), unitUsed)
        return {
          totol: newTotal,
          used: newUsed,
          noUsed: newTotal - newUsed,
        }
      }
      const dataInfo = calcData(dataTotal, dataUsed)
      const metaInfo = calcData(metaTotal, metaUsed)
      this.mpDpchartData.push({ data: [{ name: '已使用', value: dataInfo.used }, {name: '未使用', value: dataInfo.noUsed}], title: '数据使用率' })
      this.mpDpchartData.push({ data: [{ name: '已使用', value: metaInfo.used }, {name: '未使用', value: metaInfo.noUsed}], title: '元数据使用率' })
    },
    showDialog(name) {
      if (name === '磁盘状态') {
        this.DiskDialogVisible = true
      }
      if (name === 'DP状态') {
        this.DataPartitionDialogVisible = true
      }
      if (name === 'MP状态') {
        this.MetaPartitionDialogVisible = true
      }
    },
    async showDetail(id, type) {
      if (type === 2) {
        this.DataPartitionDetailDialogVisible = true
        const res = await getDataPartitionList({
          id,
          cluster_name: this.curClusterInfo.clusterName,
        })
        this.PartitionTableData = res.data
      } else {
        this.MetaPartitionDetailDialogVisible = true
        const res = await getMetaPartitionList({
          id,
          cluster_name: this.curClusterInfo.clusterName,
        })
        this.metaPartitionTableData = res.data
      }
    },
    handleSelectionDiskData(val) {
      this.seletedDisk = val
    },
    async batchOfflineDisk() {
      if (!this.seletedDisk.length) {
        this.$message.warning('请至少勾选一个需要下线的磁盘')
      }
      // await batchOfflineDisk(params)
    },
  },
}
</script>
<style lang='scss' scoped>
.height {
  height: 600px;
  width: 100%;
}
</style>
