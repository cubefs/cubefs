import { getDataNodeList, getBadDataP, getBadMetaP, getMetaNodeList, getDataNodeDiskList, getDataPartitionList, getMetaPartitionList, batchOfflineDisk } from '@/api/cfs/cluster'
import { mapGetters } from 'vuex'
import { toByte, renderSize } from '@/utils'
export default {
  name: '',
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
      this.chartData.push({ data: [{ name: '正常', value: this.dataPartition - this.badDataPartitionNum }, { name: '损坏', value: this.badDataPartitionNum }], title: 'DP状态' })
      this.chartData.push({ data: [{ name: '正常', value: this.metaPartition - this.badMetaPartitionNum }, { name: '损坏', value: this.badMetaPartitionNum }], title: 'MP状态' })
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
