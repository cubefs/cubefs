<template>
  <div>
    <el-row>
      <el-col :span="24">
        <el-row class="search">
          <el-input
            v-model.trim="inputParams"
            placeholder="请输入分区ID"
            clearable
            class="input"
          ></el-input>
          <el-button
            type="primary"
            class="search-btn"
            @click="onsearch"
          >搜 索</el-button>
        </el-row>
      </el-col>
    </el-row>
    <el-row class="userInfo">
      <u-page-table :data="dataList" :page-size="page.per_page">
        <!-- <el-table-column label="序号" type="index"></el-table-column> -->
        <el-table-column
          label="分区ID"
          prop="partition_id"
          sortable
        ></el-table-column>
        <el-table-column
          label="卷名"
          prop="vol_name"
          sortable
        ></el-table-column>
        <el-table-column label="start" prop="start" sortable></el-table-column>
        <el-table-column label="end" prop="end" sortable></el-table-column>
        <el-table-column label="peers">
          <template slot-scope="scope">
            <div v-for="item in scope.row.peers" :key="item.id">
              {{ item.addr }}
            </div>
          </template>
        </el-table-column>
        <el-table-column label="操作">
          <template slot-scope="scope">
            <MoreOPerate :count="2">
              <el-button
                v-auth="'CFS_METAPARTITION_DECOMMISSION'"
                size="medium"
                type="text"
                @click="handleOffLine(scope.row)"
              >下线</el-button>
            </MoreOPerate>
          </template>
        </el-table-column>
      </u-page-table>
    </el-row>
  </div>
</template>
<script>
import { getMetaNodeInfoList, offLineMetaNodePartitions } from '@/api/cfs/cluster'
import MoreOPerate from '@/pages/components/moreOPerate'
import UPageTable from '@/pages/components/uPageTable'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  components: {
    MoreOPerate,
    UPageTable,
  },
  mixins: [Mixin],
  props: {
    curNode: {
      type: Object,
      default() {
        return {}
      },
    },
    // path: {
    //   type: String,
    //   default() {
    //     return '{}'
    //   },
    // },
  },
  data() {
    return {
      checkList: [],
      checkBoxStatusList: [],
      dataList: [],
      inputParams: '', // 输入查询
      page: {
        per_page: 5, // 页面大小
      },
    }
  },
  computed: {
    addr() {
      return this.curNode.addr || ''
    },
  },
  watch: {},
  created() {
    const { diskPath } = this.$route.query
    this.getData({ diskPath, id: this.inputParams })
  },
  methods: {
    onsearch() {
      this.getData({ id: this.inputParams })
    },
    OnCheckedChange() {
      if (!this.checkList.length) {
        // this.getData()
        this.dataList = [...this.originDataList]
      } else {
        this.dataList = this.originDataList.filter((item) => {
          return this.checkList.includes(item.status)
        })
      }
    },
    async handleOffLine({ partition_id }) {
      const nodeAddr = this.addr
      try {
        await this.$confirm(`确定要下线该分区(${partition_id})磁盘?`, '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        })
        await offLineMetaNodePartitions({
          cluster_name: this.clusterName,
          partitions: [{
            node_addr: nodeAddr || '',
            id: partition_id,
          }],
        })
        this.$message.success('下线成功')
        this.onsearch()
      } catch (e) {}
    },
    async getData({ id }) {
      this.dataList = []
      this.originDataList = []
      const res = await getMetaNodeInfoList({
        addr: this.addr,
        id,
      })
      const tempData = (res.data || []).sort((a, b) => {
        if (a.id < b.id) return -1
        if (a.id > b.id) return 1
        return 0
      })
      this.dataList = tempData
      this.originDataList = [...tempData]
      this.checkBoxStatusList = [
        ...new Set(tempData.map((item) => item.status) || []),
      ]
    },
  },
}
</script>
<style lang="scss" scoped>
.filter {
  padding-top: 12px;
}
.noborder {
  border: none;
}
.input {
  width: 300px;
}
.search {
  position: relative;
  text-align: right;
}
.userInfo {
  margin-bottom: 40px;
}
.mr-l {
  color: #66cc99;
  cursor: pointer;
}
</style>
