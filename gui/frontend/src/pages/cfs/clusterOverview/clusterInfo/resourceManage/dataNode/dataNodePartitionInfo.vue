<template>
  <div>
    <el-row class="filter-wrap">
      <div style="margin-right: 10px;">
        <el-row class="filter">
          <span style="margin-right: 10px">过滤选项</span>
          <el-checkbox-group v-model="checkList" style="margin-right: 10px;" @change="OnCheckedChange">
            <el-checkbox
              v-for="label in checkBoxStatusList"
              :key="label"
              :label="label"
            ></el-checkbox>
          </el-checkbox-group>
          <div v-auth="'CFS_DATAPARTITION_DECOMMISSION'">
            <span>批量操作</span>
            <el-select v-model="operateType" style="width: 100px;margin: 0 20px 0 10px;flex: 1 0 100px;">
              <el-option :value="1" label="下线"></el-option>
            </el-select>
            <el-button type="primary" @click="batchOperate">执行</el-button>
          </div>
        </el-row>
      </div>
      <el-row class="search">
        <el-input
          v-model.trim="inputParams"
          placeholder="请输入分区ID或磁盘路径"
          clearable
          class="input"
        ></el-input>
        <el-button
          type="primary"
          class="search-btn"
          @click="onsearch"
        >搜 索</el-button>
      </el-row>
    </el-row>
    <el-row class="userInfo">
      <u-page-table :data="dataList" :page-size="page.per_page" @selection-change="handleSelectionChange">
        <!-- <el-table-column label="序号" type="index"></el-table-column> -->
        <el-table-column type="selection" width="80"></el-table-column>
        <el-table-column label="分区ID" prop="id" sortable></el-table-column>
        <el-table-column
          label="磁盘路径"
          prop="path"
          sortable
        ></el-table-column>
        <el-table-column
          label="容量"
          prop="size"
          sortable
          :sort-method="sortMethodTotal"
        ></el-table-column>
        <el-table-column
          label="已使用"
          prop="used"
          sortable
          :sort-method="sortMethodUsed"
        ></el-table-column>
        <el-table-column label="副本">
          <template slot-scope="scope">
            <div v-for="item in scope.row.replicas" :key="item">{{ item }}</div>
          </template>
        </el-table-column>
        <el-table-column label="状态" prop="status"></el-table-column>
        <el-table-column label="操作">
          <template slot-scope="scope">
            <MoreOPerate :count="2">
              <el-button
                v-auth="'CFS_DATAPARTITION_DECOMMISSION'"
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
import {
  getDataNodePartitionList,
  offLineDataNodePartitions,
} from '@/api/cfs//cluster'
import MoreOPerate from '@/pages/components/moreOPerate'
import UPageTable from '@/pages/components/uPageTable'
import { sortSizeWithUnit } from '@/utils'
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
    path: {
      type: String,
      default() {
        return ''
      },
    },
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
      selectedData: [],
      operateType: 1,
    }
  },
  computed: {
    addr() {
      return this.curNode.addr || ''
    },
  },
  watch: {},
  created() {
    const diskPath = this.path
    this.getData({ diskPath, id: this.inputParams })
  },
  methods: {
    sortMethodTotal(a, b) {
      return sortSizeWithUnit(a.size, b.size)
    },
    sortMethodUsed(a, b) {
      return sortSizeWithUnit(a.used, b.used)
    },
    onsearch() {
      const { diskPath } = this.$route.query
      const isNubmer =
        parseInt(this.inputParams).toString() === this.inputParams
      const params = {
        diskPath: isNubmer ? diskPath : this.inputParams,
        id: isNubmer ? this.inputParams : '',
      }
      this.getData(params)
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
    async getData({ id, diskPath }) {
      this.dataList = []
      this.originDataList = []
      const res = await getDataNodePartitionList({
        addr: this.addr,
        id,
        disk_path: diskPath,
        cluster_name: this.clusterName,
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
      this.$emit('pathChange', '')
    },
    async handleOffLine({ id }) {
      try {
        await this.$confirm(`确定要下线该分区(${id})磁盘?`, '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        })
        await offLineDataNodePartitions({
          cluster_name: this.clusterName,
          partitions: [{
            node_addr: this.addr || '',
            id,
          }],
        })
        this.$message.success('下线成功')
        this.onsearch()
      } catch (e) { }
    },
    handleSelectionChange(val) {
      this.selectedData = val
    },
    async batchOperate() {
      if (!this.operateType) {
        this.$message.warning('请选择操作')
        return
      }
      if (!this.selectedData.length) {
        this.$message.warning('请至少勾选一项')
        return
      }
      try {
        await this.$confirm('确定要批量下线吗?', '提示', { type: 'warning' })
        const partitions = this.selectedData.map(item => ({ node_addr: this.addr || '', id: item.id }))
        await offLineDataNodePartitions({
          cluster_name: this.clusterName,
          partitions,
        })
        this.$message.success('下线成功')
        this.getData({ diskPath: this.path, id: this.inputParams })
      } catch (e) { }
    },
  },
}
</script>
<style lang="scss" scoped>
.filter-wrap {
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.filter {
  display: flex;
  align-items: center;
}

.noborder {
  border: none;
}

.input {
  width: 250px;
  margin-right: 20px;
}

.search {
  display: flex;
  align-items: center;
}

.userInfo {
  margin-bottom: 40px;
}

.mr-l {
  color: #66cc99;
  cursor: pointer;
}
</style>
