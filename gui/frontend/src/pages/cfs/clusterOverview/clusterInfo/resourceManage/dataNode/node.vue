<template>
  <el-card class="container">
    <div class="searchPosition">
      <div>
        <FilterTableData
          ref="filterTableData"
          class="search"
          :data-list="originDataList"
          @filterData="filterData"
        ></FilterTableData>
      </div>
      <div>
        <el-row class="search">
          <el-input
            v-model.trim="searchData.node_set_id"
            placeholder="请输入nodesetid"
            clearable
            class="input"
          ></el-input>
          <el-input
            v-model.trim="searchData.inputParams"
            placeholder="请输入节点IP"
            clearable
            class="input"
          ></el-input>
          <el-button
            type="primary"
            class="search-btn"
            @click="onSearchClick"
          >搜 索</el-button>
        </el-row>
      </div>
    </div>
    <u-page-table :data="dataList" :page-size="page.per_page" sortable="custom" @sort-change="changeSort">
      <el-table-column
        label="节点ID"
        prop="id"
        sortable
        :width="100"
      ></el-table-column>
      <el-table-column label="节点地址">
        <template slot-scope="scope">
          <a @click="showDrawer(scope.row)">{{ scope.row.addr }}</a>
        </template>
      </el-table-column>
      <el-table-column label="Zone" prop="zone_name"></el-table-column>
      <el-table-column
        label="总量"
        prop="total"
        sortable
        :sort-method="sortMethodTotal"
        :width="100"
      ></el-table-column>
      <el-table-column
        label="剩余"
        prop="available"
        sortable
        :sort-method="sortMethodAvai"
        :width="100"
      ></el-table-column>
      <el-table-column
        label="已使用"
        prop="used"
        :width="120"
        sortable
        :sort-method="sortMethodUsed"
      ></el-table-column>
      <el-table-column
        label="使用率"
        prop="usage_ratio"
        sortable
        :width="120"
        :sort-method="sortMethodUsedRatio"
      >
        <template slot-scope="scope">
          <!-- scope.row.size / scope.row.used -->
          <span>{{ scope.row.usage_ratio }}</span>
          <el-progress
            :show-text="false"
            :percentage="Number(scope.row.usage_ratio.replace(/%/g, ''))"
            :color="[
              { color: '#f56c6c', percentage: 100 },
              { color: '#e6a23c', percentage: 80 },
              { color: '#5cb87a', percentage: 60 },
              { color: '#1989fa', percentage: 40 },
              { color: '#6f7ad3', percentage: 20 },
            ]"
          >
          </el-progress> </template>
      </el-table-column>
      <el-table-column label="分区数" prop="partition_count" sortable :width="110">
        <template slot-scope="scope">
          <a @click="showDrawer(scope.row, 'partitionList')">{{ scope.row.partition_count }}</a>
        </template>
      </el-table-column>
      <el-table-column label="节点状态" prop="status" :width="90"></el-table-column>
      <el-table-column label="读写状态" prop="writable_str" :width="100">
      </el-table-column>
      <el-table-column label="更新时间" prop="report_time" sortable width="100">
        <template slot-scope="scope">
          <span>{{ scope.row.report_time | fFormatDate }}</span>
        </template>
      </el-table-column>
      <el-table-column label="nodesetid" prop="node_set_id" sortable :width="110"></el-table-column>
      <el-table-column label="操作" :width="150">
        <template slot-scope="scope">
          <MoreOPerate :count="2">
            <el-button
              v-auth="'CFS_DATANODE_DECOMMISSION'"
              size="medium"
              type="text"
              @click="handleOffLine(scope.row)"
            >下线</el-button>
            <el-button
              v-auth="'CFS_DATANODE_MIGRATE'"
              size="medium"
              type="text"
              @click="openNodeMigrateModal(scope.row)"
            >迁移</el-button>
          </MoreOPerate>
        </template>
      </el-table-column>
    </u-page-table>
    <node-migrate ref="NodeMigrate" :address-list="dataList.map(item => item.addr)" @refresh="getData"></node-migrate>
    <el-drawer :destroy-on-close="true" :visible.sync="drawer" size="1000px">
      <div slot="title" class="fontType">
        节点详情
      </div>
      <div class="fontTypeSpan">

        <span
          class="mg-lf-m"
        ><span>addr:</span><span class="mg-lf-m">{{ curNode.addr }}</span></sp>
          <span
            class="mg-lf-m"
          ><span>卷状态:</span><span class="mg-lf-m">{{ curNode.status }}</span></span>
          <span
            class="mg-lf-m"
          ><span>writable:</span><span class="mg-lf-m">{{ curNode.writable }}</span></span>
          <span
            class="mg-lf-m"
          ><span>总空间:</span><span class="mg-lf-m">{{ curNode.total }}</span></span>
          <span
            class="mg-lf-m"
          ><span>已使用:</span><span class="mg-lf-m">{{ curNode.used }}</span></span>
          <span
            class="mg-lf-m"
          ><span>使用率:</span><span class="mg-lf-m">{{ curNode.usage_ratio }}</span></span>
        </span></div>
      <el-tabs v-model="activeName" class="inside">
        <el-tab-pane
          v-for="item in tabs"
          :key="item.name"
          :label="item.label"
          :name="item.name"
        >
          <component
            :is="item.component"
            v-if="item.name === activeName"
            show-type="darwerPosition"
            :cur-node="curNode"
            :path="path"
            @pathChange="pathChange"
          />
        </el-tab-pane>
      </el-tabs></el-drawer>
  </el-card>
</template>
<script>
import MoreOPerate from '@/pages/components/moreOPerate'
import { offLineDataNodes } from '@/api/cfs/cluster'
import UPageTable from '@/pages/components/uPageTable'
import FilterTableData from '@/pages/components/filter'
import { formatDate, sortSizeWithUnit } from '@/utils'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
import NodeMixin from '@/pages/cfs/clusterOverview/clusterInfo/mixin'
import DiskList from './dataNodeInfo.vue'
import PartitionList from './dataNodePartitionInfo.vue'
import NodeMigrate from '@/pages/cfs/clusterOverview/clusterInfo/components/NodeMigrate.vue'
export default {
  components: {
    MoreOPerate,
    UPageTable,
    FilterTableData,
    PartitionList,
    DiskList,
    NodeMigrate,
  },
  filters: {
    fFormatDate(v) {
      return formatDate(v)
    },
  },
  mixins: [Mixin, NodeMixin],
  props: {
    info: {
      type: Object,
      default() {
        return {
          node: 0,
          partition: 0,
          total: 0,
          used: 0,
        }
      },
    },
  },
  data() {
    return {
      dataList: [],
      originDataList: [],
      nodeTotal: 0,
      errorNodeTotal: 0,
      searchData: {
        inputParams: '', // 输入查询
        node_set_id: '',
      },
      page: {
        per_page: 5, // 页面大小
      },
      drawer: false,
      curNode: {},
      activeName: 'diskList',
      tabs: [
        {
          label: '磁盘列表',
          name: 'diskList',
          component: 'DiskList',
        },
        {
          label: '分区列表',
          name: 'partitionList',
          component: 'PartitionList',
        }],
      path: '',
      sort: 'asc',
    }
  },
  computed: {},
  watch: {},
  created() {
    this.refresh()
  },
  methods: {
    onSearchClick() {
      this.$refs.filterTableData.clear()
      this.getData()
    },
    sortMethodUsedRatio(a, b) {
      const ausage_ratio = parseFloat(a.usage_ratio)
      const busage_ratio = parseFloat(b.usage_ratio)
      if (ausage_ratio < busage_ratio) return -1
      if (ausage_ratio > busage_ratio) return 1
      return 0
    },
    sortMethodTotal(a, b) {
      return sortSizeWithUnit(a.total, b.total)
    },
    sortMethodAvai(a, b) {
      return sortSizeWithUnit(a.available, b.available)
    },
    sortMethodUsed(a, b) {
      return sortSizeWithUnit(a.used, b.used)
    },
    async refresh() {
      this.searchData = {
        inputParams: '',
        node_set_id: '',
      }
      this.nodeTotal = await this.getData()
    },
    filterData(data) {
      this.dataList = [...data]
    },
    async getData() {
      this.dataList = []
      if (this.clusterName) {
        const tempData = await this.getDataNodeList({
          node_set_id: this.searchData.node_set_id,
          node: this.searchData.inputParams.trim(),
          cluster_name: this.clusterName,
          sort: this.sort,
        })
        this.dataList = tempData
        const info = this.countDataNodeInfo(tempData)
        this.$emit('update:info', info)
        this.originDataList = [...tempData]
        this.errorNodeTotal = tempData.filter(
          (item) => item.status !== 'Active',
        ).length
        return tempData.length || 0
      } else {
        this.$router.push('/file-two/cluster/')
      }
    },
    changeSort({ column, prop, order }) {
      if (prop === 'node_set_id') {
        this.sort = order === 'descending' ? 'desc' : 'asc'
        this.getData()
      }
    },
    async handleOffLine({ addr }) {
      try {
        await this.$confirm(`确定要下线该节点(${addr})?`, '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        })
        await offLineDataNodes({ addrs: [addr], cluster_name: this.clusterName })
        this.$message.success('下线成功')
        this.refresh()
      } catch (e) { }
    },
    showDrawer(row, type) {
      this.drawer = true
      this.curNode = row
      this.activeName = type || 'diskList'
    },
    pathChange(path) {
      this.path = path
      this.activeName = 'partitionList'
    },
    openNodeMigrateModal(row) {
      this.$refs.NodeMigrate.init({
        src_addr: row.addr,
      })
    },
  },
}
</script>
<style lang="scss" scoped>
.link {
  color: #66cc99;
  cursor: pointer;
}

.container {
  position: relative;
}

.noborder {
  border: none;
}

.infoBox {
  width: 40%;
  display: flex;
}

// .right{
//     position: relative;
//     text-align: right;
// }
.inside {
  margin: 10px;
}

.input {
  width: 200px;
  margin-right: 20px
}

.search {
  position: relative;
  display: flex;
  align-items: center;
}

.searchPosition {
  // position: absolute;
  // width: 55%;
  // right: 0px;
  // top: -40px;
  display: flex;
  justify-content: space-between;
}

.fontType {
  font-family: 'OPPOSans B';
  font-style: normal;
  font-weight: 300;
  font-size: 22px;
  line-height: 24px;
  /* or 100% */
  color: #000000;
}

.fontTypeSpan {
  font-family: 'OPPOSans M';
  font-style: normal;
  font-weight: 400;
  font-size: 16px;
  line-height: 24px;
}

::v-deep .el-drawer__body {
  overflow: auto;
}

/*2.隐藏滚动条，太丑了*/
::v-deep .el-drawer__container ::-webkit-scrollbar {
  display: none;
}
</style>
