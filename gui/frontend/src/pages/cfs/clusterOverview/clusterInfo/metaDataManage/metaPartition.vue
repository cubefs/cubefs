<template>
  <el-card class="container">
    <div class="mg-bt-s flex">
      <span class="fontType"><span>总节点数:</span> <span class="mg-lf-m"></span>{{ info.node }}</span>
      <span class="fontType mg-lf-m"><span>总分区数:</span> <span class="mg-lf-m"></span>{{ info.partition }}</span>
      <span class="fontType mg-lf-m"><span>总容量:</span> <span class="mg-lf-m"></span>{{ info.total | renderSize }}</span>
      <div class="mg-lf-m progress">
        <span>{{ info.used |renderSize }}/{{ (isNaN(info.used/info.total*100) ? 0 : info.used/info.total*100).toFixed(0)+'%' }}</span>
        <el-progress
          v-if="info.node!==0"
          :stroke-width="10"
          :show-text="false"
          :percentage="info.used/info.total*100 || 0"
          :color="[
            { color: '#f56c6c', percentage: 100 },
            { color: '#e6a23c', percentage: 80 },
            { color: '#5cb87a', percentage: 60 },
            { color: '#1989fa', percentage: 40 },
            { color: '#6f7ad3', percentage: 20 },
          ]"
        >
        </el-progress>
      </div>
    </div>
    <div style="display:flex;flex-direction: row-reverse;">
      <div class="darwerPosition">
        <div class="form-wrap">
          <FilterTableData
            ref="filterTableData"
            :data-list="originDataList"
            :types="['STATUS', 'RECOVER']"
            @filterData="filterData"
            style="flex: 0 0 auto; margin-right: 20px;"
          ></FilterTableData>
          <div class="search">
            <el-radio-group v-model="radio" @change="onChangeRadio">
              <el-radio label="vol">
                <span class="label">卷名</span>
                <el-select
                  v-model="params.name"
                  filterable
                  placeholder="请选择卷"
                  class="input"
                  :disabled="radio !== 'vol'"
                  @change="onSelectChange"
                >
                  <el-option
                    v-for="item in volNameList"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value"
                  >
                  </el-option>
                </el-select>
              </el-radio>
              <el-radio label="id">
                <span class="label">分区ID</span>
                <el-input
                  v-model.trim="params.zoneId"
                  placeholder="请输入分区ID"
                  clearable
                  class="input"
                  :disabled="radio === 'vol'"
                  @keyup.native.enter="getData"
                ></el-input></el-radio>
            </el-radio-group>
            <el-button
              type="primary"
              class="search-btn"
              @click="onSearchClick"
            >搜 索</el-button>
            <el-button
              type="primary"
              class="search-btn"
              @click="onExportClick"
            >导 出</el-button>
          </div>
        </div>
      </div>
    </div>
    <!-- 分页参数预留可能后端会分页,目前前端分页 -->
    <u-page-table :data="dataList" :page-size="page.per_page">
      <!-- <el-table-column label="序号" type="index"></el-table-column> -->
      <el-table-column
        label="分区ID"
        prop="PartitionID"
        :width="90"
        sortable
      ></el-table-column>
      <el-table-column label="卷名" prop="VolName" sortable></el-table-column>
      <el-table-column label="Start" prop="Start" sortable :width="110"></el-table-column>
      <el-table-column label="End" prop="End" sortable :width="110"></el-table-column>
      <el-table-column
        :width="130"
        label="DentryCount"
        prop="DentryCount"
        sortable
      ></el-table-column>
      <el-table-column
        :width="130"
        label="InodeCount"
        prop="InodeCount"
        sortable
      ></el-table-column>
      <el-table-column
        :width="130"
        label="MaxInodeID"
        prop="MaxInodeID"
        sortable
      ></el-table-column>
      <el-table-column label="isRecovering" :width="120">
        <template slot-scope="scope">
          <span>{{ scope.row.IsRecover }}</span>
        </template>
      </el-table-column>
      <el-table-column
        label="Leader"
        prop="Leader"
        :width="150"
      ></el-table-column>
      <el-table-column label="Members" prop="Members" :width="200">
        <template slot-scope="scope">
          <div v-for="item in scope.row.Members" :key="item">{{ item }}</div>
        </template>
      </el-table-column>
      <el-table-column label="状态" prop="status" :width="90"></el-table-column>
      <el-table-column
        label="操作"
        :width="120"
        align="center"
        fixed="right"
      >
        <template slot-scope="scope">
          <MoreOPerate :count="2">
            <!-- <el-button
              size="medium"
              type="text"
              @click="handleLoad(scope.row)"
            >load</el-button> -->
            <el-button
              size="medium"
              type="text"
              @click="handleDetail(scope.row)"
            >inode详情</el-button>
          </MoreOPerate>
        </template>
      </el-table-column>
    </u-page-table>
    <Detail ref="detail" />
  </el-card>
</template>
<script>
import MoreOPerate from '@/pages/components/moreOPerate'
import UPageTable from '@/pages/components/uPageTable'
import FilterTableData from '@/pages/components/filter'
import Detail from './components/Detail'
import { renderSize } from '@/utils'
import {
  getMetaPartitionList,
  getVolList,
  loadMetaPartition,
} from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
import NodeMixin from '@/pages/cfs/clusterOverview/clusterInfo/mixin'
export default {
  components: {
    MoreOPerate,
    UPageTable,
    FilterTableData,
    Detail,
  },
  filters: {
    renderSize(val) {
      const data = renderSize(val, 1)
      return data
    },
  },
  mixins: [Mixin, NodeMixin],
  props: {
    showType: {
      type: String,
      // 对象或数组默认值必须从一个工厂函数获取
      default: function () {
        return 'normalPosition'
      },

    },
    curVol: {
      type: Object,
      default: function () {
        return {}
      },
    },
  },
  data() {
    return {
      radio: 'vol',
      checkList: [],
      volNameList: [],
      checkBoxStatusList: [],
      dataList: [],
      originDataList: [],
      params: {
        name: '',
        zoneId: '',
      }, // 输入查询
      page: {
        per_page: 5, // 页面大小
      },
      info: {
        node: 0,
        partition: 0,
        total: 0,
        used: 0,
      },
    }
  },
  computed: {},
  watch: {},
  async created() {
    await this.getVolList()
    this.initVolName()
    this.getData()
    this.getNodeData()
  },
  methods: {
    async handleDetail({ PartitionID }) {
      const res = await getMetaPartitionList({
        id: PartitionID,
        cluster_name: this.clusterName,
      })
      this.$refs.detail.init(res?.data?.[0] || {})
    },
    refresh() {
      // this.resetParam()
      this.getData()
    },
    onSearchClick() {
      this.$refs.filterTableData.clear()
      this.getData()
    },
    resetParam() {
      this.params = {
        name: '',
        zoneId: '',
      }
    },
    onChangeRadio(v) {
      if (v === 'vol') {
        this.params.name = this.curVol.name || this.volNameList[0]?.value
        this.params.zoneId = ''
        this.getData()
      } else {
        this.params.name = ''
      }
    },
    async getVolList() {
      if (this.clusterName) {
        const volList = await getVolList({
          keywords: '',
          cluster_name: this.clusterName,
        })
        this.volNameList = (volList.data || []).map((item) => {
          return { label: item.name, value: item.name }
        })
      } else {
        this.$router.push('/cfs-front/cluster/')
      }
    },
    initVolName() {
      const volName = this.$route.query.volName || this.curVol.name
      this.params.name = volName || this.volNameList[0]?.value || ''
    },
    async getNodeData() {
      const data = await this.getMetaNodeList({
        cluster_name: this.clusterName,
      })
      this.info = this.countMetaNodeInfo(data)
    },
    async getData() {
      this.dataList = []
      this.originDataList = []
      const { name, zoneId } = this.params
      const flag = this.radio === 'vol'
      const res = await getMetaPartitionList({
        vol_name: flag ? name : '',
        id: !flag ? zoneId : '',
        cluster_name: this.clusterName,
      })
      const temp = (res.data || [])
        .map((item) => {
          return {
            ...item,
            status: item.Status,
          }
        })
        .sort((a, b) => {
          if (a.PartitionID < b.PartitionID) return -1
          if (a.PartitionID > b.PartitionID) return 1
          return 0
        })
      this.dataList = temp
      this.originDataList = [...temp]
    },
    onExportClick() {
      const XLSX = require('xlsx')
      const exportData = this.dataList.map(item => ({
        分区ID: item.PartitionID,
        卷名: item.VolName,
        Start: item.Start,
        End: item.End,
        DentryCount: item.DentryCount,
        InodeCount: item.InodeCount,
        MaxInodeID: item.MaxInodeID,
        isRecovering: item.IsRecover,
        Leader: item.Leader,
        Members: item.Members.join(),
        状态: item.Status,
      }))
      const wb = XLSX.utils.book_new()
      const ws = XLSX.utils.json_to_sheet(exportData)
      XLSX.utils.book_append_sheet(wb, ws, 'Sheet1')
      XLSX.writeFile(wb, 'data.xlsx')
    },
    filterData(data) {
      this.dataList = [...data]
    },
    onSelectChange() {
      this.getData()
    },
    async handleLoad({ PartitionID }) {
      await loadMetaPartition({
        id: PartitionID,
        cluster_name: this.clusterName,
      })
      this.$message.success('操作成功')
    },
  },
}
</script>
<style lang="scss" scoped>
.form-wrap {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  justify-content: flex-end;
}
.noborder {
  border: none;
}

.input {
  width: 150px;
}

.search {
  position: relative;
  text-align: right;
  flex: 0 0 auto;
}

.search-btn {}

.filter {
  padding-top: 12px;
}

::v-deep .el-checkbox,
.el-radio {
  margin-right: 10px;
}

.normalPosition {
  position: absolute;
  width: 65%;
  right: 0px;
  top: -40px;
  display: flex;
  justify-content: space-between;
}

.container {
  position: relative;
}

.darwerPosition {
  display: flex;
  justify-content: space-between;
}
</style>
