<template>
  <el-card>
    <el-row class="form-wrap">
      <FilterTableData
        ref="filterTableData"
        :data-list="originDataList"
        :types="['STATUS', 'USEDRATIO']"
        style="flex: 0 0 auto; margin-right:20px;"
        @filterData="filterData"
      ></FilterTableData>
      <div class="search">
        <el-input
          v-model.trim="inputParams"
          placeholder="请输入卷名"
          clearable
          class="input"
        ></el-input>
        <el-button
          type="primary"
          class=""
          style="margin-left: 10px;"
          @click="onSearchClick"
        >搜 索</el-button>
        <el-button
          icon="el-icon-circle-plus"
          type="primary"
          @click.stop="download"
        >导出列表</el-button>
        <el-button
          v-auth="'CFS_VOLS_CREATE'"
          icon="el-icon-circle-plus"
          type="primary"
          @click.stop="createVol"
        >创建卷</el-button>
      </div>
    </el-row>
    <u-page-table :data="dataList" :page-size="page.per_page">
      <el-table-column label="序号" type="index" :width="80"></el-table-column>
      <el-table-column label="卷名" prop="name" sortable>
        <template slot-scope="scope">
          <a @click="showDrawer(scope.row)">{{ scope.row.name }}</a>
        </template>
      </el-table-column>
      <el-table-column label="卷类型" prop="vol_type">
        <template slot-scope="scope">
          {{ scope.row.vol_type | formatVolType }}
        </template>
      </el-table-column>
      <el-table-column
        label="EC模式"
        prop="dp_replica_num"
        :width="110"
      >
        <template slot-scope="scope">
          {{ codeMap[scope.row.dp_replica_num] }}
        </template>
      </el-table-column>
      <el-table-column label="owner租户" prop="owner" :width="120"></el-table-column>
      <el-table-column label="状态" prop="status" :width="80"></el-table-column>
      <el-table-column
        label="总容量"
        prop="total_size"
        sortable
        :width="90"
        :sort-method="sortMethodTotal"
      >
        <template slot-scope="scope">
          <span>{{ scope.row.total_size | renderSize }}</span>
        </template>
      </el-table-column>
      <el-table-column
        label="已使用"
        prop="used_size"
        sortable
        :width="90"
        :sort-method="sortMethodUsed"
      ></el-table-column>
      <el-table-column
        label="使用率"
        prop="usage_ratio"
        sortable
        :sort-method="sortMethodUsedRatio"
        :width="120"
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
      <el-table-column label="MP数" prop="mp_cnt" sortable :width="80">
        <template slot-scope="scope">
          <a @click="showDrawer(scope.row, 'metaPartition')">{{ scope.row.mp_cnt }}</a>

        </template>
      </el-table-column>
      <el-table-column label="DP数" prop="dp_cnt" sortable :width="80">
        <template slot-scope="scope">
          <a @click="showDrawer(scope.row, 'partition')">{{ scope.row.dp_cnt }}</a>
        </template>
      </el-table-column>
      <el-table-column
        label="inode数"
        prop="inode_count"
        sortable
        width="100"
      ></el-table-column>
      <el-table-column
        label="dentry数"
        prop="dentry_count"
        sortable
        width="110"
      ></el-table-column>
      <el-table-column
        label="创建时间"
        prop="create_time"
        :width="100"
        sortable
      ></el-table-column>
      <el-table-column label="业务" prop="business"></el-table-column>
      <el-table-column label="操作" :width="80">
        <template slot-scope="scope">
          <MoreOPerate
            :count="1"
            title="操作"
          >
            <el-button
              v-auth="'CFS_VOLS_EXPAND'"
              class="btn"
              size="medium"
              type="text"
              @click="handleExpansion(scope.row, 'expansion')"
            >扩容</el-button>
            <el-button
              v-auth="'CFS_VOLS_SHRINK'"
              class="btn"
              size="medium"
              type="text"
              @click="handleExpansion(scope.row, 'shrink')"
            >缩容</el-button>
            <el-button
              class="btn"
              size="medium"
              type="text"
              @click="handleAuth(scope.row)"
            >授权</el-button>
            <!-- <el-button
              size="medium"
              type="text"
              class="btn"
              @click="handleDp(scope.row)"
            >创建dp</el-button>
            <el-button
              size="medium"
              type="text"
              class="btn"
              @click="handleMp(scope.row)"
            >创建mp</el-button> -->
            <el-button
              v-auth="'CFS_VOLS_UPDATE'"
              size="medium"
              type="text"
              class="btn"
              @click="openUpdateVolumnModal(scope.row)"
            >
              更新卷
            </el-button>
          </MoreOPerate>
        </template>
      </el-table-column>
    </u-page-table>
    <Expansion ref="expansion" @refresh="refresh" />
    <CreateVol ref="createVol" @refresh="refresh" />
    <Auth ref="auth" @refresh="refresh" />
    <CreateDp ref="createDp" @refresh="refresh" />
    <CreateMp ref="createMp" @refresh="refresh" />
    <update-vol ref="updateVol" @refresh="refresh"></update-vol>
    <el-drawer :destroy-on-close="true" :visible.sync="drawer" size="1000px">
      <div slot="title" class="fontType">
        卷详情
      </div>
      <div class="fontTypeSpan">
        <span class="mg-lf-m">
          <span>卷名:</span>
          <span class="mg-lf-m">{{ curVol.name }}</span>
        </span>
        <span class="mg-lf-m">
          <span>卷状态:</span>
          <span class="mg-lf-m">{{ curVol.status }}</span>
        </span>
        <span
          class="mg-lf-m"
        ><span>业务:</span><span class="mg-lf-m">{{ curVol.business }}</span></span>
        <span
          class="mg-lf-m"
        ><span>总空间:</span><span class="mg-lf-m">{{ curVol.total_size | renderSize }}</span></span>
        <span
          class="mg-lf-m"
        ><span>已使用:</span><span class="mg-lf-m">{{ curVol.used_size }}</span></span>
        <span
          class="mg-lf-m"
        ><span>使用率:</span><span class="mg-lf-m">{{ curVol.usage_ratio }}</span></span>
        <el-row v-if="curVol.vol_type === 1">
          <span class="mg-lf-m">
            <span>低频卷cache容量大小:</span>
            <span class="mg-lf-m">{{ volDetail.CacheCapacity }}</span>
          </span>
          <span class="mg-lf-m">
            <span>低频卷cacheThreshold:</span>
            <span class="mg-lf-m">{{ volDetail.CacheThreshold }}</span>
          </span>
          <span class="mg-lf-m">
            <span>低频卷cache的淘汰时间:</span>
            <span class="mg-lf-m">{{ volDetail.CacheTtl }}</span>
          </span>
        </el-row>
      </div>
      <el-tabs
        v-model="activeName"
        class="inside"
        @tab-click="handleClick"
      >
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
            :cur-vol="curVol"
          />
        </el-tab-pane>
      </el-tabs>
    </el-drawer>
  </el-card>
</template>
<script>
import MoreOPerate from '@/pages/components/moreOPerate'
import UPageTable from '@/pages/components/uPageTable'
import Expansion from './components/expansion'
import CreateVol from './components/createVol'
import Auth from './components/auth'
import CreateDp from './components/createDp'
import CreateMp from './components/createMp'
import UpdateVol from './components/updateVol.vue'
import FilterTableData from '@/pages/components/filter'
import { getVolList, getVolDetail } from '@/api/cfs/cluster'
import { sortSizeWithUnit, renderSize, generateEXCEL, codeMap } from '@/utils'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
import Partition from '../partition.vue'
import MetaPartition from '../metaPartition.vue'
export default {
  components: {
    MoreOPerate,
    Expansion,
    CreateVol,
    Auth,
    CreateDp,
    CreateMp,
    UPageTable,
    FilterTableData,
    Partition,
    MetaPartition,
    UpdateVol,
  },
  filters: {
    renderSize(val) {
      const data1 = renderSize(val, 1)
      if (data1.includes('GB')) {
        const value = parseInt(data1)
        return value + 'GB'
      } else {
        return data1
      }
    },
    formatVolType(val) {
      return ['多副本卷', '纠删码卷'][val]
    },
  },
  mixins: [Mixin],
  data() {
    return {
      dataList: [],
      originDataList: [],
      inputParams: '', // 输入查询
      volTotal: 0,
      drawer: false,
      page: {
        //   page: 1, // 当前页
        per_page: 5, // 页面大小
        //   total: 0,
      },
      curVol: {},
      activeName: 'partition',
      volDetail: {},
      codeMap,
    }
  },
  computed: {
    tabs() {
      const tabs = [
        {
          label: '数据分区',
          name: 'partition',
          component: 'partition',
        },
        {
          label: '元数据分区',
          name: 'metaPartition',
          component: 'MetaPartition',
        }]
      return tabs
    },
  },
  created() {
    this.getData()
  },
  methods: {
    async download() {
      const props = [
        {
          value: 'name',
          label: '卷名',
        },
        {
          value: 'owner',
          label: 'owner租户',
        },
        {
          value: 'total_size',
          label: '总容量',
        },
        {
          value: 'used_size',
          label: '已使用',
        },
        {
          value: 'usage_ratio',
          label: '使用率',
        },
      ]
      const downLoadData = this.dataList.map(item => {
        return {
          ...item,
          total_size: this.$options.filters.renderSize(item.total_size),
        }
      })
      generateEXCEL(props, downLoadData, { region: this.clusterName }, '卷详情')
    },
    sortMethodUsedRatio(a, b) {
      const ausage_ratio = +parseFloat(a.usage_ratio)
      const busage_ratio = +parseFloat(b.usage_ratio)
      if (ausage_ratio < busage_ratio) return -1
      if (ausage_ratio > busage_ratio) return 1
      return 0
    },
    sortMethodTotal(a, b) {
      if (a.total_size < b.total_size) return -1
      if (a.total_size > b.total_size) return 1
      return 0
    },
    sortMethodUsed(a, b) {
      return sortSizeWithUnit(a.used_size, b.used_size)
    },
    refresh() {
      this.inputParams = ''
      this.getData()
    },
    onSearchClick() {
      this.$refs.filterTableData.clear()
      this.getData()
    },
    filterData(data) {
      this.dataList = [...data]
    },
    async getData() {
      this.dataList = []
      this.volTotal = 0
      if (this.clusterName) {
        const volList = await getVolList({
          keywords: this.inputParams,
          cluster_name: this.clusterName,
        })
        const tempData = (volList.data || []).sort((a, b) => {
          if (a.name < b.name) return -1
          if (a.name > b.name) return 1
          return 0
        }).filter(item => item.vol_type === 1)
        this.dataList = tempData
        this.originDataList = [...tempData]
        this.volTotal = tempData.length || 0
      }
    },
    handleExpansion({ name, total_size }, type) {
      this.$refs.expansion.open(type)
      this.$refs.expansion.initForm({
        volName: name,
        size: total_size,
      })
    },
    createVol() {
      this.$refs.createVol.open()
    },
    handleAuth({ name }) {
      this.$refs.auth.open()
      this.$refs.auth.initForm({
        volName: name,
      })
    },
    handleDp({ name }) {
      this.$refs.createDp.open()
      this.$refs.createDp.initForm({
        name,
      })
    },
    showDrawer(row, type) {
      this.drawer = true
      this.curVol = row
      this.getVolumnDetail(row)
    },
    async getVolumnDetail(row) {
      const res = await getVolDetail({
        name: row.name,
        cluster_name: this.clusterName,
      })
      this.volDetail = res.data || {}
    },
    handleMp({ name }) {
      this.$refs.createMp.open()
      this.$refs.createMp.initForm({
        name,
      })
    },
    handleClick() {
    },
    openUpdateVolumnModal(row) {
      this.$refs.updateVol.init(row, this.clusterName)
    },
  },
}
</script>
<style lang="scss" scoped>
.form-wrap {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
}
.noborder {
  border: none;
}

.infoBox {
  width: 40%;
  display: flex;
}

.input {
  // position: absolute;
  width: 300px;
  // right: 70px;
}

.search {
  position: relative;
  text-align: right;
  flex: 0 0 auto;
}

.right {
  position: relative;
  text-align: right;
}

.inside {
  margin: 10px;
}

.search-btn {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  height: 31px;
}

p {
  margin-bottom: 20px;
}

.btn {
  width: 100%;
}

.filter {
  // padding-top: 12px;
}

.link {
  color: #66cc99;
  cursor: pointer;
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
  line-height: 16px;
}

.fl-l {
  float: left;
}

::v-deep .el-drawer__body {
  overflow: auto;
}

/*2.隐藏滚动条，太丑了*/
::v-deep .el-drawer__container ::-webkit-scrollbar {
  width: 10px;
}
</style>
