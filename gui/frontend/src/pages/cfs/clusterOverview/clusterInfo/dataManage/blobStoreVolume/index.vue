<template>
  <el-card class="">
    <el-row v-if="showWhich==='normal'" style="text-align:right;margin-bottom: 10px">
      <a @click="changeShow('writing')">正在写入的条带组</a>
      <SearchCom ref="searchCom" @set-forms="getForms" />
      <UTablePage
        ref="table"
        :marker-pagination="markerPagination"
        :marker="marker"
        :layouts="markerPagination ? 'sizes': 'total, sizes, prev, pager, next, jumper'"
        :data="data"
        class="list-table"
        :current-page.sync="pages.page"
        :page-size.sync="pages.count"
        :total="pages.total"
        sort-by-order
        default-should-sort-key="vid"
        @updateMarker="updateMarker"
        @shouldUpdateData="shouldUpdateData"
      >
        <el-table-column
          type="index"
          label="序号"
          class-name="link"
          fixed="left"
          width="80"
        >
        </el-table-column>
        <el-table-column prop="vid" label="vid" sortable="custom"></el-table-column>
        <el-table-column prop="used" label="已使用" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.used | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column prop="free" label="空闲" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.free | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column prop="total" label="总空间" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.total | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column label="使用率" width="120" prop="usedRadio" sortable="custom">
          <template slot-scope="scope">
            <!-- scope.row.size / scope.row.used -->
            <!-- <span>{{ scope.row.used | readablizeBytes }}{{ '/'+scope.row.usedRadio+'%' }}</span> -->
            <el-progress
              :percentage="scope.row.usedRadio > 100 ? 100 : scope.row.usedRadio"
              :color="[
                { color: '#f56c6c', percentage: 100 },
                { color: '#e6a23c', percentage: 80 },
                { color: '#5cb87a', percentage: 60 },
                { color: '#1989fa', percentage: 40 },
                { color: '#6f7ad3', percentage: 20 },
              ]"
            >
            </el-progress> </template></el-table-column>
        <el-table-column prop="health_score" label="健康度" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.code_mode }}
          </template>
        </el-table-column>
        <el-table-column prop="status" label="状态" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.status | filterStatus }}
          </template>
        </el-table-column>
        <el-table-column prop="code_mode" label="code_mode" sortable="custom">
          <template slot-scope="scope">
            {{ codeMap[scope.row.code_mode] }}
          </template>
        </el-table-column>
        <el-table-column
          prop="cluster"
          label="操作"
          :width="150"
          align="center"
        >
          <template slot-scope="scope">
            <router-link
              class="link"
              :to="{
                query: {
                  vid: scope.row.vid,
                },
              }"
            >详情</router-link>
          </template>
        </el-table-column>
      </UTablePage>
    </el-row>
    <el-row v-if="showWhich==='writing'">
      <WritingVol @changeShow="changeShow" :clusterId="clusterId"/>
    </el-row>
    <el-drawer :destroy-on-close="true" :visible.sync="drawer" size="80%" :before-close="handleClose" title="条带组详情">
      <div v-if="$route.query.vid ||$route.query.disk_id" slot="title" class="TitleFontType">
        条带组详情
      </div>
      <div v-if="$route.query.host" slot="title" class="TitleFontType">
          节点详情
      </div>
      <volumnDetail v-if="$route.query.vid ||$route.query.disk_id" />
      <nodeDetail v-if="$route.query.host" />
    </el-drawer>
  </el-card>
</template>
<script>
import SearchCom from '@/pages/cfs/clusterOverview/clusterInfo/components/searchCom.vue'
import UTablePage from '@/components/uPageTable.vue'
import WritingVol from './writtingVol.vue'
import volumnDetail from './volumnDetail.vue'
import nodeDetail from '@/pages/cfs/clusterOverview/clusterInfo/components/nodeDetail.vue'
import { readablizeBytes, getType, codeMap } from '@/utils'
import { volStatusMap } from '@/pages/cfs/status.conf'
export default {
  components: {
    SearchCom,
    UTablePage,
    WritingVol,
    volumnDetail,
    nodeDetail,
  },
  provide() {
    return {
      app: this
    }
  },
  filters: {
    readablizeBytes(value) {
      return readablizeBytes(value)
    },
    filterStatus(v) {
      const temp = Object.entries(volStatusMap).filter(
        (temp) => temp[1] === v,
      )?.[0]
      return temp?.[0]
    },
  },
  data() {
    return {
      // forms: {},
      showWhich: 'normal',
      data: [],
      pages: {
        count: 15,
        page: 1,
        total: 0,
      },
      marker: '', // 上一页 \ 下一页的标志位
      markerPagination: true,
      clusterId: undefined,
      curVol: undefined,
      drawer: false,
      host: '',
      codeMap,
    }
  },
  watch: {
    page: {
      handler() {
        this.$refs.searchCom.searchClick()
      },
      deep: true,
    },
    $route: {
      handler() {
        if (this.$route.query.vid || this.$route.query.host) {
          this.drawer = true
        } else {
          this.drawer = false
        }
      },
      immediate: true,
    }
  },
  created() {
  },
  methods: {
    openDrawer(row) {
      this.curVol = row
      this.drawer = true
    },
    handleClose(done) {
      this.$router.push({ query: {} })
      this.activeName = 'dataBlock'
      done()
    },
    updateMarker(marker) {
      this.marker = marker
      this.shouldUpdateData()
    },
    shouldUpdateData() {
      this.$refs.searchCom.searchClick(false)
    },
    // finish() {
    //   if (this.$route.name === 'ebsVolumn') { this.$refs.searchCom.searchClick(false) }
    // },
    resetPage() {
      this.pages = {
        count: this.pages.count,
        page: 1,
        total: 0,
      }
    },

    async getForms(fn, params, { region, clusterId }, flag) {
      this.clusterId = clusterId
      if (flag) {
        this.resetPage()
        this.marker = ''
        this.$refs.table.clearMarker()
      }
      this.markerPagination = !params?.status // 切换分页样式
      this.data = []
      const { page, count } = this.pages
      const res = await fn(
        !params
          ? {
              page,
              marker: this.marker,
              count,
              region,
              clusterId,
            }
          : { ...params, page, count, region, clusterId },
      )
      if (getType(res.data.volumes) === 'array' || res.data.volumes === null) {
        this.data = (res.data?.volumes || []).map(item => {
          return {
            ...item,
            usedRadio: +((item.used / item.total) * 100).toFixed(2),
          }
        })
        const temp = this.data.map(item => item.vid)
        const tempMarker = Math.max(...temp)
        this.pages.total = res?.total || 0
        this.marker = tempMarker || ''
      } else {
        this.marker = ''
        this.$refs.table.clearMarker()
        Object.keys(res).length && (this.data = [res].map(item => {
          return {
            ...item,
            usedRadio: +((item.used / item.total) * 100).toFixed(2),
          }
        }))
        this.pages.total = this.data.length
      }
    },
    changeShow(val) {
      this.showWhich = val
    },
  },
}
</script>
<style lang="scss" scoped>
.p-t-26 {
  padding-top: 26px;
}
::v-deep .el-progress__text {
  display: block !important;
  font-size: 12px !important;
}
.fontType{
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
line-height: 20px;
}

::v-deep .el-drawer__body {
  overflow: auto;
}
/*2.隐藏滚动条，太丑了*/
::v-deep .el-drawer__container ::-webkit-scrollbar{
    width: 10px;
}
</style>
