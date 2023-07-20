<template>
  <div class="outer-card">
    <SearchCom ref="searchCom" has-host @set-forms="getForms" />
    <el-row>
      <UTablePage
        :data="data"
        class="list-table"
        :current-page.sync="pages.page"
        :page-size.sync="pages.count"
        :total="pages.total"
        sort-by-order
        default-should-sort-key="host"
        @shouldUpdateData="shouldUpdateData"
      >
        <el-table-column
          type="index"
          label="序号"
          class-name="link"
          fixed="left"
          width="60"
        >
        </el-table-column>
        <el-table-column prop="host" label="主机" :width="200" sortable="custom">
        </el-table-column>
        <el-table-column prop="idc" label="机房" :width="100" sortable="custom"></el-table-column>
        <el-table-column prop="rack" label="机柜" sortable="custom"></el-table-column>
        <el-table-column prop="readonly" label="读写信息" :width="120">
          <template slot-scope="scope">
            <div class="rw h-58">
              <el-button-group size="mini">
                <el-button class="rw-title rw-btn">RO</el-button>
                <el-button class="rw-num rw-btn" :style="{ color: (scope.row.readonlyIds || []).length ? '#fbc200' : '#fff' }">{{
                  (scope.row.readonlyIds || []).length
                }}</el-button>
              </el-button-group>
              <el-button-group size="mini">
                <el-button class="rw-title rw-btn">RW</el-button>
                <el-button class="rw-num rw-btn">{{
                  (scope.row.readWriteIds || []).length
                }}</el-button>
              </el-button-group>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="size" label="总空间" :width="90" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.size | readablizeBytes }}
          </template></el-table-column>
        <el-table-column prop="used" label="已使用" :width="90" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.used | readablizeBytes }}
          </template></el-table-column>
        <el-table-column prop="free" label="剩余" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.free | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column label="使用率" width="120" prop="usedRadio" sortable="custom">
          <template slot-scope="scope">
            <!-- scope.row.size / scope.row.used -->
            <el-progress
              :percentage="scope.row.usedRadio"
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
        <el-table-column prop="badDiskCt" label="坏盘/总(数)">
          <template slot-scope="scope">
            {{ `${(scope.row.badDiskIds || []).length} / ${scope.row.allNodeCt}` }}
          </template>
        </el-table-column>
        <el-table-column label="下线中" prop="offLineNum" width="100" sortable="custom">
          <template slot-scope="scope">
            {{ `${scope.row.offLineNum}` }}
          </template>
        </el-table-column>
        <el-table-column
          prop="cluster"
          label="操作"
          :width="230"
          align="center"
          fixed="right"
        >
          <template slot-scope="scope">
            <MoreOperate :count="2">
              <el-button
                type="text"
                size="mini"
              ><router-link
                class="link"
                :to="{
                  query: {
                    host: scope.row.host,
                  },
                }"
              >详情</router-link></el-button>
              <el-button
                v-auth="'BLOBSTORE_NODES_ACCESS'"
                class="w100"
                type="text"
                size="mini"
                :disabled="computeRW(scope.row, false) === 0"
                @click="changeRW(scope.row, false)"
              >
                切读写</el-button>
              <el-button
                v-auth="'BLOBSTORE_DISKS_ACCESS'"
                class="w100"
                type="text"
                size="mini"
                :disabled="computeRW(scope.row, true) === 0"
                @click="changeRW(scope.row, true)"
              >
                切只读</el-button>
              <!-- <div>
                <el-popover
                  v-if="haveOrder(scope.row, 'NODE_DROP')"
                  placement="top-start"
                  title="磁盘下线工单正在审批中"
                  trigger="hover"
                  :disabled="!haveOrder(scope.row, 'NODE_DROP')"
                >
                  <span><a style="color:#00c9c9; cursor: pointer;" class="link" @click.stop.prevent="goOrderDetail(haveOrder(scope.row, 'NODE_DROP'))">工单详情</a></span>
                  <el-button
                    slot="reference"
                    type="text"
                    size="mini"
                    class="dis-btn ml-8"
                  >磁盘下线</el-button>
                </el-popover>
                <el-button
                  v-if="disabledOffLine(scope) && !haveOrder(scope.row, 'NODE_DROP')"
                  type="text"
                  class="w100 dis-btn"
                  size="mini"
                  :title="scope.row.readonlyIds.length === scope.row.allNodeCt ? '' : '必须切只读'"
                >磁盘下线</el-button>
                <el-button
                  v-if="!disabledOffLine(scope) && !haveOrder(scope.row, 'NODE_DROP')"
                  type="text"
                  class="w100"
                  size="mini"
                  @click="offLine(scope.row)"
                >磁盘下线</el-button>
              </div>
              <el-popover
                v-if="haveOrder(scope.row, 'NODE_SERVER_OFFLINE')"
                placement="top-start"
                title="服务下线工单正在审批中"
                trigger="hover"
                :disabled="!haveOrder(scope.row, 'NODE_SERVER_OFFLINE')"
              >
                <span><a style="color:#00c9c9; cursor: pointer;" class="link" @click.stop.prevent="goOrderDetail(haveOrder(scope.row, 'NODE_SERVER_OFFLINE'))">工单详情</a></span>
                <el-button
                  slot="reference"
                  type="text"
                  size="mini"
                  class="dis-btn ml-8"
                >服务下线</el-button>
              </el-popover>
              <el-button
                v-else-if="scope.row.repairedCt !== scope.row.allNodeCt"
                type="text"
                size="mini"
                class="dis-btn ml-8"
                title="这个节点所有磁盘状态为repaired的时候才能下线"
              >服务下线</el-button>
              <el-button
                v-else
                class="w100"
                type="text"
                size="mini"
                @click="offLineService(scope.row)"
              >服务下线</el-button> -->
            </MoreOperate>
          </template>
        </el-table-column>
      </UTablePage>
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
    </el-row>
  </div>
</template>
<script>
import SearchCom from '@/pages/cfs/clusterOverview/clusterInfo/components/searchCom.vue'
import MoreOperate from '@/components/moreOPerate.vue'
import volumnDetail from '@/pages/cfs/clusterOverview/clusterInfo/dataManage/blobStoreVolume/volumnDetail.vue'
import nodeDetail from '@/pages/cfs/clusterOverview/clusterInfo/components/nodeDetail.vue'
import {
  getNodeList,
  offLineNodeList,
  offlineService,
  changeRWNodeList,
  offLineDropNodeList,
} from '@/api/ebs/ebs'
import { readablizeBytes } from '@/utils'
import { nodeStatusMap } from '@/pages/cfs/status.conf'
import UTablePage from '@/components/uPageTable.vue'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  components: {
    SearchCom,
    UTablePage,
    MoreOperate,
    volumnDetail,
    nodeDetail,
  },
  filters: {
    readablizeBytes(value) {
      return readablizeBytes(value)
    },
    filterStatus(v) {
      const temp = Object.entries(nodeStatusMap).filter(
        (temp) => temp[1] === v,
      )?.[0]
      return temp?.[0]
    },
  },
  mixins: [mixin],
  provide() {
    return {
      app: this,
    }
  },
  data() {
    return {
      data: [],
      droppingList: [],
      pages: {
        count: 15,
        page: 1,
        total: 0,
      },
      clusterId: undefined,
      drawer: false,
      activeName: 'diskList',
      curHost: {},
      forms: {},
    }
  },
  computed: {
    user() {
      return this.$store.state.userInfo
    },
  },
  watch: {
    $route: {
      handler() {
        if (this.$route.query.vid || this.$route.query.host) {
          this.drawer = true
        } else {
          this.drawer = false
        }
      },
      immediate: true,
    },
  },
  mounted() {
    this.clusterId = this.$refs.searchCom.forms.clusterId
    this.forms = this.$refs.searchCom.forms
    this.getData()
  },
  methods: {
    handleClose(done) {
      this.$router.push({ query: {} })
      this.activeName = 'dataBlock'
      done()
    },
    haveOrder(row, type) {
      const temp = (row?.docking_info || []).find(i => i.OrderType === type)
      return temp?.OrderUrl
    },
    disabledOffLine(scope) {
      return scope.row.disabledCt === scope.row.allNodeCt || (scope.row.readonlyIds || []).length !== scope.row.allNodeCt
    },
    goOrderDetail(scope) {
      window.open(scope)
    },
    async offLineService(row) {
      try {
        const remark = await this.$prompt(
          '审批成功后,会自动触发服务下线',
          '提示',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            inputValue: '',
            dangerouslyUseHTMLString: true,
          },
        )
        const { user_id: userId, user_name: userName } = this.user
        const { cluster } = this.forms
        const res = await offlineService({ host: row.host, cluster, user_id: userId, user_name: userName, host_name: row.host_name, remark: `${remark?.value}`, docking_remark: '审批通过,将下线' })
        if (res.data?.orderDetailUrl) {
          try {
            await this.$confirm('服务下线操作申请成功, 请前往云工单查看审批详情', '提示', {
              confirmButtonText: '查看',
              type: 'success',
            })
            window.open(res.data?.orderDetailUrl)
          } catch (error) {
            this.getData()
          }
        }
      } catch (error) {

      }
    },
    isShowoffLine(id) {
      return !(
        this.droppingList.filter((item) => {
          return item.disk_id === id
        })?.length || 0
      )
    },

    shouldUpdateData() {
      this.getData()
    },
    async getDroppingList() {
      const res = await offLineDropNodeList({
        region: this.clusterName,
        clusterId: this.forms.clusterId,
      })
      this.droppingList = res.data.disks || []
    },
    async getData() {
      await this.getDroppingList()
      this.data = []
      this.forms.host = this.forms.host || this.$route.query.host || ''
      const { page, count } = this.pages
      const res = await getNodeList({
        ...this.forms,
        region: this.clusterName,
        page,
        count,
      })
      const temp = []
      const nodes = res.data.nodes
      Object.entries(nodes).forEach(([k, re]) => {
        const item = (re?.disks || []).reduce(
          (pre, next) => {
            return {
              ...next,
              allNodeCt: re?.disks.length,
              disks: re?.disks,
              readonlyIds: next.readonly === true ? [...pre.readonlyIds, next.disk_id] : pre.readonlyIds,
              readWriteIds: next.readonly !== true ? [...pre.readWriteIds, next.disk_id] : pre.readWriteIds,
              repairedCt:
                next.status === 4 ? ++pre.repairedCt : pre.repairedCt,
              badDiskCt: next.status > 1 ? ++pre.badDiskCt : pre.badDiskCt,
              badDiskIds: next.status > 1 ? [...pre.badDiskIds, next.disk_id] : pre.badDiskIds,
              size: pre.size + next.size,
              used: pre.used + next.used,
              free: pre.free + next.free,
              offLineIdcs: this.droppingList.filter((i) => {
                return i.host === next.host
              }).map(i => i.disk_id),
              offLineNum:
                this.droppingList.filter((i) => {
                  return i.host === next.host
                })?.length || 0,
            }
          },
          {
            size: 0,
            used: 0,
            free: 0,
            badDiskCt: 0,
            repairedCt: 0,
            badDiskIds: [],
            readWriteIds: [],
            readonlyIds: [],
            offLineIdcs: [],
          },
        )
        temp.push({ ...item, docking_info: re?.docking_info })
      })
      this.data = temp.map(item => {
        return {
          ...item,
          usedRadio: +((item.used / item.size) * 100).toFixed(2),
        }
      }).map(kkk => {
        const temp = [...new Set(this.droppingList.filter((it) => {
          return it.host === kkk.host
        }).map(i => i.disk_id).concat(kkk.badDiskIds))].length || 0
        return {
          ...kkk,
          disabledCt: temp >= kkk.allNodeCt ? kkk.allNodeCt : temp,
        }
      })
      this.pages.total = res.data?.total || (this.forms.host ? 1 : 0)
      // }
    },
    resetPage() {
      this.pages = {
        count: this.pages.count,
        page: 1,
        total: 0,
      }
    },
    getForms(fn, params, forms, flag) {
      this.clusterId = forms.clusterId
      this.resetPage()
      this.forms = {
        ...params,
        ...forms,
      }
      this.getData()
    },
    computeRW(row, readOnly = true) {
      const res = (row[!readOnly ? 'readonlyIds' : 'readWriteIds'].filter(i => {
        return !row.offLineIdcs.includes(i) && !row.badDiskIds.includes(i)
      }))?.length
      return res
    },
    computeOffline(row) {
      return [...new Set()]
    },
    async offLine(row) {
      try {
        const remark = await this.$prompt(
          `<span>请确认是否将该节点下<span style="color: #f5222d">${
          row.disabledCt ? (row.allNodeCt - row.disabledCt) : row.allNodeCt
          }块磁盘</span>下线? 共${row.allNodeCt}块</span>。<br>审批成功后,会自动触发磁盘下线`,
          '提示',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            inputValue: '',
            dangerouslyUseHTMLString: true,
          },
        )
        const { user_id: userId, user_name: userName } = this.user
        const { cluster } = this.forms
        const res = await offLineNodeList({ host: row.host, cluster, user_id: userId, user_name: userName, host_name: row.host_name, remark: `${remark?.value}`, docking_remark: '审批通过,将下线' })
        if (res.data?.orderDetailUrl) {
          try {
            await this.$confirm('磁盘下线操作申请成功, 请前往云工单查看审批详情', '提示', {
              confirmButtonText: '查看',
              type: 'success',
            })
            window.open(res.data?.orderDetailUrl)
          } catch (error) {
            this.getData()
          }
        } else {
          this.$message.success('下线成功')
        }
        this.getData()
      } catch (e) {}
    },
    async changeRW(row, readonly) {
      try {
        await this.$confirm(
          `<span>请确认是否将该节点下<span style="color: #f5222d">${
          this.computeRW(row, readonly)
          }块磁盘</span>切换为<span style="color: #f5222d">${
            !readonly ? ' 读写 ' : ' 只读 '
          }</span>状态? 共${row.allNodeCt}块</span>`,
          '提示',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning',
            dangerouslyUseHTMLString: true,
          },
        )
        const { cluster } = this.forms
        await changeRWNodeList({
          host: row.host,
          readonly: readonly,
          cluster,
        })
        this.$message.success('切换成功')
        this.getData()
      } catch (e) {}
    },
  },
}
</script>
<style lang="scss" scoped>
.dis-btn {
  color: #bfbfbf;
}
.ml-8 {
  margin-left: 8px;
}
.w100 {
  width: 100%;
}
.h-58 {
  height: 58px;
}
.p-t-26 {
  padding-top: 26px;
}

  .rw {
    > div {
      display: flex;
    }
    &-btn {
      color: #fff;
      &:hover {
        border: 1px solid #00000000;
      }
    }
    &-title {
      width: 52px;
      background-color: #90a4ae;
    }
    &-num {
      width: 35px;
      background-color: #808080;
    }
  }
@media (max-width: 1447px) {
  .rw {
    > div {
      display: flex;
    }
    &-btn {
      color: #fff;
      &:hover {
        border: 1px solid #00000000;
      }
    }
    &-title {
      width: 32px;
      background-color: #90a4ae;
    }
    &-num {
      width: 35px;
      background-color: #808080;
    }
  }
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
