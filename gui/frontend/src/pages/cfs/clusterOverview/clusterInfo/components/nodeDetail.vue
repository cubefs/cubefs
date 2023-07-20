<template>
  <div>
    <div class="fontTypeSpan">
      <span
        class="mg-lf-m"
      ><span>主机:</span><span class="mg-lf-m">{{ curHost.host }}</span></span>
      <span
        class="mg-lf-m"
      ><span>机房:</span><span class="mg-lf-m">{{ curHost.idc }}</span></span>
      <span
        class="mg-lf-m"
      ><span>机柜:</span><span class="mg-lf-m">{{ curHost.rack }}</span></span>
      <span
        class="mg-lf-m"
      ><span>总空间:</span><span class="mg-lf-m">{{ curHost.size | readablizeBytes }}</span></span>
      <span
        class="mg-lf-m"
      ><span>已使用:</span><span class="mg-lf-m">{{ curHost.used| readablizeBytes }}</span></span>
      <span
        class="mg-lf-m"
      ><span>空闲:</span><span class="mg-lf-m">{{ curHost.free | readablizeBytes }}</span></span>
    </div>
    <el-row class="">
      <el-button
        type="text"
        size="mini"
        class="back"
        @click="goBack"
      >返 回
      </el-button>
    </el-row>
    <el-row class="inside">
      <UTablePage :data="data" class="list-table" :has-page="false" sort-by-order default-should-sort-key="disk_id">
        <el-table-column
          prop="disk_id"
          label="磁盘id"
          width="90"
          sortable="custom"
        >
        </el-table-column>
        <!-- <el-table-column prop="host" label="主机" sortable="custom"> </el-table-column>
        <el-table-column prop="idc" label="机房" sortable="custom"></el-table-column> -->
        <el-table-column prop="path" label="path" width="180"></el-table-column>
        <el-table-column label="写入中的条带组id" width="150" prop="warittingIdsCount" sortable="custom">
          <template slot-scope="scope">
            <span v-if="scope.row.volume_ids" class="color9">
              <span v-for="(link, index) in scope.row.volume_ids" :key="link">
                <a @click="toDataBlock(link)">{{ link }}</a>
                <span v-if="index !== scope.row.volume_ids.length - 1"> | </span>
              </span>
            </span>
          </template>
        </el-table-column>
        <el-table-column prop="disk_load" label="写入中的条带组数" width="150" sortable="custom"></el-table-column>
        <el-table-column prop="readonly" label="读写信息" width="100" sortable="custom">
          <template slot-scope="scope">
            <span
              :class="[
                'rw',
                `rw-${scope.row.readonly ? 'readonly' : 'readwrite'}`,
              ]"
            >
              {{ scope.row.readonly ? '只读' : '读写' }}</span>
          </template></el-table-column>
        <el-table-column prop="free_chunk_cnt" label="空闲chunk" width="110" sortable="custom"></el-table-column>
        <el-table-column prop="used_chunk_cnt" label="已用chunk" width="110" sortable="custom"></el-table-column>

        <el-table-column label="使用率" width="100" prop="percentage" sortable="custom">
          <template slot-scope="scope">
            <el-progress
              :percentage="scope.row.percentage"
              :color="[
                { color: '#f56c6c', percentage: 100 },
                { color: '#e6a23c', percentage: 80 },
                { color: '#5cb87a', percentage: 60 },
                { color: '#1989fa', percentage: 40 },
                { color: '#6f7ad3', percentage: 20 },
              ]"
            >
            </el-progress> </template></el-table-column>
        <el-table-column prop="status" label="磁盘状态" width="100" sortable="custom">
          <template slot-scope="scope">
            <div
              class="text-center"
              :class="['ant-tag', `ant-tag-${filterStatus(scope.row.status)}`]"
            >
              {{ filterStatus(scope.row.status) }}</div><br>
            <DiskRepairStatus
              :status="scope.row.status"
              :is-off-line="!isShowoffLine(scope.row.disk_id)"
              :percentage="
                +(scope.row.repair_schedule * 100).toFixed(2)
              "
            >
            </DiskRepairStatus>
          </template>
        </el-table-column>
        <el-table-column prop="size" label="总空间" width="100" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.size | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column prop="used" label="已用空间" width="100" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.used | readablizeBytes }}
          </template></el-table-column>
        <el-table-column prop="free" label="空闲空间" width="100" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.free | readablizeBytes }}
          </template>
        </el-table-column>
        <el-table-column
          fixed="right"
          prop="cluster"
          label="操作"
          :width="160"
          align="center"
        >
          <template slot-scope="{ row }">
            <el-button
              v-if="row.readonly"
              v-auth="'BLOBSTORE_NODES_ACCESS'"
              type="text"
              size="mini"
              :disabled="row.status > 1 || !isShowoffLine(row.disk_id)"
              @click="changeRW(row)"
            >切读写</el-button>
            <el-button
              v-if="!row.readonly"
              v-auth="'BLOBSTORE_DISKS_ACCESS'"
              type="text"
              size="mini"
              :disabled="row.status > 1 || !isShowoffLine(row.disk_id)"
              @click="changeRW(row)"
            >切只读</el-button>
            <!-- <el-popover
              v-if="haveOrder(row, 'NODE_DROP', 'DISK_DROP')"
              placement="top-start"
              title="磁盘下线工单正在审批中"
              trigger="hover"
              :disabled="!haveOrder(row, 'NODE_DROP', 'DISK_DROP')"
            >
              <span><a style="color:#00c9c9; cursor: pointer;" class="link" @click.stop.prevent="goOrderDetail(haveOrder(row, 'NODE_DROP', 'DISK_DROP'))">工单详情</a></span>
              <el-button
                slot="reference"
                type="text"
                size="mini"
                class="dis-btn ml-8"
              >下线</el-button>
            </el-popover>
            <el-button
              v-if="isShowoffLine(row.disk_id) && !haveOrder(row, 'NODE_DROP', 'DISK_DROP') && row.status <= 1&& !row.readonly"
              type="text"
              class="dis-btn"
              size="mini"
              title="必须切只读"
            >下线</el-button>
            <el-button
              v-if="isShowoffLine(row.disk_id) && !haveOrder(row, 'NODE_DROP', 'DISK_DROP') && row.status <= 1&& row.readonly"
              type="text"
              size="mini"
              :disabled="haveOrder(row, 'DISK_SET')"
              @click="offLine(row)"
            >下线</el-button>
            <el-button
              v-if="!isShowoffLine(row.disk_id)"
              type="text"
              size="mini"
              class="waring-status"
              disabled
            >下线中</el-button> -->
            <el-popover
              v-if="haveOrder(row, 'DISK_SET')"
              placement="top-start"
              title="设置坏盘工单正在审批中"
              trigger="hover"
              :disabled="!haveOrder(row, 'DISK_SET')"
            >
              <span><a style="color:#00c9c9; cursor: pointer;" class="link" @click.stop.prevent="goOrderDetail(haveOrder(row, 'DISK_SET'))">工单详情</a></span>
              <el-button
                slot="reference"
                v-auth="'BLOBSTORE_DISKS_SET'"
                type="text"
                size="mini"
                class="dis-btn ml-8"
              >设置坏盘</el-button>
            </el-popover>
            <el-button
              v-else
              v-auth="'BLOBSTORE_DISKS_SET'"
              type="text"
              size="mini"
              :disabled="row.status > 1 || !isShowoffLine(row.disk_id) || haveOrder(row, 'NODE_DROP', 'DISK_DROP')"
              @click="setBad(row)"
            >设置坏盘</el-button>
            <el-button
              v-auth="'BLOBSTORE_DISKS_PROBE'"
              type="text"
              size="mini"
              :disabled="+row.status == 1"
              @click="register(row)"
            >磁盘注册</el-button>
          </template>
        </el-table-column>
      </UTablePage>
    </el-row>
  </div>
</template>
<script>
import {
  getNodeList,
  offLineNodeInfo,
  offLineDropNodeList,
  changeNodeInfoRW,
  setNodeInfoBad,
  registerDisk,
} from '@/api/ebs/ebs'
import { readablizeBytes } from '@/utils'
import { nodeStatusMap } from '@/pages/cfs/status.conf'
import UTablePage from '@/components/uPageTable.vue'
import DiskRepairStatus from '@/components/diskRepairStatus'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  components: {
    UTablePage,
    DiskRepairStatus,
  },
  filters: {
    readablizeBytes(value) {
      return readablizeBytes(value)
    },
  },
  mixins: [mixin],
  inject: ['app'],
  props: {
    detail: {
      type: Object,
      default() {
        return {}
      },
    },
  },
  data() {
    return {
      data: [],
      droppingList: [],
      curHost: {},
    }
  },
  computed: {
    user() {
      return this.$store.state.userInfo
    },
    clusterInfo() {
      return JSON.parse(sessionStorage.getItem('clusterInfo'))
    },
    showHost() {
      return this.$route.query.host
    },
  },
  created() {
    this.getData()
  },
  methods: {
    async register(row) {
      try {
        await this.$confirm('请确认是否注册该磁盘?', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        })
        await registerDisk({ region: this.clusterName, clusterId: this.app.clusterId, path: row.path, host: row.host, disk_id: row.disk_id })
        this.$message.success('注册成功')
        await this.getData()
      } catch (e) {}
    },
    haveOrder(row, type, type2) {
      const temp = (row?.docking_info || []).find(i => i.OrderType === type)
      if (temp) {
        return temp?.OrderUrl
      }
      if (type2) {
        const temp2 = (row?.docking_info || []).find(i => i.OrderType === type2)
        return temp2?.OrderUrl
      }
      return false
    },
    resh() {
      this.refresh = 1
      this.getData()
    },
    filterStatus(v) {
      const temp = Object.entries(nodeStatusMap).filter(
        (temp) => temp[1] === v,
      )?.[0]
      return temp?.[0]
    },
    isShowoffLine(id) {
      return !(
        this.droppingList.filter((item) => {
          return item.disk_id === id
        })?.length || 0
      )
    },
    goBack() {
      // this.$router.replace({query:{...}})
      this.$router.go(-1)
    },
    getForms(forms) {
      this.forms = forms
      this.getData()
    },
    goOrderDetail(url) {
      window.open(url)
    },
    async getData() {
      this.data = []
      await this.getDroppingList()
      const host = this.$route.query.host
      const res = await getNodeList({
        region: this.clusterName,
        clusterId: this.app.clusterId,
        host,
      })
      const nodes = res.data.nodes
      // eslint-disable-next-line camelcase
      const { docking_info } = nodes[host]
      this.curHost = nodes[host].disks.reduce(
        (pre, next) => {
          return {
            ...next,
            host_name: nodes[host]?.host_name,
            sn: nodes[host]?.sn,
            size: pre.size + next.size,
            used: pre.used + next.used,
            free: pre.free + next.free,
          }
        },
      )
      this.data = (nodes[host]?.disks || []).map(item => {
        return {
          ...item,
          docking_info: docking_info,
          percentage: +((item.used / item.size) * 100).toFixed(2),
          warittingIdsCount: (item.volume_ids || []).length,
        }
      })
    },
    // eslint-disable-next-line camelcase
    async setBad({ disk_id }) {
      try {
        await this.$confirm('确定设置成坏盘吗', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
        })
        const res = await setNodeInfoBad({ region: this.clusterName, clusterId: this.app.clusterId, disk_id })
        if (res.data?.orderDetailUrl) {
          try {
            await this.$confirm('设置坏盘申请成功, 请前往云工单查看审批详情', '提示', {
              confirmButtonText: '查看',
              type: 'success',
            })
            window.open(res.data?.orderDetailUrl)
          } catch (error) {
            this.getData()
          }
        } else {
          this.$message.success('设置成功')
        }
        await this.getData()
      } catch (e) {
        console.log(e)
      }
    },
    async getDroppingList() {
      this.droppingList = []
      const res = await offLineDropNodeList({
        region: this.clusterName,
        clusterId: this.app.clusterId,
      })
      this.droppingList = res.data.disks || []
    },
    // eslint-disable-next-line camelcase
    async offLine({ disk_id, host, path }) {
      try {
        const remark = await this.$prompt('审批成功后,会自动触发磁盘下线', '提示', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          inputValue: '',
        })
        const { user_id: userId, user_name: userName } = this.user
        const res = await offLineNodeInfo({ region: this.clusterName, clusterId: this.app.clusterId, disk_id, user_id: userId, host_name: '', user_name: userName, node: host, disk_path: path, remark: `${remark?.value}`, docking_remark: '审批通过,将下线' })
        if (res.data?.orderDetailUrl) {
          try {
            await this.$confirm('下线操作申请成功, 请前往云工单查看审批详情', '提示', {
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
        await this.getData()
      } catch (e) {}
    },
    // eslint-disable-next-line camelcase
    async changeRW({ disk_id, readonly }) {
      try {
        await this.$confirm(
          `请确认是否将该磁盘切换为${readonly ? '读写' : '只读'}状态`,
          '提示',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning',
          },
        )
        await changeNodeInfoRW({
          region: this.clusterName,
          clusterId: this.app.clusterId,
          disk_id,
          readonly: !readonly,
        })
        this.$message.success('切换成功')
        await this.getData()
      } catch (e) {}
    },
    toDataBlock(id, type) {
      this.$router.push({ query: { vid: id } })
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
.mb10{
  margin-bottom: 10px;
}
.p-t-26 {
  padding-top: 26px;
}
.back {
  // color: #00c9c9;
  cursor: pointer;
  text-align: right;
  margin-right: 20px;
  float: right;
}
.color9 {
  color: #999;
}
.color6 {
  color: #666;
}
.mr-rt {
  margin-right: 20px;
}
::v-deep .el-progress__text {
  display: block !important;
  font-size: 12px !important;
}
.text-center {
  text-align: center;
  width: 80px;
}
.inside{
  margin: 10px;
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
