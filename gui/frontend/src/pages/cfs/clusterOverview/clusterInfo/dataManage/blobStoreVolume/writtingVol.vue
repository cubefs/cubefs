<template>
  <div class="outer-card ">
    <div
      class="mb10 back"
      @click="change"
    ><a>返回</a> </div>
    <!-- <SearchCom ref="searchCom" @set-forms="getForms" @finish="finish" /> -->
    <el-row>
      <UTablePage
        :data="data"
        class="list-table"
        :current-page.sync="pages.page"
        :page-size.sync="pages.count"
        :total="pages.total"
        sort-by-order
        default-should-sort-key="vid"
        @shouldUpdateData="shouldUpdateData"
      >
        <el-table-column prop="allocator" label="allocator" sortable="custom"></el-table-column>
        <el-table-column prop="vid" label="vid" sortable="custom"></el-table-column>
        <!-- <el-table-column prop="host_name" label="主机名" sortable="custom"></el-table-column>
        <el-table-column prop="host" label="主机" sortable="custom"> </el-table-column> -->
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
        <el-table-column prop="health_score" label="健康度" sortable="custom" width="100">
        </el-table-column>
        <el-table-column prop="status" label="状态" width="120" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.status | filterStatus }}
          </template>
        </el-table-column>
        <el-table-column prop="code_mode" label="code_mode" width="100">
          <template slot-scope="scope">
            {{ scope.row.code_mode }}
          </template>
        </el-table-column>
        <el-table-column prop="过期时间" label="expire_time" sortable="custom">
          <template slot-scope="scope">
            {{ scope.row.expire_time | fTime }}
          </template>
        </el-table-column>
        <el-table-column
          prop="cluster"
          label="操作"
          :width="120"
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
  </div>
</template>
<script>
import UTablePage from '@/components/uPageTable.vue'
import { getWrittingVolList } from '@/api/ebs/ebs'
import { readablizeBytes, getType } from '@/utils'
import { volStatusMap } from '@/pages/cfs/status.conf'
import moment from 'moment'
import mixin from '@/pages/cfs/clusterOverview/mixin';
export default {
  components: {
    UTablePage,
  },
  mixins: [mixin],
  props: {
    clusterId: {
      type: Number,
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
    fTime(v) {
      return moment(+v / 1000000).format('YYYY-MM-DD HH:mm:ss')
    },
  },
  data() {
    return {
      data: [],
      pages: {
        count: 15,
        page: 1,
        total: 0,
      },
      cluster: '',
      drawer: false,
      activeName: 'dataBlock',
      tabs: [
        {
          label: '数据块',
          name: 'dataBlock',
          component: 'DataBlock',
        },
        {
          label: '磁盘列表',
          name: 'diskList',
          component: 'DiskList',
        },
      ],
      curVol: {},
      host: '',
    }
  },
  watch: {
    page: {
      handler() {
        this.$refs.searchCom.search()
      },
      deep: true,
    },
  },
  created() {
    this.search(false)
  },

  methods: {
    search(flag = true) {
      let fn = null
      fn = getWrittingVolList({ clusterId: this.clusterId, region: this.clusterName })
      this.getForms(fn, { vid: '', status: '' }, { region: this.clusterName, clusterId: this.clusterId }, flag)
    },
    goBack() {
      this.$router.push({ name: 'ebsVolumn' })
    },
    shouldUpdateData() {
      this.search(false)
    },
    resetPage() {
      this.pages = {
        count: this.pages.count,
        page: 1,
        total: 0,
      }
    },
    async getForms(fn, params, { cluster }, flag) {
      flag && this.resetPage()
      try {
        this.data = []
        this.cluster = cluster
        const { page, count } = this.pages
        const res = await fn(
          { ...params, page, count },
        )
        const data = res.data
        if (getType(data.volumes) === 'array' || data.volumes === null) {
          this.data = data?.volumes || []
          this.pages.total = data?.total || 0
        }
        if (this.$route.query.vid) {
          this.drawer = true
          this.activeName = this.$route.query.fromTab || 'dataBlock'
        }
      } catch (error) {
      }
    },
    change() {
      this.$emit('changeShow', 'normal')
    },
  },
}
</script>
<style lang="scss" scoped>
.p-t-26 {
  padding-top: 26px;
}
.back {
  // color: #00c9c9;
  cursor: pointer;
  text-align: right;
  margin-right: 20px;
}
.back:hover {
  color: #08e4e4d7;
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
