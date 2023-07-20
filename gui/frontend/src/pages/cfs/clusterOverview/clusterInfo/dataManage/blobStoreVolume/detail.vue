<template>
  <div class="outer-card">
    <el-row>
      <el-button
        type="text"
        size="mini"
        class="back"
        @click="goBack"
      >返 回
      </el-button>
    </el-row>
    <el-row>
      <UTablePage :data="data" class="list-table" :has-page="false" sort-by-order default-should-sort-key="vuid">
        <el-table-column prop="vuid" label="vuid" sortable="custom"> </el-table-column>
        <el-table-column
          prop="vuid_prefix"
          label="vuid_prefix"
          sortable="custom"
        ></el-table-column>
        <el-table-column prop="epoch" label="epoch" sortable="custom"></el-table-column>
        <el-table-column prop="index" label="index" sortable="custom"></el-table-column>
        <el-table-column prop="disk_load" label="disk_load" sortable="custom"></el-table-column>
        <el-table-column prop="host" width="200" label="host" sortable="custom">
          <template slot-scope="scope">
            <!-- tag="span" -->
            <a @click="toNodeDetail(scope.row.host)">{{ scope.row.host }}</a>
            <!-- <router-link
              class="link"
              :to="{
                name: 'nodeManage',
                query: {
                  host: scope.row.host,
                  activeName:'nodeManage',
                  type:'openDetail'
                },
              }"
            >{{ scope.row.host }}</router-link> -->
          </template>
        </el-table-column>
        <el-table-column prop="disk_id" label="disk_id" sortable="custom">
          <template slot-scope="scope">
            <!-- tag="span" -->
            <a @click="toDiskList(scope.row.disk_id)">{{ scope.row.disk_id }}</a>
            <!-- <router-link
              class="link"
              :to="{
                name: 'diskManage',
                query: {
                  disk_id: scope.row.disk_id,
                },
              }"
            >{{ scope.row.disk_id }}</router-link> -->
          </template>
        </el-table-column>
      </UTablePage>
    </el-row>
  </div>
</template>
<script>
import { readablizeBytes } from '@/utils'
import { volStatusMap } from '@/pages/cfs/status.conf'
import UTablePage from '@/components/uPageTable.vue'
import { getVolListById } from '@/api/ebs/ebs'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  components: {
    UTablePage,
  },
  inject: ['app'],
  mixins: [mixin],
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
      data: [],
      form: {
        vid: '',
        status: '',
        used: '',
        free: '',
        total: '',
        health_score: '',
      },
      diskArr: [],
    }
  },
  mounted() {
    this.getVolList()
  },
  methods: {
    goBack() {
      this.$router.go(-1)
    },
    async getVolList() {
      const res = await getVolListById(
        {
          region: this.clusterName,
          clusterId: this.app.clusterId,
        },
      )({ vid: Number(this.$route.query.vid), page: 1, count: 10 })
      const temp = res.data
      this.form = temp
      this.$emit('curVolChange', temp)
      this.data = temp?.unit_details || temp?.units || []
      this.data.forEach(item => {
        this.diskArr.push(item.disk_id)
      })
      this.$emit('diskArrChange', this.diskArr)
    },
    toDiskList(diskId) {
      this.$router.push({ query: { ...this.$route.query, disk_id: diskId } })
      this.$emit('activeChange', 'diskList')
    },
    toNodeDetail(host) {
      this.$router.replace({ query: { host: host } })
    },
  },
}
</script>
<style lang="scss" scoped>
.p-t-26 {
  padding-top: 26px;
}
.form-content {
  width: 30%;
}
.healthy {
margin-top: 11px;
}
.back {
  // color: #00c9c9;
  cursor: pointer;
  text-align: right;
  margin-right: 20px;
  float: right;
}
.back:hover {
  color: #08e4e4d7;
}
</style>
