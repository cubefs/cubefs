<template>
  <el-card>
    <el-form :inline="true">
      <el-form-item label="卷名称">
        <el-input v-model.trim="searchForm.name" placeholder="请输入" clearable></el-input>
      </el-form-item>
      <el-form-item label="owner租户">
        <el-input v-model.trim="searchForm.owner" placeholder="请输入" clearable></el-input>
      </el-form-item>
      <el-form-item label="状态">
        <el-select v-model.trim="searchForm.status" placeholder="请选择" clearable>
          <el-option label="Normal" value="Normal" />
          <el-option label="Marked delete" value="Marked delete" />
          <el-option label="Unknown" value="Unknown" />
        </el-select>
      </el-form-item>
      <el-form-item>
        <el-button type="primary" @click="search">查询</el-button>
      </el-form-item>
    </el-form>
    <u-page-table
      :data="dataList"
      :form-data="formData"
      :search-data.sync="searchData"
      :url="tableUrl"
      :current-page.sync="page.page"
      :page-size.sync="page.per_page"
      :total="page.total"
    >
      <el-table-column label="卷名称" prop="name">
        <template slot-scope="scope">
          <el-button
            type="text"
            class="link primary-color text-decoration-none"
            @click="gotoFileList(scope.row)"
          >
            {{ scope.row.name }}
          </el-button>
        </template>
      </el-table-column>
      <el-table-column label="owner租户" prop="owner">
        <template slot-scope="scope">
          <div>{{ scope.row.owner }}</div>
        </template>
      </el-table-column>
      <el-table-column label="状态" prop="status">
        <template slot-scope="scope">
          <div>{{ scope.row.status }}</div>
        </template>
      </el-table-column>
      <el-table-column label="是否配置s3">
        <template slot="header">
          <el-tooltip
            class="item"
            effect="dark"
            content="管理文件请先配置s3地址"
            placement="top-start"
          >
            <span><i class="el-icon-question"></i> 是否配置s3 </span>
          </el-tooltip>
        </template>
        <template slot-scope="scope">
          <span>{{ s3Endpoint ? "是" : '否' }}</span>
        </template>
      </el-table-column>
      <el-table-column label="master域名" prop="master_addr"></el-table-column>
      <el-table-column label="创建时间" prop="create_time">
        <template slot-scope="scope">
          <div>{{ scope.row.create_time }}</div>
        </template>
      </el-table-column>
    </u-page-table>
  </el-card>
</template>
<script>
import { getVolList, getCors } from '@/api/cfs/cluster'
import uPageTable from '@/pages/components/uPageTable.vue'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
import { cloneDeep } from 'lodash'
export default {
  components: {
    uPageTable,
  },
  mixins: [Mixin],
  data() {
    return {
      dataList: [],
      formData: {},
      searchData: {},
      allNames: [],
      tableUrl: '',
      page: {
        page: 1, // 当前页
        per_page: 15, // 页面大小
        total: 0,
      },
      searchForm: {
        name: '',
      },
      originDataList: [],
    }
  },
  computed: {
    s3Endpoint() {
      return this.clusterInfo.clusterInfo.s3_endpoint
    }
  },
  watch: {
    searchData() {
      this.getData()
    },
  },
  async created() {
    await this.getData()
  },
  methods: {
    async getCors({ vol, user }) {
      const params = {
        vol,
        user
      }
      const res = await getCors(params)
    },
    gotoFileList(row) {
      if (!this.s3Endpoint) {
        this.$message.warning('没有s3配置，请到资源管理页面配置s3 endpoint')
        return;
      }
      this.$router.push({
        name: 'fileList',
        query: {
          id: row.id,
          zone: row.zone,
          zone_name: row.zone_name,
          name: row.name,
          owner: row.owner,
        }
      })
    },
    search() {
      let tmpDataList = cloneDeep(this.originDataList)
      if (this.searchForm.name) {
        tmpDataList = tmpDataList.filter(item => item.name.includes(this.searchForm.name))
      }
      if (this.searchForm.owner) {
        tmpDataList = tmpDataList.filter(item => item.owner === this.searchForm.owner)
      }
      if (this.searchForm.status) {
        tmpDataList = tmpDataList.filter(item => item.status === this.searchForm.status)
      }
      this.dataList = tmpDataList
      this.page.total = this.dataList?.length || 0
    },
    refresh() {
      this.getData()
    },
    async getData() {
      const res = await getVolList({
        cluster_name: this.clusterName,
      })
      this.dataList = res.data
      this.originDataList = cloneDeep(this.dataList)
      this.page.total = res.data?.length || 0
    },
  },
}
</script>
<style lang="scss" scoped>
.noborder {
  border: none;
}

.link {
  color: #2fc29b;
  cursor: pointer;
}

.text-decoration-none {
  text-decoration: none;
}
</style>
