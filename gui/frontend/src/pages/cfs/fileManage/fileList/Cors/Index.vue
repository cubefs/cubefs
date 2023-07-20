<template>
  <div>
    <div>
      <el-button
        class="font14"
        type="primary"
        plain
        o-auth="treeCorsAddBtn || adminCorsAddBtn"
        @click="handleNew"
      >新增规则
      </el-button>
      <el-button
        type="danger"
        class="font14"
        o-auth="treeCorsDelAllBtn || adminCorsDelAllBtn"
        plain
        :class="{ 'disable-btn': !disableDelBtn }"
        @click="handleAllDel"
      >清空全部规则</el-button>
    </div>
    <el-table
      :data="tableData"
      class="main_table"
    >
      <el-table-column prop="AllowedOrigins" label="来源">
        <template slot-scope="scope">{{ scope.row.AllowedOrigins.join(',') }}</template>
      </el-table-column>
      <el-table-column prop="AllowedMethods" label="允许 Methods">
        <template slot-scope="scope">{{ scope.row.AllowedMethods && scope.row.AllowedMethods.join(',') }}</template>
      </el-table-column>
      <el-table-column prop="AllowedHeaders" label="允许 Headers">
        <template slot-scope="scope">{{ scope.row.AllowedHeaders && scope.row.AllowedHeaders.join(',') }}</template>
      </el-table-column>
      <el-table-column prop="ExposeHeaders" label="暴露 Headers">
        <template slot-scope="scope">{{ scope.row.ExposeHeaders && scope.row.ExposeHeaders.join(',') }}</template>
      </el-table-column>
      <el-table-column prop="MaxAgeSeconds" label="缓存时间"></el-table-column>
      <el-table-column label="操作" o-auth="treeCorsEditBtn || treeCorsDelBtn || adminCorsEditBtn || adminCorsDelBtn">
        <template slot-scope="scope">
          <i
            class="el-icon-edit color_blue"
            o-auth="treeCorsEditBtn || adminCorsEditBtn"
            @click="handleEdit(scope.row, scope)"
          ></i>
          <i class="el-icon-delete color_red" o-auth="treeCorsDelBtn || adminCorsDelBtn" @click="handleDel(scope)"></i>
        </template>
      </el-table-column>
    </el-table>
    <NewCom ref="NewCom" @get-data="getData" />
  </div>
</template>

<script>
import { getCors, setCors, deleteCors } from '@/api/cfs/cluster'
import mixin from '@/pages/cfs/clusterOverview/mixin'
import NewCom from './NewCors'
import { cloneDeep } from 'lodash'

export default {
  components: {
    NewCom
  },
  mixins: [mixin],
  data() {
    return {
      tableData: []
    }
  },
  computed: {
    disableDelBtn() {
      return this.tableData.length
    }
  },
  created() {
    this.getData()
  },
  methods: {
    async getData() {
      const { name, owner } = this.$route.query
      const res = await getCors({
        cluster_name: this.clusterName,
        vol: name,
        user: owner
      })
      this.tableData = res.data.CORSRules || []
      this.$emit('get-cors', cloneDeep(this.tableData))
    },
    handleNew() {
      this.$refs.NewCom.init(true, {}, this.tableData)
    },
    handleEdit(row, { $index }) {
      this.$refs.NewCom.init(false, row, this.tableData, $index)
    },
    handleAllDel() {
      this.$confirm('确认删除所有？')
        .then(_ => {
          this.delAllList()
        })
        .catch(_ => { })
    },
    async delAllList() {
      const { name, owner } = this.$route.query
      await deleteCors({
        cluster_name: this.clusterName,
        vol: name,
        user: owner,
      })

      this.getData()
    },
    handleDel({ $index }) {
      this.delOne($index)
    },
    async delOne(index) {
      const { name, owner } = this.$route.query
      this.tableData.splice(index, 1)
      await setCors({
        cluster_name: this.clusterName,
        vol: name,
        user: owner,
        rules: this.tableData
      })
      this.getData()
    }
  }
}
</script>

<style lang="scss" scoped>
::v-deep .el-table tr {
  background-color: #FBFBFD;
}

::v-deep .el-table__empty-block {
  min-height: 124px;
  background: #FBFBFD;
}

.color_blue {
  color: blue;
  cursor: pointer;
  font-size: 15px;
}

.color_red {
  color: red;
  margin-left: 8px;
  font-size: 15px;
  cursor: pointer;
}

.main_table {
  width: 100%;
  margin-top: 20px;
}
</style>
