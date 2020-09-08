<template>
  <div class="operation-server">
    <el-table
      :data="resData.resLists"
      class="mt20"
      style="width: 100%">
      <el-table-column prop="id" label="#">
      </el-table-column>
      <el-table-column prop="name" label="User Name">
      </el-table-column>
      <el-table-column prop="userType" label="User Type">
      </el-table-column>
      <el-table-column prop="comments" label="Comments">
      </el-table-column>
    </el-table>
    <div class="clearfix mt20">
      <el-pagination
        class="fr"
        @size-change="handleSizeChange"
        @current-change="handleCurrentChange"
        :page-sizes="resData.page.pageSizes"
        :page-size="resData.page.pageSize"
        layout="sizes, prev, pager, next"
        :total="resData.page.totalRecord">
      </el-pagination>
      <span class="fr page-tips pr10">{{ $t('chubaoFS.commonTxt.eachPageShows') }}</span>
    </div>
  </div>
</template>

<script>
import baseGql from '../../graphql/operations'
export default {
  name: 'FailureList',
  data () {
    return {
      resData: {
        loading: true,
        page: {
          pageSizes: [10, 20, 30, 40],
          pageNo: 1,
          pageSize: 10,
          totalRecord: 0,
          totalPage: 1
        },
        resLists: []
      }
    }
  },
  methods: {
    handleSizeChange (val) {
      this.resData.page.pageSize = val
      this.queryList()
    },
    handleCurrentChange (val) {
      this.resData.page.pageNo = val
      this.queryList()
    },
    queryList () {
      const that = this
      that.resData.loading = true
      const variables = {
        'page': {
          'pageSize': this.resData.page.pageSize,
          'pageIndex': this.resData.page.pageNo
        },
        'param': {}
      }
      this.apollo.query(this.url.operations.queryServerList, baseGql.queryList, variables).then((res) => {
        that.resData.loading = false
        if (res) {
          const data = res.content.data
          that.resData.page.totalRecord = data.recordsTotal
          that.resData.resLists = data.data
        } else {
        }
      }).catch((error) => {
        that.resData.loading = false
        this.$Message.error(error)
      })
    }
  },
  mounted () {
    this.queryList()
  }
}
</script>

<style scoped>

</style>
