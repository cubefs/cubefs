<template>
  <div class="alarm">
    <div class="fr mb20">
      <el-input v-model="searchVal" style="width: 255px;"></el-input>
      <el-button type="primary" class="ml5" @click="queryList">{{ $t('chubaoFS.tools.Search') }}</el-button>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        class="mt20"
        style="width: 100%">
        <!--<el-table-column type="index" label="#"></el-table-column>-->
        <el-table-column prop="addr" :label="$t('chubaoFS.servers.Address')">
        </el-table-column>
        <el-table-column prop="isLeader" :label="$t('chubaoFS.servers.Leader')">
          <template slot-scope="scope">
            <div>{{scope.row.isLeader}}</div>
          </template>
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
  </div>
</template>

<script>
import baseGql from '../../graphql/server'
export default {
  name: 'alarm',
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
      },
      typeVal: '',
      searchVal: null
    }
  },
  methods: {
    handleSizeChange (val) {
      this.resData.page.pageSize = val
      this.resData.page.pageNo = 1
      this.handleCurrentChange(1)
    },
    handleCurrentChange (val) {
      this.resData.page.pageNo = val
      const start = (val - 1) * this.resData.page.pageSize
      const end = val * this.resData.page.pageSize
      this.resData.resLists = this.storageLists.slice(start, end)
    },
    queryList () {
      const that = this
      that.resData.loading = true
      const variables = {
        num: 10000
      }
      this.apollo.query(this.url.cluster, baseGql.queryMasterList, variables).then((res) => {
        that.resData.loading = false
        if (res) {
          const data = res.data
          let finalLists = []
          if (this.searchVal && this.searchVal.trim() !== '') {
            data.masterList.forEach(eachItem => {
              if (eachItem.addr.indexOf(this.searchVal.trim()) > -1) {
                finalLists.push(eachItem)
              }
            })
          } else {
            finalLists = data.masterList
          }
          this.storageLists = finalLists
          this.resData.page.totalRecord = finalLists.length
          this.handleCurrentChange(1)
        } else {
        }
      }).catch((error) => {
        that.resData.loading = false
        console.log(error)
      })
    }
  },
  mounted () {
    this.queryList()
  }
}
</script>

<style scoped>
.alarm h3{
  line-height: 16px;
  font-family: Helvetica;
  font-weight: normal;
  font-size: 13px;
  color: rgba(51,51,51,1);
  margin-bottom: 20px;
}
.volume-name{
  cursor: pointer;
  color: #466BE4;
}
</style>
