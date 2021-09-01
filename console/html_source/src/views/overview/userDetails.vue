<template>
  <div class="cluster alarm">
    <div class="clearfix">
      <h3 class="fl"> {{$t('chubaoFS.userList.UserList')}}</h3>
      <div class="fr">
        <el-input v-model="searchVal" style="width: 255px;"></el-input>
        <el-button type="primary" class="ml5" @click="queryList">{{ $t('chubaoFS.tools.Search') }}</el-button>
      </div>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        class="mt20"
        style="width: 100%">
        <el-table-column type="index" label="#"></el-table-column>
        <el-table-column
          prop="user_id"
          :label="$t('chubaoFS.userList.User')"
          width="180">
        </el-table-column>
        <el-table-column
          prop="userStatistical.data"
          :label="$t('chubaoFS.userList.Data')">
        </el-table-column>
        <el-table-column
          prop="userStatistical.volumeCount"
          :label="$t('chubaoFS.userList.VolumeCount')">
        </el-table-column>
        <el-table-column
          prop="userStatistical.dataPartitionCount"
          :label="$t('chubaoFS.userList.DataPartitionCount')">
        </el-table-column>
        <el-table-column
          prop="userStatistical.metaPartitionCount"
          :label="$t('chubaoFS.userList.MetaPartitionCount')">
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
import baseGql from '../../graphql/overview'

export default {
  name: 'userDetails',
  data () {
    return {
      searchVal: '',
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
      storageLists: []
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
      this.apollo.query(this.url.user, baseGql.queryUserList, variables).then((res) => {
        that.resData.loading = false
        if (res) {
          let finalUserInfo = []
          if (this.searchVal && this.searchVal.trim() !== '') {
            res.data.listUserInfo.forEach(user => {
              if (user.user_id.indexOf(this.searchVal.trim()) > -1) {
                finalUserInfo.push(user)
              }
            })
          } else {
            finalUserInfo = res.data.listUserInfo
          }
          this.storageLists = finalUserInfo
          this.storageLists.forEach(item => {
            if (item.user_type === 1) {
              item.userTypeStr = 'root'
            }
            if (item.user_type === 2) {
              item.userTypeStr = 'admin'
            }
            if (item.user_type === 3) {
              item.userTypeStr = 'user'
            }
          })
          this.resData.page.totalRecord = this.storageLists.length
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
    // this.resData.resLists = this.testData
    // this.resData.loading = false
  }
}
</script>

<style scoped>
.alarm h3{
  display: inline-block;
  margin-right: 60px;
  line-height: 16px;
  font-weight: bold;
  font-size: 14px;
  color: #333;
}
</style>
