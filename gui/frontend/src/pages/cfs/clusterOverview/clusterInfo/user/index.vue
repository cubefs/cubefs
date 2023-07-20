<template>
  <el-card>
    <el-row>
      <el-col :span="12">
        <el-button
          icon="el-icon-circle-plus"
          type="primary"
          @click.stop="createUser"
        >创建租户</el-button>
      </el-col>
      <el-col :span="12">
        <el-row class="search">
          <el-input
            v-model="params.userID"
            placeholder="请输入租户ID"
            clearable
            class="input"
          ></el-input>
          <el-button
            type="primary"
            class="search-btn"
            @click="getData"
          >搜 索</el-button>
        </el-row>
      </el-col>
    </el-row>
    <el-row class="userInfo">
      <u-page-table :data="dataList" :page-size="page.per_page">
        <!-- <el-table-column label="序号" type="index"></el-table-column> -->
        <el-table-column label="租户ID" prop="user_id"></el-table-column>
        <el-table-column label="租户类型" prop="user_type"></el-table-column>
        <el-table-column label="AK" prop="access_key"></el-table-column>
        <el-table-column label="SK" prop="secret_key"></el-table-column>
        <el-table-column label="创建时间" prop="create_time"></el-table-column>
      </u-page-table>
    </el-row>
    <el-row>
      <el-row>
        <el-col :span="12">
          <div class="detail_title">授权详情</div>
        </el-col>
        <el-col :span="12">
          <el-row class="search">
            <el-input
              v-model.trim="params.volName"
              placeholder="请输入卷名"
              clearable
              class="input"
            ></el-input>
            <el-button
              type="primary"
              class="search-btn"
              @click="getData"
            >搜 索</el-button>
          </el-row>
        </el-col>
      </el-row>
      <el-row>
        <u-page-table :data="dataListAuth" :page-size="pageAuth.per_page">
          <!-- <el-table-column label="序号" type="index"></el-table-column> -->
          <el-table-column label="租户ID" prop="user_id"></el-table-column>
          <el-table-column label="卷名" prop="volume"></el-table-column>
          <!-- <el-table-column label="子目录" prop="business_team"></el-table-column> -->
          <el-table-column label="业务" prop="business"></el-table-column>
          <el-table-column label="权限" prop="policy">
            <template slot-scope="scope">
              <el-radio
                v-for="item in scope.row.policy"
                :key="item"
                disabled
                :value="getTableItemPolicyVal(item)"
                :label="getTableItemPolicyVal(item)"
              >{{ getTableItemPolicyVal(item) }}</el-radio>
            </template>
          </el-table-column>
        </u-page-table>
      </el-row>
    </el-row>
    <CreateUser ref="createUser" @refresh="refresh" />
  </el-card>
</template>
<script>
import UPageTable from '@/pages/components/uPageTable'
import CreateUser from './components/createUser'
import { getUserList } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  components: {
    CreateUser,
    UPageTable,
  },
  mixins: [Mixin],
  data() {
    return {
      userTotal: 0,
      dataList: [],
      dataListAuth: [],
      params: {
        userID: '', // 输入租户id
        volName: '', // 输入卷名
      },
      page: {
        per_page: 5, // 页面大小
      },
      pageAuth: {
        per_page: 5, // 页面大小
      },
    }
  },
  computed: {},
  watch: {},
  created() {
    this.getData()
  },
  methods: {
    getTableItemPolicyVal(val) {
      return val.split(':').slice(-1).join('').toLowerCase()
    },
    refresh() {
      this.params = {
        userID: '',
        volName: '',
      }
      this.getData()
    },
    async getData() {
      this.dataList = []
      this.dataListAuth = []
      this.userTotal = 0
      const { userID, volName } = this.params
      if (this.clusterName) {
        const res = await getUserList({
          keywords: userID,
          cluster_name: this.clusterName,
          vol_name: volName,
        })
        const { users, policy } = res.data
        this.dataList = users || []
        this.userTotal = users.length || 0
        this.dataListAuth = policy || []
      }
    },
    createUser() {
      this.$refs.createUser.open()
    },
  },
}
</script>
<style lang="scss" scoped>
.noborder {
  border: none;
}
.input {
  position: absolute;
  width: 300px;
  right: 70px;
}
.search {
  position: relative;
  text-align: right;
}
.search-btn {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  height: 31px;
}
.userInfo {
  margin-bottom: 40px;
}
.detail_title {
  height: 20px;
  line-height: 20px;
  border-left: 4px solid #66cc99;
  padding-left: 8px;
  font-size: 14px;
  margin-bottom: 20px;
}
</style>
