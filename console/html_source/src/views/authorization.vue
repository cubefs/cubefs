<template>
  <div class="cluster author">
    <div>
      <div class="volume-left">
        <el-button type="primary" @click="openDialog('add')">{{ $t('chubaoFS.authorization.AddUser') }}</el-button>
      </div>
      <div class="volume-right">
        <el-input v-model="searchVal" placeholder="" style="width: 255px;"></el-input>
        <el-button type="primary" class="ml5" @click="queryUserList">{{ $t('chubaoFS.tools.Search') }}</el-button>
      </div>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        class="mt20"
        style="width: 100%">
        <el-table-column type="index" label="#">
        </el-table-column>
        <el-table-column prop="user_id" :label="$t('chubaoFS.authorization.detail.UserName')">
        </el-table-column>
        <el-table-column prop="userTypeStr" :label="$t('chubaoFS.authorization.detail.UserType')">
        </el-table-column>
        <el-table-column prop="description" :label="$t('chubaoFS.authorization.detail.Comments')">
        </el-table-column>
        <el-table-column :label="$t('chubaoFS.tools.Actions')">
          <template slot-scope="scope">
            <el-button type="text" class="text-btn" @click="openDialog('edit', scope.row)">{{ $t('chubaoFS.tools.Edit') }}</el-button>
            <el-button type="text" class="text-btn" @click="openDialog('delete', scope.row)">{{ $t('chubaoFS.tools.Delete') }}</el-button>
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
    <!-- Add New User -->
    <el-dialog
      :title="$t('chubaoFS.authorization.actions.AddNewUser')"
      :visible.sync="addUserDialog"
      width="30%"
      @close="closeDialog">
      <el-form :label-position="labelPosition" ref="addUserForm" :model="addUserForm" :rules="rules">
        <el-form-item :label="$t('chubaoFS.authorization.actions.UserName')" prop="userName" :rules="[
            { required: true, message: $t('chubaoFS.volume.userNameMsg'), trigger: 'blur' },
            { validator: checkUserName, message: $t('chubaoFS.authorization.tips.checkUserNameMsg'), trigger: 'blur' }
          ]">
          <el-input v-model="addUserForm.userName"></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.SetPassword')" prop="password" :rules="[
            { required: true, message: $t('chubaoFS.authorization.tips.SetPasswordMsg'), trigger: 'blur' },
            { validator: checkPassword, message:  $t('chubaoFS.authorization.tips.checkPasswordMsg'), trigger: 'blur' }
          ]">
          <el-input v-model="addUserForm.password" show-password></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.ConfirmPassword')" prop="confirmPassword" :rules="[
            { required: true, message: $t('chubaoFS.authorization.tips.confirmPasswordMsg1'), trigger: 'blur' },
            { validator: confirmPassword, message: $t('chubaoFS.authorization.tips.confirmPasswordMsg2'), trigger: 'blur' }
          ]">
          <el-input v-model="addUserForm.confirmPassword" show-password></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Type')" prop="type">
          <el-select v-model="addUserForm.type" style="width: 100%;">
            <el-option v-for="item in typeList" :value="item.value" :label="item.label" :key="item.value"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Comments')">
          <el-input type="textarea" v-model="addUserForm.comments"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="closeDialog">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary" @click="addUser('addUserForm')">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
    <!-- Edit -->
    <el-dialog
      :title="$t('chubaoFS.authorization.actions.EditUserAuthorization')"
      :visible.sync="editUserDialog"
      width="30%">
      <el-form :label-position="labelPosition" :model="editUserForm" :rules="rules">
        <el-form-item :label="$t('chubaoFS.authorization.actions.UserName')">
          <el-input v-model="editUserForm.user_id" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.NewPassword')" prop="password" :rules="[
            { validator: checkPassword, message: $t('chubaoFS.authorization.tips.checkPasswordMsg'), trigger: 'blur' }
          ]">
          <el-input v-model="editUserForm.password" show-password></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.ConfirmNewPassword')" prop="confirmPassword" :rules="[
            { validator: confirmNewPassword, message: $t('chubaoFS.authorization.tips.confirmPasswordMsg2'), trigger: 'blur' }
          ]">
          <el-input v-model="editUserForm.confirmPassword" show-password></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Type')">
          <el-select v-model="editUserForm.user_type" style="width: 100%;">
            <el-option v-for="item in typeList" :value="item.value" :label="item.label" :key="item.value"></el-option>
          </el-select>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Comments')">
          <el-input type="textarea" v-model="editUserForm.description"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="editUserDialog = false">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary"  @click="updateUser">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
    <!-- Delete -->
    <el-dialog
      :title="$t('chubaoFS.tools.Delete')"
      :visible.sync="deleteUserDialog"
      width="30%">
      <div class="delete-tip">
        <i class="warn"><img src="../assets/images/warn.png" alt=""></i>
        {{ $t('chubaoFS.authorization.tips.delTip') }}
      </div>
      <el-form :label-position="labelPosition" :model="editUserForm">
        <el-form-item  :label="$t('chubaoFS.authorization.actions.UserName')">
          <el-input v-model="editUserForm.user_id" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Comments')">
          <el-input type="textarea" v-model="editUserForm.description" disabled></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.authorization.actions.Type')">
          <el-select v-model="editUserForm.userTypeStr" disabled style="width: 100%;">
            <el-option v-for="item in typeList" :value="item.value" :label="item.label" :key="item.value"></el-option>
          </el-select>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="deleteUserDialog = false">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary" @click="deleteUser">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from '../graphql/authorization'
export default {
  name: 'authorization',
  data () {
    return {
      labelPosition: 'top',
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
      userInfoList: [],
      volumeName: '',
      addUserDialog: false,
      editUserDialog: false,
      deleteUserDialog: false,
      addUserForm: {
        userName: '',
        password: '',
        confirmPassword: '',
        comments: '',
        type: null
      },
      typeList: [
        {
          value: '2',
          label: 'admin'
        },
        {
          value: '3',
          label: 'user'
        }
      ],
      editUserForm: {
        password: '',
        confirmPassword: ''
      },
      rules: {
        type: [
          { required: true, message: this.$t('chubaoFS.authorization.tips.typeMsg'), trigger: 'change' }
        ]
      }
    }
  },
  computed: {
    crumbInfo () {
      return this.volumeName
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
      this.resData.resLists = this.userInfoList.slice(start, end)
    },
    queryUserList () {
      const that = this
      that.resData.loading = true
      const variables = {
        num: 10000
      }
      this.apollo.query(this.url.authorization, baseGql.queryUserList, variables).then((res) => {
        that.resData.loading = false
        if (res.data) {
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
          // 按user_type和user_id排列
          finalUserInfo.sort(function (a, b) {
            return a.user_id.charCodeAt(0) - b.user_id.charCodeAt(0)
          }).sort(function (a, b) {
            return a.user_type - b.user_type
          })
          this.userInfoList = finalUserInfo
          this.userInfoList.forEach(item => {
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
          this.resData.page.totalRecord = this.userInfoList.length
          this.handleCurrentChange(1)
        } else {
          this.$message.error(res.message)
        }
      }).catch(() => {
        that.resData.loading = false
      })
    },
    addUser (formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const variables = {
            accessKey: '',
            iD: this.addUserForm.userName,
            password: this.addUserForm.password,
            secretKey: '',
            description: this.addUserForm.comments || '',
            type: parseInt(this.addUserForm.type)
          }
          this.apollo.mutation(this.url.authorization, baseGql.createUser, variables).then((res) => {
            if (res.code === 200) {
              this.queryUserList()
              this.addUserDialog = false
              this.addUserForm = {}
              this.$message({
                message: this.$t('chubaoFS.message.Success'),
                type: 'success'
              })
            } else {
              this.$message.error(res.message)
            }
          }).catch((error) => {
            this.resData.loading = false
            console.log(error)
          })
        } else {
          return false
        }
      })
    },
    updateUser () {
      if (this.editUserForm.password !== this.editUserForm.confirmPassword) {
        this.$message({
          message: this.$t('chubaoFS.authorization.tips.confirmPasswordMsg2'),
          type: 'warning'
        })
        return
      }
      const variables = {
        accessKey: '',
        secretKey: '',
        password: this.editUserForm.password || '',
        userID: this.editUserForm.user_id,
        description: this.editUserForm.description,
        type: parseInt(this.editUserForm.user_type)
      }
      this.apollo.mutation(this.url.authorization, baseGql.updateUser, variables).then((res) => {
        if (res.code === 200) {
          this.queryUserList()
          this.editUserDialog = false
          this.$message({
            message: this.$t('chubaoFS.message.Success'),
            type: 'success'
          })
        } else {
          this.$message.error(res.message)
        }
      })
    },
    deleteUser () {
      const variables = {
        userID: this.editUserForm.user_id
      }
      this.apollo.mutation(this.url.authorization, baseGql.deleteUser, variables).then((res) => {
        if (res.code === 200) {
          this.queryUserList()
          this.deleteUserDialog = false
          this.$message({
            message: this.$t('chubaoFS.message.Success'),
            type: 'success'
          })
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        this.resData.loading = false
        console.log(error)
      })
    },
    openDialog (tag, row) {
      if (tag === 'add') {
        this.addUserDialog = true
      }
      if (tag === 'edit') {
        this.editUserDialog = true
        this.editUserForm = Object.assign({}, row)
        this.editUserForm.user_type = this.editUserForm.user_type + ''
        if (this.editUserForm.user_id === 'root') {
          this.typeList = [
            {
              value: '1',
              label: 'root'
            }
          ]
          this.editUserForm.user_type = '1'
          this.editUserForm.user_type.disable = true
        } else {
          this.typeList = [
            {
              value: '2',
              label: 'admin'
            },
            {
              value: '3',
              label: 'user'
            }
          ]
          this.editUserForm.user_type.disable = false
        }
      }
      if (tag === 'delete') {
        this.deleteUserDialog = true
        this.editUserForm = row
        // console.log(row)
      }
    },
    closeDialog () {
      this.addUserDialog = false
      this.addUserForm = {}
      this.$refs['addUserForm'].resetFields()
    },
    checkUserName (rule, value, callback) {
      if (!(/(^[A-Za-z][A-Za-z0-9_]{0,20}$)/.test(value))) {
        return callback(new Error(this.$t('chubaoFS.authorization.tips.checkUserNameMsg')))
      } else {
        callback()
      }
    },
    checkPassword (rule, value, callback) {
      if (!(/([A-Za-z0-9]{6,30}$)/.test(value))) {
        return callback(new Error(this.$t('chubaoFS.authorization.tips.checkPasswordMsg')))
      } else {
        callback()
      }
    },
    confirmPassword (rule, value, callback) {
      if (this.addUserForm.password !== this.addUserForm.confirmPassword) {
        return callback(new Error(this.$t('chubaoFS.authorization.tips.confirmPasswordMsg2')))
      } else {
        callback()
      }
    },
    confirmNewPassword (rule, value, callback) {
      if (this.editUserForm.password !== this.editUserForm.confirmPassword) {
        return callback(new Error(this.$t('chubaoFS.authorization.tips.confirmPasswordMsg2')))
      } else {
        callback()
      }
    }
  },
  mounted () {
    let query = this.$route.query
    this.volumeName = query.volumeName
    this.queryUserList()
  }
}
</script>

<style scoped>
  .volume-left {
    display: inline-block;
  }
  .volume-right {
    display: inline-block;
    float: right;
  }
  .delete-tip {
    padding-bottom: 20px;
    line-height: 30px;
    text-align: center;
    color: #333;
  }
  .delete-tip .warn img {
    padding-right: 5px;
    width: 20px;
    height: 20px;
  }
</style>
<style>
  .el-form-item__error {
    position: unset !important;
  }
</style>
