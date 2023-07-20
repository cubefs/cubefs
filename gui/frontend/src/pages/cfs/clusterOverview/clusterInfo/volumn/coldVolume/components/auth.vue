<template>
  <el-dialog
    title="授权"
    :visible.sync="dialogFormVisible"
    width="700px"
    @closed="clearData"
  >
    <el-form
      ref="form"
      :model="forms"
      :rules="rules"
      label-width="25%"
      class="mid-block"
    >
      <el-form-item label="卷名:" prop="volName">
        <el-input v-model="forms.volName" disabled class="input"></el-input>
      </el-form-item>
      <el-form-item label="租户:" prop="user">
        <el-select
          v-model="forms.user"
          class="input"
          filterable
          reserve-keyword
          remote
          placeholder="请输入租户"
          :remote-method="remoteMethod"
        >
          <el-option
            v-for="item in userList"
            :key="item.value"
            :label="item.label"
            :value="item.value"
          ></el-option>
        </el-select>
      </el-form-item>
      <!-- <el-form-item label="子路径:" prop="childPath">
        <el-input
          v-model="forms.childPath"
          class="input"
          placeholder="请输入子路径"
        ></el-input>
      </el-form-item> -->
      <el-form-item label="权限:" prop="auth">
        <el-checkbox-group v-model="forms.auth">
          <el-checkbox label="perm:builtin:ReadOnly">readonly</el-checkbox>
          <el-checkbox label="perm:builtin:Writable">writable</el-checkbox>
        </el-checkbox-group>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button ref="pol" type="primary" @click="doCheck">确 定</el-button>
      <el-button ref="pol" type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { updateUserPolicy, getUserNameList } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      userList: [],
      forms: {
        user: '',
        volName: '',
        childPath: '',
        auth: ['perm:builtin:Writable'],
      },
      dialogFormVisible: false,
    }
  },
  computed: {
    rules() {
      return {
        user: [
          {
            required: true,
            message: '请输入用户ID',
            trigger: 'blur',
          },
        ],
        auth: [
          {
            required: true,
            message: '请选择权限',
            trigger: 'blur',
          },
        ],
        volName: [
          {
            required: true,
            message: '请输入卷名称',
            trigger: 'blur',
          },
        ],
      }
    },
  },
  created() {},
  methods: {
    initForm(val) {
      this.forms = { ...this.forms, ...val }
    },
    open() {
      this.dialogFormVisible = true
      // 默认显示所有的用户
      this.getUserNameList()
    },
    clearData() {
      this.forms = {
        user: '',
        volName: '',
        childPath: '',
        auth: ['perm:builtin:Writable'],
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const { user, volName, auth } = this.forms
      await updateUserPolicy({
        user_id: user,
        volume: volName,
        cluster_name: this.clusterName,
        policy: auth,
      })
      this.$message.success('授权成功')
      this.$emit('refresh')
      this.close()
    },
    close() {
      this.clearData()
      this.dialogFormVisible = false
    },
    async remoteMethod(query) {
      this.getUserNameList(query)
    },
    async getUserNameList(query) {
      const res = await getUserNameList({
        keywords: query,
        cluster_name: this.clusterName,
      })
      this.userList = (res.data || []).map((item) => {
        return {
          label: item,
          value: item,
        }
      })
    },
  },
}
</script>
<style lang="scss" scoped>
.input {
  width: 60%;
}
.dialog-footer {
  text-align: center;
}
</style>
