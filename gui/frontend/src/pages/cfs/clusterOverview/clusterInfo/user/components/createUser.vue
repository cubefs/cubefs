<template>
  <el-dialog
    title="创建租户"
    :visible.sync="dialogFormVisible"
    width="800px"
    :append-to-body="true"
    @closed="clearData"
  >
    <el-form
      ref="form"
      :model="forms"
      :rules="rules"
      label-width="25%"
      class="mid-block"
    >
      <el-form-item label="租户ID:" prop="user">
        <el-input
          v-model="forms.user"
          class="input"
          placeholder="请输入租户ID"
        ></el-input>
        <el-tooltip
          class="item"
          effect="dark"
          content="必须以字母开头,可有下划线,数字,字母"
          placement="top"
        >
          <i class="el-icon-question fontS16"></i>
        </el-tooltip>
      </el-form-item>
      <el-form-item label="租户说明:" prop="description">
        <el-input
          v-model="forms.description"
          type="textarea"
          :rows="2"
          placeholder="请输入租户说明"
          class="input"
        ></el-input>
      </el-form-item>
      <el-form-item label="租户类型:" prop="accountType">
        <el-radio-group v-model="forms.accountType">
          <!-- <el-radio :label="1">root</el-radio>
          <el-radio :label="2">admin</el-radio> -->
          <el-radio :label="3">normal</el-radio>
        </el-radio-group>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button ref="pol" type="primary" @click="doCheck">确 定</el-button>
      <el-button ref="pol" type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { createUser } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      userList: [],
      forms: {
        user: '',
        description: '',
        accountType: 3,
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
            trigger: 'blur',
            validator: (rule, value, cb) => {
              const reg = /^[a-zA-Z][a-zA-Z_0-9]*$/
              if (!value) {
                cb(new Error('请输入租户ID'))
              } else if (!reg.test(value)) {
                cb(new Error('必须以字母开头,可有下划线,数字,字母'))
              }
              cb()
            },
          },
        ],
      }
    },
  },
  methods: {
    initForm(val) {
      this.forms = { ...val }
    },
    open() {
      this.dialogFormVisible = true
    },
    clearData() {
      this.forms = {
        user: '',
        description: '',
        accountType: 3,
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const { user, description, accountType } = this.forms
      await createUser({
        id: user,
        type: accountType,
        description,
        cluster_name: this.clusterName,
      })
      this.$message.success('创建租户成功')
      this.$emit('refresh')
      this.close()
    },
    close() {
      this.clearData()
      this.dialogFormVisible = false
    },
  },
}
</script>
<style lang="scss" scoped>
.input {
  width: 65%;
}
.dialog-footer {
  text-align: center;
}
</style>
