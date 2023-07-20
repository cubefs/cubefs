<template>
  <el-dialog
    v-if="dialogVisible"
    :title="title"
    :visible.sync="dialogVisible"
    width="540px"
  >
    <el-form ref="form" :model="form" label-width="85px" :rules="rules" style="width: 450px">
      <el-form-item label="用户名" prop="user_name">
        <el-input v-model="form.user_name" :disabled="type !== 'create'" placeholder="请输入用户名，推荐用姓名或工号" />
      </el-form-item>
      <el-form-item label="手机" prop="phone">
        <el-input v-model="form.phone" :disabled="type === 'delete'" placeholder="请输入手机" />
      </el-form-item>
      <el-form-item label="邮箱" prop="email">
        <el-input v-model="form.email" :disabled="type === 'delete'" placeholder="请输入邮箱" />
      </el-form-item>
      <el-form-item label="用户角色" prop="roles">
        <el-select
          v-model="form.role_ids"
          multiple
          filterable
          :disabled="type === 'delete'"
        >
          <el-option
            v-for="item in roleList"
            :key="item.id"
            :label="item.role_name"
            :value="item.id"
          />
        </el-select>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <el-button @click="dialogVisible = false">取 消</el-button>
      <el-button v-if="type !== 'delete'" type="primary" @click="submit">确 定</el-button>
      <el-button v-else type="danger" @click="submit">删 除</el-button>
    </span>
  </el-dialog>
</template>

<script>
import {
  getRoleList,
  userCreate,
  userUpdate,
  userDelete,
} from '@/api/auth'
export default {
  name: 'UserDialog',
  data() {
    return {
      dialogVisible: false,
      id: '',
      type: '',
      form: {
        user_name: '',
        phone: '',
        email: '',
        role_ids: [],
      },
      roleList: [],
      rules: {
        user_name: [{ required: true, message: '请输入用户名', trigger: 'blur' }],
        phone: [{ required: true, message: '请输入手机号', trigger: 'blur' }],
        email: [{ required: true, message: '请输入邮箱', trigger: 'blur' }],
      },
    }
  },
  computed: {
    title() {
      let title = ''
      switch (this.type) {
        case 'create':
          title = '添加用户'
          break
        case 'edit':
          title = '编辑'
          break
        case 'delete':
          title = '删除用户'
          break
      }
      return title
    },
  },
  mounted() {
    this.getRoleList()
  },
  methods: {
    initDialog() {
      this.type = 'create'
      this.form = {
        user_name: '',
        phone: '',
        email: '',
        role_ids: [],
      }
      this.dialogVisible = true
    },
    async editDialog(userInfo) {
      // eslint-disable-next-line camelcase
      const { id, user_name, phone, email, roles } = userInfo
      this.form = {
        user_name,
        phone,
        email,
        role_ids: roles.map(item => item.id),
      }
      this.id = id
      this.type = 'edit'
      this.dialogVisible = true
    },
    async deleteDialog(userInfo) {
      // eslint-disable-next-line camelcase
      const { id, user_name, phone, email, roles } = userInfo
      this.form = {
        user_name,
        phone,
        email,
        role_ids: roles.map(item => item.id),
      }
      this.id = id
      this.type = 'delete'
      this.dialogVisible = true
    },
    async getRoleList() {
      const { data: { roles } } = await getRoleList({ page_size: 500 })
      this.roleList = roles
    },
    async submit() {
      await this.$refs.form.validate()
      if (this.type === 'create') {
        await userCreate({
          ...this.form,
          password: 'abcd1234',
        })
        this.$message.success('创建成功')
        this.$emit('submit')
      } else if (this.type === 'edit') {
        await userUpdate({
          ...this.form,
          id: this.id,
        })
        this.$message.success('编辑成功')
        this.$emit('submit')
      } else if (this.type === 'delete') {
        await userDelete({
          ids: [this.id],
        })
        this.$message.success('删除成功')
        this.$emit('submit')
      }
      this.dialogVisible = false
    },
  },
}

</script>
<style scoped lang='scss'>
</style>
