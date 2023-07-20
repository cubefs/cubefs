<template>
  <el-card>
    <div class="bar">
      <el-input v-model="user_name" prefix-icon="el-icon-search" placeholder="请输入用户名" style="width: 240px" clearable />
      <div>
        <el-button v-auth="'AUTH_USER_DELETE'" type="text" icon="el-icon-delete" style="color: #ed4014">删除</el-button>
        <el-button type="primary" icon="el-icon-plus" @click="addUser">添加用户</el-button>
      </div>
    </div>
    <o-page-table
      ref="table"
      :url="url"
      :columns="tableColumns"
      :form-data="{
        user_name_like: user_name
      }"
      data-key="users"
      method="get"
      page-key="page"
      page-size-key="page_size"
      total-key="count"
    />
    <UserDialog ref="dialog" @submit="onSubmit" />
  </el-card>
</template>

<script>
import UserDialog from './userDialog.vue'
import { passwordUpdate } from '@/api/auth'
export default {
  components: {
    UserDialog,
  },
  data() {
    return {
      url: '/api/cubefs/console/auth/user/list',
      user_name: '',
    }
  },
  computed: {
    tableColumns() {
      return [
        {
          type: 'selection',
          align: 'center',
          width: 80,
          selectable: this.selectInit,
        },
        {
          title: '姓名',
          key: 'user_name',
          width: 200,
        },
        {
          title: '角色',
          key: 'roles',
          render: (h, { row }) => {
            return <div>
              {
                row.roles.map((item, index) => {
                  return <span>{ item.role_name }{ index < row.roles.length - 1 ? '、' : '' }</span>
                })
              }
            </div>
          },
        },
        {
          title: '手机',
          key: 'phone',
        },
        {
          title: '邮箱',
          key: 'email',
        },
        {
          title: '操作',
          key: '',
          width: 200,
          render: (h, { row }) => {
            return <div>
              <el-button v-auth="AUTH_USER_UPDATE" type='text' onClick={() => this.editUser(row)}>编辑</el-button>
              <el-button v-auth="AUTH_USER_PASSWORD_UPDATE" type='text' onClick={() => this.updatePassword(row)}>修改密码</el-button>
              <el-button v-auth="AUTH_USER_DELETE" type='text' style={ row.id === 1 ? {} : { color: '#ed4014' } } disabled={row.id === 1} onClick={() => this.deleteUser(row)}>删除</el-button>
            </div>
          },
        },
      ]
    },
  },
  methods: {
    addUser() {
      this.$refs.dialog.initDialog()
    },
    editUser(row) {
      this.$refs.dialog.editDialog(row)
    },
    deleteUser(row) {
      this.$refs.dialog.deleteDialog(row)
    },
    selectInit(row, index) {
      if ([1].includes(row.id)) {
        return false
      } else {
        return true
      }
    },
    updatePassword(row) {
      this.$prompt('<div>请输入新密码<div style="color: red; font-size: 12px;">密码长度大于等于8小于等于16<br />密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上</div><div>', '修改密码', {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        inputValidator: this.checkPassword,
        inputErrorMessage: '密码格式不正确',
        dangerouslyUseHTMLString: true,
      }).then(async ({ value }) => {
        await passwordUpdate({
          user_name: row.user_name,
          password: value,
        })
        this.$message.success('修改密码成功')
      })
    },
    checkPassword(str) {
      // 密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上
      const regList = [/[0-9]/, /[A-Z]/, /[a-z]/, /[~!@#$%^&*_.?]/]
      let num = 0
      regList.forEach(item => {
        if (item.test(str)) {
          num++
        }
      })
      if (str.length > 7 && str.length < 17 && num > 1) {
        return true
      }
      return false
    },
    async onSubmit() {
      this.$refs.table.search()
      await this.$store.dispatch('moduleUser/setAuth')
    },
  },
}

</script>
<style scoped lang='scss'>
.bar {
  display: flex;
  justify-content: space-between;
}
</style>
