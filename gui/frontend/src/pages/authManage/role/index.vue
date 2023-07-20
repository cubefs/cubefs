<template>
  <el-card>
    <div class="bar">
      <el-input v-model="role_name" prefix-icon="el-icon-search" placeholder="请输入角色名" style="width: 240px" clearable />
      <div>
        <el-button v-auth="'AUTH_ROLE_DELETE'" type="text" icon="el-icon-delete" style="color: #ed4014">删除</el-button>
        <el-button v-auth="'AUTH_ROLE_CREATE'" type="primary" icon="el-icon-plus" @click="addRole">添加角色</el-button>
      </div>
    </div>
    <o-page-table
      ref="table"
      :url="url"
      :columns="tableColumns"
      :form-data="{
        role_name_like: role_name
      }"
      data-key="roles"
      method="get"
      page-key="page"
      page-size-key="page_size"
      total-key="count"
      :after-send-hook="afterSendHook"
    />
    <RoleDialog ref="dialog" @submit="onSubmit" />
  </el-card>
</template>

<script>
import RoleDialog from './roleDialog.vue'
import lang from '@/i18n/lang/zh'
export default {
  components: {
    RoleDialog,
  },
  data() {
    return {
      url: '/api/cubefs/console/auth/role/list',
      role_name: '',
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
          title: 'ID',
          key: 'id',
          width: 120,
        },
        {
          title: '角色名称',
          key: 'role_name',
          width: 200,
        },
        {
          title: '权限',
          key: 'permissions',
          render: (h, { row }) => {
            return <div>
              {
                row.permissions.map((item, index) => {
                  return <span>{ this.$t(item.auth_code) }{ index < row.permissions.length - 1 ? '、' : '' }</span>
                })
              }
            </div>
          },
        },
        {
          title: '操作',
          key: '',
          align: 'center',
          width: 200,
          render: (h, { row }) => {
            return <div>
              <el-button v-auth="AUTH_ROLE_UPDATE" type='text' onClick={() => this.editRole(row)}>编辑</el-button>
              <el-button v-auth="AUTH_ROLE_DELETE" type='text' style={ [1, 2, 3].includes(row.id) ? {} : { color: '#ed4014' } } disabled={[1, 2, 3].includes(row.id)} onClick={() => this.deleteRole(row)}>删除</el-button>
            </div>
          },
        },
      ]
    },
  },
  methods: {
    afterSendHook(data) {
      const codeList = Object.keys(lang)
      data.forEach(item => {
        item.origin_permissions = item.permissions
        item.permissions = item.permissions.filter(_item => _item.is_check).filter(_item => codeList.includes(_item.auth_code))
      })
    },
    addRole() {
      this.$refs.dialog.initDialog()
    },
    editRole(row) {
      this.$refs.dialog.editDialog(row)
    },
    deleteRole(row) {
      this.$refs.dialog.deleteDialog(row)
    },
    async onSubmit() {
      this.$refs.table.search()
      await this.$store.dispatch('moduleUser/setAuth')
    },
    selectInit(row, index) {
      if ([1, 2, 3].includes(row.id)) {
        return false
      } else {
        return true
      }
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
