<template>
  <el-dialog
    v-if="dialogVisible"
    :title="title"
    :visible.sync="dialogVisible"
    width="950px"
  >
    <el-form ref="form" :model="form" label-width="90px" :rules="rules" style="width: 900px">
      <el-form-item label="角色Code" prop="role_code">
        <el-input v-model="form.role_code" :disabled="type !== 'create'" placeholder="请输入角色Code,只能包含输入英文、数字和下划线" style="width: 790px" />
      </el-form-item>
      <el-form-item label="角色名称" prop="role_name">
        <el-input v-model="form.role_name" :disabled="type !== 'create'" placeholder="请输入角色名称" style="width: 790px" />
      </el-form-item>
      <!-- <div>角色权限</div>
      <div v-for="item in codeList" :key="item.title">
        <el-form-item :label="item.title">
          <el-checkbox-group v-model="form.permission_ids" :disabled="type === 'delete'">
            <el-checkbox
              v-for="_item in item.children"
              :key="_item"
              class="permission_checkbox"
              :label="_item"
              :title="$t(_item)"
            >{{ $t(_item) }}</el-checkbox>
          </el-checkbox-group>
        </el-form-item>
      </div> -->
      <el-form-item label="角色权限" prop="roles">
        <el-table :data="codeList" style="width: 97%" :header-cell-style="{ background: '#f8f9fc', color: '#606266', fontWeight: '550' }">
          <el-table-column prop="title" label="模块" width="120" />
          <el-table-column label="权限">
            <template slot-scope="scope">
              <el-checkbox
                v-for="item in scope.row.children"
                :key="item.id"
                v-model="item.checked"
                :title="$t(item.id)"
                :disabled="type === 'delete'"
                @change="value => {
                  itemSelect(value, item, scope.row)
                }"
              >
                {{ $t(item.id) }}
              </el-checkbox>
            </template>
          </el-table-column>
          <el-table-column label="全选" width="100">
            <template slot-scope="scope">
              <el-checkbox
                v-model="scope.row.all"
                :disabled="type === 'delete'"
                @change="
                  value => {
                    allSelect(value, scope.row)
                  }
                "
              />
            </template>
          </el-table-column>
        </el-table>
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
  getAuthList,
  roleCreate,
  roleUpdate,
  roleDelete,
} from '@/api/auth'
import { codeList, backendAuthids } from '@/utils/auth'

export default {
  name: 'UserDialog',
  data() {
    return {
      dialogVisible: false,
      id: '',
      type: '',
      form: {
        role_code: '',
        role_name: '',
        permission_ids: [],
      },
      authList: [],
      rules: {
        role_code: [
          { required: true, message: '请输入角色Code', trigger: 'blur' },
          { pattern: /^[a-zA-Z0-9_]+$/, message: '仅允许包含输入英文、数字和下划线', trigger: 'blur' },
        ],
        role_name: [{ required: true, message: '请输入角色名称', trigger: 'blur' }],
      },
      codeList: [],
      codeMap: {},
    }
  },
  computed: {
    title() {
      let title = ''
      switch (this.type) {
        case 'create':
          title = '添加角色'
          break
        case 'edit':
          title = '修改角色权限'
          break
        case 'delete':
          title = '删除角色'
          break
      }
      return title
    },
  },
  mounted() {
    this.getAuthList()
  },
  methods: {
    initDialog() {
      this.type = 'create'
      this.form = {
        role_code: '',
        role_name: '',
        permission_ids: [],
      }
      this.codeList = codeList.map(item => ({
        title: item.title,
        children: item.children.map(_item => ({
          checked: false,
          id: _item,
        })),
        all: false,
      }))
      this.dialogVisible = true
    },
    async editDialog(roleInfo) {
      // eslint-disable-next-line camelcase
      const { id, role_code, role_name, origin_permissions } = roleInfo
      this.form = {
        role_code,
        role_name,
        permission_ids: origin_permissions.map(item => item.id),
      }
      this.echoCode()
      this.id = id
      this.type = 'edit'
      this.dialogVisible = true
    },
    async deleteDialog(roleInfo) {
      // eslint-disable-next-line camelcase
      const { id, role_code, role_name, origin_permissions } = roleInfo
      this.form = {
        role_code,
        role_name,
        permission_ids: origin_permissions.map(item => item.id),
      }
      this.echoCode()
      this.id = id
      this.type = 'delete'
      this.dialogVisible = true
    },
    async getAuthList() {
      const { data: { permissions } } = await getAuthList({ page_size: 500 })
      this.codeMap = permissions.reduce((acc, cur) => {
        acc[cur.auth_code] = cur.id
        return acc
      }, {})
    },
    echoCode() {
      this.codeList = codeList.map(item => ({
        title: item.title,
        children: item.children.map(_item => ({
          checked: false,
          id: _item,
        })),
        all: false,
      }))
      this.codeList.forEach(item => {
        let flag = true
        item.children.forEach(_item => {
          if (this.form.permission_ids.findIndex(id => id === this.codeMap[_item.id]) !== -1) {
            _item.checked = true
          } else {
            flag = false
          }
        })
        if (flag) {
          item.all = true
        }
      })
    },
    deleteCode(code) {
      const index = this.form.permission_ids.findIndex(item => item === this.codeMap[code])
      if (index !== -1) {
        this.form.permission_ids.splice(index, 1)
      }
    },
    addCode(code) {
      const index = this.form.permission_ids.findIndex(item => item === this.codeMap[code])
      if (index === -1) {
        this.form.permission_ids.push(this.codeMap[code])
      }
    },
    itemSelect(value, item, row) {
      if (value) {
        this.addCode(item.id)
      } else {
        this.deleteCode(item.id)
      }
      row.all = !row.children.some(item => !item.checked)
    },
    allSelect(value, { children }) {
      children.forEach(item => {
        if (value) {
          this.addCode(item.id)
        } else {
          this.deleteCode(item.id)
        }
        item.checked = value
      })
    },
    async submit() {
      await this.$refs.form.validate()
      if (this.type === 'create') {
        await roleCreate({
          ...this.form,
          permission_ids: [...this.form.permission_ids, ...backendAuthids],
          password: 'abcd1234',
        })
        this.$message.success('创建成功')
        this.$emit('submit')
      } else if (this.type === 'edit') {
        await roleUpdate({
          ...this.form,
          id: this.id,
        })
        this.$message.success('编辑成功')
        this.$emit('submit')
      } else if (this.type === 'delete') {
        await roleDelete({
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
::v-deep .el-checkbox {
  margin: 0;
  display: inline-block;
  width: 175px;
  box-sizing: border-box;
  .el-checkbox__label {
    max-width: 145px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    vertical-align: middle;
  }
}
</style>
