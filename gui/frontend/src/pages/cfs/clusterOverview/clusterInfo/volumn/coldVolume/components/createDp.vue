<template>
  <el-dialog
    title="创建dp"
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
        <el-input v-model="forms.name" disabled class="input"></el-input>
      </el-form-item>
      <el-form-item label="数量:" prop="count">
        <el-input
          v-model.number="forms.count"
          class="input"
          placeholder="请输入数量"
        ></el-input>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button ref="pol" type="primary" @click="doCheck">确 定</el-button>
      <el-button ref="pol" type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { createDataPartition } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      userList: [],
      forms: {
        name: '',
        count: '',
      },
      dialogFormVisible: false,
    }
  },
  computed: {
    rules() {
      return {
        count: [
          {
            required: true,
            message: '请输入数量',
            trigger: 'blur',
          },
          {
            max: 50,
            type: 'number',
            trigger: 'blur',
            message: '上限值为50'
          }
        ],
        name: [
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
    },
    clearData() {
      this.forms = {
        name: '',
        count: '',
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const { name, count } = this.forms
      await createDataPartition({
        name,
        count: +count,
        cluster_name: this.clusterName,
      })
      this.$message.success('创建dp成功')
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
  width: 60%;
}
.dialog-footer {
  text-align: center;
}
</style>
