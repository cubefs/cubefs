<template>
  <el-dialog
    title="创建Mp"
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
      <el-form-item label="元数据分片值:" prop="start">
        <el-input
          v-model.number="forms.start"
          class="input"
          placeholder="请输入分片值"
        ></el-input>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button type="primary" @click="doCheck">确 定</el-button>
      <el-button type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { createMetaPartition } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      userList: [],
      forms: {
        name: '',
        start: '',
      },
      dialogFormVisible: false,
    }
  },
  computed: {
    rules() {
      return {
        start: [
          {
            required: true,
            message: '请输入分片值',
            trigger: 'blur',
          },
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
        start: '',
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const { name, start } = this.forms
      await createMetaPartition({
        name,
        start: +start,
        cluster_name: this.clusterName,
      })
      this.$message.success('创建mp成功')
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
