<template>
  <el-dialog
    :title="`${isNew === true ? '新增' : '编辑'}规则`"
    :visible.sync="dialogVisible"
    width="680px"
    :before-close="handleClose"
  >
    <el-form ref="form" :model="form" label-width="120px" :rules="rules">
      <el-form-item label="来源" prop="AllowedOrigins">
        <el-input v-model.trim="form.AllowedOrigins" type="textarea"></el-input>
        <span class="color9">来源可以设置多个，以逗号(,)分割，最多能有一个通配符 *，但不建议填写通配符 *</span>
      </el-form-item>
      <el-form-item label="允许 Methods" prop="AllowedMethods">
        <el-checkbox-group v-model="form.AllowedMethods">
          <el-checkbox label="GET"></el-checkbox>
          <el-checkbox label="POST"></el-checkbox>
          <el-checkbox label="PUT"></el-checkbox>
          <el-checkbox label="DELETE"></el-checkbox>
          <el-checkbox label="HEAD"></el-checkbox>
        </el-checkbox-group>
      </el-form-item>
      <el-form-item label="允许 Headers" prop="AllowedHeaders">
        <el-input v-model="form.AllowedHeaders" type="textarea"></el-input>
        <span class="color9">允许 Headers 可以设置多个，以逗号(,)分割，最多能有一个通配符 *</span>
      </el-form-item>
      <el-form-item label="暴露 Headers">
        <el-input v-model="form.ExposeHeaders" type="textarea"></el-input>
        <span class="color9">暴露 Headers 可以设置多个，以逗号(,)分割，不允许出现通配符 *</span>
      </el-form-item>
      <el-form-item label="缓存时间（秒）">
        <el-input v-model="form.MaxAgeSeconds" type="number"></el-input>
      </el-form-item>
    </el-form>
    <span slot="footer" class="dialog-footer">
      <el-button @click="handleClose">取 消</el-button>
      <el-button type="primary" @click="handleApply">确 定</el-button>
    </span>
  </el-dialog>
</template>

<script>
import { setCors } from '@/api/cfs/cluster'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [mixin],
  data() {
    return {
      form: {
        AllowedOrigins: '',
        AllowedMethods: [],
        AllowedHeaders: '',
        ExposeHeaders: '',
        MaxAgeSeconds: 0
      },
      isNew: true,
      dialogVisible: false,
      allList: [],
      editIndex: 0,
      rules: {
        AllowedOrigins: [{
          required: true,
          message: '请输入来源',
          trigger: 'blur'
        }],
        AllowedMethods: [{
          required: true,
          message: '请选择Methods',
          trigger: 'blur'
        }]
      }
    }
  },
  methods: {
    init(isNew, obj, allList, index) {
      this.isNew = isNew
      if (!isNew) {
        this.form = {
          ...obj,
          AllowedOrigins: obj.AllowedOrigins.join(','),
          AllowedHeaders: obj.AllowedHeaders
            ? obj.AllowedHeaders.join(',')
            : '',
          ExposeHeaders: obj.ExposeHeaders ? obj.ExposeHeaders.join(',') : ''
        }
        this.editIndex = index
      }
      this.allList = allList
      this.dialogVisible = true
    },
    async handleApply() {
      await this.$refs.form.validate()
      // 检查
      if (this.form.AllowedOrigins.length) {
        const arr = this.form.AllowedOrigins.split(',')
        if (this.checkSingFun(arr)) {
          return
        }
      }

      if (this.form.AllowedHeaders.length) {
        const arr = this.form.AllowedHeaders.split(',')
        if (this.checkSingFun(arr)) {
          return
        }
      }

      if (this.form.ExposeHeaders.length) {
        const arr = this.form.ExposeHeaders.split(',')
        if (this.checkSingFun(arr)) {
          return
        }
      }
      this.setData()
    },
    async setData() {
      const { name, owner } = this.$route.query
      const newObj = {
        ...this.form,
        AllowedOrigins: this.form.AllowedOrigins.split(','),
        AllowedHeaders: this.form.AllowedHeaders.length
          ? this.form.AllowedHeaders.split(',')
          : null,
        ExposeHeaders: this.form.ExposeHeaders.length
          ? this.form.ExposeHeaders.split(',')
          : null,
        MaxAgeSeconds: Number(this.form.MaxAgeSeconds)
      }
      if (this.isNew) {
        this.allList.push(newObj)
      } else {
        this.allList.splice(this.editIndex, 1, newObj)
      }
      const data = this.allList
      const res = await setCors({
        cluster_name: this.clusterName,
        vol: name,
        user: owner,
        rules: data
      })

      this.$message.success(res.msg)
      this.handleClose()
    },
    handleClose() {
      this.form = {
        AllowedOrigins: '',
        AllowedMethods: [],
        AllowedHeaders: '',
        ExposeHeaders: '',
        MaxAgeSeconds: 0
      }
      this.allList = []
      this.$emit('get-data')
      this.dialogVisible = false
    },
    checkSingFun(arr) {
      for (let i = 0; i < arr.length; i++) {
        const sing = []
        for (let t = 0; t < arr[i].length; t++) {
          if (arr[i][t] === '*') {
            sing.push(t)
          }
        }
        if (sing.length > 1) {
          this.$message.warning(
            '每个以逗号分割的内容中，最多能有只一个通配符（*）'
          )
          return true
        }
        return false
      }
    }
  }
}
</script>

<style lang="scss" scoped>
</style>
