<!--
 * @Author: willerwu wuweile@oppo.com
 * @Date: 2023-02-14 15:07:50
 * @LastEditors: wuweile wuweile@oppo.com
 * @LastEditTime: 2023-05-24 10:33:11
 * @FilePath: /cloud-file-front/src/pages/cfs/clusterOverview/clusterInfo/volumn/components/updateVol.vue
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
-->
<template>
  <el-dialog
    title="更新卷"
    :visible.sync="dialogFormVisible"
    width="800px"
    :destroy-on-close="true"
    @closed="close"
  >
    <el-form
      ref="form"
      :model="formData"
      :rules="rules"
      label-width="25%"
      class="mid-block"
    >
      <el-form-item prop="CacheCapacity" label="卷cache容量大小">
        <el-input v-model.number="formData.CacheCapacity"></el-input>
      </el-form-item>
      <el-form-item prop="CacheThreshold" label="cacheThreeshold">
        <el-input v-model.number="formData.CacheThreshold"></el-input>
      </el-form-item>
      <el-form-item prop="CacheTtl" label="卷cache淘汰时间">
        <el-input v-model.number="formData.CacheTtl"></el-input>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button ref="pol" type="primary" :loading="loading" @click="save">确 定</el-button>
      <el-button ref="pol" type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { updateVol, getVolDetail } from '@/api/cfs/cluster'
import mixin from '../../../mixin'
export default {
  mixins: [mixin],
  data() {
    return {
      dialogFormVisible: false,
      rowDetail: {},
      volDetail: {},
      formData: {
        CacheCapacity: undefined,
        CacheThreshold: undefined,
        CacheTtl: undefined,
      },
      rules: {
        CacheCapacity: [
          { required: true, message: '必填项', trigger: 'blur' },
        ],
        CacheThreshold: [
          { required: true, message: '必填项', trigger: 'blur' },
        ],
        CacheTtl: [
          { required: true, message: '必填项', trigger: 'blur' },
        ],
      },
      loading: false,
    }
  },
  computed: {

  },
  watch: {

  },
  created() {

  },
  methods: {
    close() {
      this.dialogFormVisible = false
    },
    open() {
      this.dialogFormVisible = true
    },
    async init(rowDetail) {
      this.rowDetail = rowDetail
      this.open()
      await this.getVolumnDetail()
      this.formData = {
        CacheCapacity: this.volDetail.CacheCapacity,
        CacheThreshold: this.volDetail.CacheThreshold,
        CacheTtl: this.volDetail.CacheTtl,
      }
    },
    async getVolumnDetail() {
      const res = await getVolDetail({
        name: this.rowDetail.name,
        cluster_name: this.clusterName,
      })
      this.volDetail = res.data || {}
    },
    async save() {
      try {
        await this.$refs.form.validate()
        const params = {
          ...this.formData,
        }
        this.loading = true
        await updateVol(params)
        this.loading = false
        this.$message.success('更新成功')
        this.$emit('refresh')
        this.close()
      } catch (e) {
        this.loading = false
      }
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
