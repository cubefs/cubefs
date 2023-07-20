<!--
 * @Author: willerwu wuweile@oppo.com
 * @Date: 2023-02-10 16:12:53
 * @LastEditors: willerwu wuweile@oppo.com
 * @LastEditTime: 2023-02-14 14:31:07
 * @FilePath: /cloud-file-front/src/pages/cfs/clusterOverview/clusterInfo/dataManage/components/NodeMigrate.vue
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
-->
<template>
  <el-dialog
    :append-to-body="true"
    :title="`${nodeType === 1 ? '' : '元'}数据节点迁移`"
    :visible.sync="dialogFormVisible"
    width="600px"
    :destroy-on-close="true"
    @closed="onClose"
  >
    <el-form ref="form" :model="formData" label-width="100px">
      <el-form-item label="集群名称" prop="cluster_name">
        <el-input v-model="formData.cluster_name" :disabled="true"></el-input>
      </el-form-item>
      <el-form-item label="源地址" prop="src_addr">
        <el-input v-model="formData.src_addr" :disabled="true"></el-input>
      </el-form-item>
      <el-form-item label="目标地址" prop="target_addr" :rules="[{ required: true, message: '请选择目标地址', trigger: 'change' }]">
        <el-select v-model="formData.target_addr">
          <el-option
            v-for="item in addressList"
            :key="item"
            :label="item"
            :value="item"
          />
        </el-select>
      </el-form-item>
    </el-form>
    <template slot="footer">
      <el-button @click="onClose">取消</el-button>
      <el-button type="primary" @click="doMigrate">确定</el-button>
    </template>
  </el-dialog>
</template>
<script>
import { mapGetters } from 'vuex'
import { migrateMetaNode, migrateDataNode } from '@/api/cfs/cluster'
export default {
  props: {
    nodeType: { // 节点类型，1代表数据节点，2是元数据节点
      type: Number,
      default: 1,
    },
    addressList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      dialogFormVisible: false,
      formData: {},
    }
  },
  computed: {
    ...mapGetters('clusterInfoModule', {
      curClusterInfo: 'clusterInfog',
    }),
    clusterInfo() {
      return this.curClusterInfo.clusterInfo
    },
  },
  methods: {
    init(data) {
      this.formData = {
        ...data,
        cluster_name: this.clusterInfo.name,
        target_addr: '',
      }
      this.open()
    },
    open() {
      this.dialogFormVisible = true
    },
    onClose() {
      this.dialogFormVisible = false
    },
    doMigrate() {
      this.$refs.form.validate(async (valid) => {
        if (valid) {
          const migrateFunc = this.nodeType === 1 ? migrateDataNode : migrateMetaNode
          await migrateFunc(this.formData)
          this.$message.success('迁移成功')
          this.$emit('refresh')
          this.onClose()
        }
      })
    },
  },
}
</script>
<style lang="scss" scoped>
.m-b {
  margin-bottom: 10px;
}

.m-r {
  padding-right: 20px;
}
</style>
