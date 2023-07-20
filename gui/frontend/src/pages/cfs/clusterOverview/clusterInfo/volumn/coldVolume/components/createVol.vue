<template>
  <el-dialog
    title="创建卷"
    :visible.sync="dialogFormVisible"
    width="800px"
    @closed="clearData"
  >
    <el-form
      ref="form"
      :model="forms"
      :rules="rules"
      label-width="25%"
      class="mid-block"
    >
      <el-form-item label="集群:" prop="clusterName">
        <el-input v-model="forms.clusterName" class="input" disabled></el-input>
      </el-form-item>
      <el-form-item label="卷名:" prop="volName">
        <el-input
          v-model="forms.volName"
          class="input"
          placeholder="请输入卷名"
        ></el-input>
        <el-tooltip
          class="item"
          effect="dark"
          content="必须以字母开头,可有下划线,数字,字母,中划线"
          placement="top"
        >
          <i class="el-icon-question fontS16"></i>
        </el-tooltip>
        <!-- <el-checkbox v-model="forms.isCrossZone">跨Zone</el-checkbox>
        <el-checkbox
          v-model="forms.defaultPriority"
          :disabled="errDisable"
        >故障域</el-checkbox> -->
      </el-form-item>
      <el-form-item label="容量:" prop="size">
        <el-input
          v-model.number="forms.size"
          class="input"
          placeholder="请输入容量"
        ></el-input>&nbsp; GB
      </el-form-item>
      <el-form-item label="缓存容量:" prop="cache_cap">
        <el-input
          v-model.number="forms.cache_cap"
          class="input"
          placeholder="请输入缓存容量"
        ></el-input>&nbsp; GB
      </el-form-item>
      <el-form-item label="owner:" prop="owner">
        <el-select v-model="forms.owner">
          <el-option v-for="item in userList" :label="item.user_id" :value="item.user_id" :key="item.user_id"></el-option>
        </el-select>
      </el-form-item>
      <el-form-item label="业务:" prop="work">
        <el-input
          v-model="forms.work"
          type="textarea"
          :rows="2"
          placeholder="请输入业务内容"
          class="input"
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
import { createVol, getClusterIsErrArea, getUserList } from '@/api/cfs/cluster'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      clusterList: [],
      forms: {
        clusterName: '',
        isCrossZone: '',
        defaultPriority: '',
        volName: '',
        size: '',
        cache_cap: '',
        work: '',
        owner: '',
      },
      dialogFormVisible: false,
      zoneList: [],
      errDisable: true,
      userList: [],
    }
  },
  computed: {
    rules() {
      return {
        clusterName: [
          {
            required: true,
            message: '请输入集群名称',
            trigger: 'blur',
          },
        ],
        volName: [
          {
            required: true,
            trigger: 'blur',
            validator: (rule, value, cb) => {
              const reg = /^[a-zA-Z][a-zA-Z_0-9-]*$/
              if (!value) {
                cb(new Error('请输入卷名称'))
              } else if (!reg.test(value)) {
                cb(new Error('必须以字母开头,可有下划线,数字,字母,中划线'))
              }
              cb()
            },
          },
        ],
        size: [
          {
            required: true,
            message: '请输入容量',
            trigger: 'blur',
          },
        ],
        cache_cap: [
          {
            required: true,
            message: '请输入缓存容量',
            trigger: 'blur',
          },
        ],
        work: [
          {
            required: true,
            message: '请填写业务',
            trigger: 'blur',
          },
        ],
        owner: [
          {
            required: true,
            message: '请选择用户',
            trigger: 'blur',
          },
        ]
      }
    },
  },
  watch: {
    async 'forms.isCrossZone'(val) {
      if (val) {
        const res = await getClusterIsErrArea({
          cluster_name: this.clusterName,
        })
        if (res?.data?.DomainOn) {
          this.errDisable = false
          return
        }
        this.errDisable = true
      } else {
        this.errDisable = true
      }
    },
  },
  created() {
    this.getClusterList()
    this.queryUserList()
  },
  methods: {
    async queryUserList() {
      const res = await getUserList({
        cluster_name: this.clusterName,
      })
      this.userList = res.data.users || []
    },
    async getClusterList() {
      this.forms.clusterName = this.clusterName
    },
    initForm(val) {
      this.forms = { ...val }
    },
    open() {
      this.dialogFormVisible = true
    },
    clearData() {
      Object.keys(this.forms).forEach((item) => {
        if (item !== 'clusterName') this.forms[item] = ''
      })
    },
    async doCheck() {
      await this.$refs.form.validate()
      const {
        clusterName,
        volName,
        size,
        work,
        owner,
        cache_cap,
      } = this.forms
      await createVol({
        cluster_name: clusterName,
        name: volName,
        cache_cap: +cache_cap || 0,
        capacity: +size || 0,
        business: work,
        owner,
        vol_type: 1,
      })
      this.$message.success('创建成功')
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
