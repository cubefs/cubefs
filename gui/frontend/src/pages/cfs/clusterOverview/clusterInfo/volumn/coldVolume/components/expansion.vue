<template>
  <el-dialog
    :title="title"
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
      <el-form-item label="卷名:" prop="volName">
        <el-input v-model="forms.volName" disabled class="input"></el-input>
      </el-form-item>
      <el-form-item label="当前容量:" prop="size">
        <el-input v-model="forms.size" disabled class="input"></el-input>&nbsp;
        GB
      </el-form-item>
      <el-form-item label="目标容量:" prop="targetSize">
        <el-input v-model.number="forms.targetSize" class="input"></el-input>&nbsp; GB
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button ref="pol" type="primary" @click="doCheck">确 定</el-button>
      <el-button ref="pol" type="primary" @click="close">取 消</el-button>
    </div>
  </el-dialog>
</template>
<script>
import { expandVol, shrinkVol } from '@/api/cfs/cluster'
import { getGBSize_B } from '@/utils'
import Mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  mixins: [Mixin],
  data() {
    return {
      type: 'expansion',
      forms: {
        volName: '',
        size: '',
        targetSize: '',
      },
      dialogFormVisible: false,
      zoneList: [
        {
          text: '东莞智享',
          value: 'tests',
        },
      ],
    }
  },
  computed: {
    title() {
      return this.type === 'expansion' ? '扩容' : '缩容'
    },
    rules() {
      return {
        volName: [
          {
            required: true,
            message: '请输入卷名称',
            trigger: 'blur',
          },
        ],
        targetSize: [
          {
            required: true,
            trigger: 'blur',
            validator: (rule, value, callback) => {
              const isError =
                this.type === 'expansion'
                  ? +value < +this.forms.size
                  : +value > +this.forms.size
              if (!value) {
                callback(new Error('请输入容量'))
              } else if (isError) {
                callback(
                  new Error(
                    `输入的容量不能${
                      this.type === 'expansion' ? '小' : '大'
                    }于当前容量`,
                  ),
                )
              } else {
                callback()
              }
            },
          },
        ],
      }
    },
  },
  methods: {
    initForm(val) {
      // 初始化默认值和单位
      const { size } = val
      this.forms = { ...this.forms, ...val }
      this.forms.targetSize = getGBSize_B(size)
      this.forms.size = getGBSize_B(size)
    },
    open(type = 'expansion') {
      this.dialogFormVisible = true
      this.type = type
    },
    clearData() {
      this.forms = {
        volName: '',
        size: '',
        targetSize: '',
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const { volName, targetSize } = this.forms
      const params = {
        name: volName,
        capacity: +targetSize || 0,
        cluster_name: this.clusterName,
      }
      if (this.title === '扩容') {
        await expandVol(params)
      } else {
        await shrinkVol(params)
      }
      this.$message.success(this.title + '成功')
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
  width: 70%;
}
.dialog-footer {
  text-align: center;
}
</style>
