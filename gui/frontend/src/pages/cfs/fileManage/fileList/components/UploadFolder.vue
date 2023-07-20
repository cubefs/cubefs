<template>
  <div>
    <el-dialog
      title="新建文件夹"
      :visible.sync="dialogVisible"
      width="440px"
      :before-close="handleClose"
    >
      <el-input v-model="folderName" placeholder="请输入文件夹名字"></el-input>
      <span slot="footer" class="dialog-footer">
        <el-button @click="dialogVisible = false">取 消</el-button>
        <el-button type="primary" @click="handleConfirm">确 定</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import { dirsCreate } from '@/api/cfs/cluster'
import { mapGetters } from 'vuex'
import mixin from '@/pages/cfs/clusterOverview/mixin'
export default {
  data() {
    return {
      dialogVisible: false,
      folderName: '',
      prefix: '',
    }
  },
  mixins: [mixin],
  computed: {
    ...mapGetters({
      env: 'getNetEnv',
    }),
  },
  methods: {
    handleClose(done) {
      this.folderName = ''
      this.dialogVisible = false
      this.$emit('reload')
    },
    openUploadView(id, prefix) {
      this.prefix = prefix
      this.dialogVisible = true
    },
    async handleConfirm() {
      if (!this.folderName) return
      const { name } = this.$route.query
      const newParam = {
        cluster_name: this.clusterName,
        vol: name,
        prefix: this.prefix,
        dir_name: this.folderName,
        user: this.$route.query.owner,
      }
      await dirsCreate(newParam)

      this.handleClose()
    },
  },
}
</script>
