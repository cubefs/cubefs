<template>
  <section class="bucket_detail">
    <el-card>
      <ModuleBucketTitle :go-back="goBack" :bucket="volName">
        <span>所在集群：<span class="value-c">{{ clusterName }}</span></span>
      </ModuleBucketTitle>
      <el-alert
        title="提示"
        type="info"
        show-icon>
        <div>
          <p>文件管理操作需要先配置跨域才能放开，且跨域来源需包含访问域名，例如</p>
          <p>来源: {{ protocol }}//{{ host }}</p>
          <p>允许Methods: PUT,GET,POST,DELETE</p>
          <p>允许Headers: *</p>
        </div>
      </el-alert>
      <el-tabs v-model="activeName">
        <el-tab-pane label="跨域设置" name="cors"></el-tab-pane>
        <el-tab-pane label="文件管理" name="file" :disabled="disabledFile"></el-tab-pane>
      </el-tabs>
      <file v-if="activeName === 'file'"></file>
      <Cors v-if="activeName === 'cors'" @get-cors="getCors"></Cors>
    </el-card>
  </section>
</template>

<script>
import ModuleBucketTitle from './components/moduleBucketTitle'
import mixin from '@/pages/cfs/clusterOverview/mixin'
import File from './file'
import Cors from './Cors/Index.vue'
export default {
  components: {
    ModuleBucketTitle, File, Cors
  },
  mixins: [mixin],
  data() {
    return {
      id: '',
      zone: '',
      activeName: 'cors',
      volName: '',
      corsRules: [],
      disabledFile: true,
    }
  },
  computed: {
    host() {
      return window.location.host
    },
    protocol() {
      return window.location.protocol
    },
  },
  watch: {
    corsRules: {
      handler(val) {
        const { host, protocol } = window.location
        const ruleItem = val.find(item => item.AllowedOrigins.find(origins => origins.includes(`${protocol}//${host}`)))
        if (ruleItem) {
          this.disabledFile = false
        } else {
          this.disabledFile = true
        }
      },
      deep: true,
    },
  },
  created() {
    this.volName = this.$route.query.name
  },
  methods: {
    goBack() {
      this.$router.push({ name: 'fileManageList' })
    },
    getCors(corsRules) {
      this.corsRules = corsRules
    },
  },
}
</script>

<style lang="scss" scoped>
.bucket_detail {
  h2 {
    padding-left: 4px;
    border-left: 4px solid #2fc29b;
    margin-bottom: 16px;
  }
  .bucket_msg {
    color: #999999;
    margin-bottom: 40px;
    span {
      margin-right: 20px;
      font-size: 14px;
      .value-c {
        color: #666666;
        margin-right: 0;
      }
    }
  }
}
::v-deep .el-card {
  margin-bottom: 16px;
  border: 1px solid #f6f6f6;
}
</style>
