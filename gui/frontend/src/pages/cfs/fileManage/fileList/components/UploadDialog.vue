<template>
  <div>
    <el-dialog
      v-if="dialogVisible"
      :title="reloadTitle"
      :visible.sync="dialogVisible"
      :before-close="handleClose"
      width="35%"
    >
      <el-upload
        class="upload-demo uploadS"
        :file-list="fileList"
        :show-file-list="false"
        :http-request="doUploadHttp"
        action=""
        multiple
        list-type="picture"
      >
        <el-button size="small" type="primary">点击上传</el-button>
      </el-upload>
      <ul class="file_list_wrap">
        <li v-for="(i, index) in fileList" :key="index" class="file_list">
          <div v-show="i.url">
            <div v-if="imgOrIcon(i.name)" class="file_wrap">
              <img :src="i.url" class="file_img" />
            </div>
            <div v-else class="file_wrap">
              <i class="el-icon-document"></i>
            </div>
          </div>
          <div v-show="!i.url" class="loading_wrap">
            <i
              :class="{
                'el-icon-loading': i.isFileUploadLoading,
                'el-icon-loading-stop': !i.isFileUploadLoading,
                fontS30: true,
              }"
            ></i>
          </div>
          <div class="nameClass">{{ i.name }}</div>
          <div>
            <el-progress
              :text-inside="true"
              :stroke-width="15"
              :percentage="i.progress"
              status="success"
            ></el-progress>
          </div>
        </li>
      </ul>
    </el-dialog>
  </div>
</template>

<script>
import { getContentType } from './contenttype'
import { putFile } from './upload'
import { getChunkSize } from './common'
import {
  getDownloadSignedUrl,
  getUploadSignedUrl,
  getMultipartUploadSignedUrl,
  multipartUploadComplete,
} from '@/api/cfs/cluster'
import mixin from '@/pages/cfs/clusterOverview/mixin'
let that = null
export default {
  mixins: [mixin],
  props: {
    isShowOriginTipForUpload: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      dialogVisible: false,
      reloadTitle: '',
      fileList: [],
      abortFun: [],
      prefix: '',
      loadedTotal: 0,
    }
  },
  computed: {
  },
  beforeDestroy() {
    that = null
    this.abortUpload()
  },
  methods: {
    handleClose() {
      this.fileList = []
      this.dialogVisible = false
      this.loadedTotal = 0
      this.abortUpload() // 取消上传
      this.$emit('reload')
    },
    openUploadView(id, prefix) {
      this.reloadTitle = '上传文件至 ' + id
      this.prefix = prefix
      this.dialogVisible = true
      that = this
    },
    completemultiData(res) {
      const boo = res && res.data
      return boo?.[0].upload_id
    },
    async doUploadHttp(param) {
      const { name: id, } = that.$route.query
      const {
        file: { name },
      } = param
      // 大小限制,超过8M用分片
      const chunkSize = getChunkSize(param.file.size) // 单个分片size
      const fileChunkedList = [] // 文件分片完成之后的数组
      if (!param.file.size) {
        // 空文件
        fileChunkedList.push(param.file.slice(0, param.file.size))
      } else {
        // 文件开始分片，push到fileChunkedList数组中
        for (let i = 0; i < param.file.size; i = i + chunkSize) {
          const tmp = param.file.slice(
            i,
            Math.min(i + chunkSize, param.file.size),
          )
          fileChunkedList.push(tmp)
        }
      }
      const putSignUrl =
        (fileChunkedList.length === 1 ? getUploadSignedUrl : getMultipartUploadSignedUrl)
      const completeUrl =
        multipartUploadComplete
      const newParam = {
        vol: id,
        file_name: name,
        content_type: getContentType(name),
        part_num: fileChunkedList.length || 1,
        putSignUrl: putSignUrl({cluster_name: this.clusterName}),
        completeUrl: completeUrl({cluster_name: this.clusterName}),
        chunkSize,
        prefix: that.prefix,
        user: this.$route.query.owner,
      }
      const options = {
        getDataFun: (res) => that.getDataFun(res, fileChunkedList.length),
        completemultiData: (res) => that.completemultiData(res),
        file: param.file,
        xhrOptions: {
          onProgress: (progress) => that.getProgress(progress, param.file.size),
          onAbort: (xhr, reject) => {
            that.abortFun.push({ xhr, reject })
          },
        },
      }
      if (!that.fileList.find((i) => i.name === name)) {
        that.fileList.push({
          name: name,
          url: '',
          uid: '',
          status: 'error',
          progress: 0,
          isFileUploadLoading: false,
          size: param.file.size,
        })
      } else {
        that.$message.warning('不能重复上传')
        return
      }
      putFile(newParam, options)
        .then((res) => {
          const errArr = res.filter((i) => i.code !== 200)
          const fIndex = that.findIndexFun(res[0].fileName)
          const status = errArr.length > 0 ? 'success' : 'error'
          if (fIndex !== -1) {
            that.getFileUrl(that.fileList[fIndex].name).then((res) => {
              // eslint-disable-next-line camelcase
              that.fileList[fIndex].url = res?.data?.signed_url
              that.fileList[fIndex].progress = 100
              that.fileList[fIndex].status = status
              that.fileList[fIndex].isFileUploadLoading = false
              that.fileList[fIndex].uid = res.reqId
            })
          }
        })
        .catch((err) => {
          const fIndex = that.findIndexFun(name)
          if (that.fileList[fIndex]) {
            that.fileList[fIndex].status = 'error'
            that.fileList[fIndex].isFileUploadLoading = false
          }
          that.$message.warning(err)
        })
    },
    getProgress({ loaded, chunkSize, fileName, partIndex }, size) {
      const fIndex = this.findIndexFun(fileName)
      if (!size) {
        this.fileList[fIndex].progress = 100
        this.fileList[fIndex].isFileUploadLoading = false
        return
      }
      this.loadedTotal = loaded + chunkSize * partIndex
      if (fIndex !== -1) {
        const prs = parseInt((this.loadedTotal / size).toFixed(2) * 100)
        if (prs >= 100) this.loadedTotal = 0
        this.fileList[fIndex].progress = prs >= 100 ? 100 : prs
        this.fileList[fIndex].isFileUploadLoading =
          this.fileList[fIndex].progress < 100
      }
    },
    getDataFun({ res, params }, part) {
      const boo = res && res.data && res.data
      if (boo) {
        if (part === 1) {
          return [boo.signed_url]
        } else {
          return boo.map((i) => {
            return i.signed_url
          })
        }
      }
    },
    async getFileUrl(n) {
      const { name } = this.$route.query
      const res = await getDownloadSignedUrl({
        cluster_name: this.clusterName,
        vol: name,
        prefix: this.prefix,
        file_name: n,
        user: this.$route.query.owner,
      })
      return res
    },
    findIndexFun(fileName) {
      return this.fileList.findIndex((i) => i.name === fileName)
    },
    abortUpload() {
      if (this.abortFun.length) {
        this.abortFun.forEach(({ xhr, reject }) => {
          if (xhr) {
            reject && reject('手动取消')
            xhr.abort()
          }
        })
      }
    },
    imgOrIcon(name) {
      // 查看是否显示图片或图表
      const index = name.lastIndexOf('.')
      const fileType = name.substring(index + 1, name.length)
      const arr = ['jpeg', 'jpg', 'png']
      return arr.indexOf(fileType) !== -1
    },
  },
}
</script>

<style lang="scss" scoped>
.file_list_wrap {
  .file_list {
    background-color: #fff;
    border: 1px solid #c0ccda;
    border-radius: 6px;
    box-sizing: border-box;
    margin-top: 10px;
    padding: 10px 10px 10px 90px;
    height: 92px;
    .file_wrap {
      float: left;
      position: relative;
      z-index: 1;
      margin-left: -80px;
      width: 70px;
      height: 70px;
      .file_img {
        vertical-align: middle;
        width: 70px;
        height: 70px;

        background-color: #fff;
      }
      i {
        font-size: 70px;
      }
    }
  }
}
.loading_wrap {
  height: 70px;
  width: 70px;
  margin-left: -80px;
  float: left;
  display: flex;
  justify-content: center;
  align-items: center;
}
.uploadS {
  width: 100%;
  margin: 0 auto;
}
.nameClass {
  margin: 15px 0 10px;
}
.fontS30 {
  font-size: 30px;
}
.el-icon-loading-stop:before {
  content: '\e6cf';
}
.colorR {
  color: red;
}
</style>
