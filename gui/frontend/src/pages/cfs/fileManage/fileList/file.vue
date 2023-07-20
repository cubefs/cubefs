<template>
  <div class="wraper">
    <div>
      <div class="file_control">
        <div>
          <el-button
            v-auth="'CFS_S3_FILES_UPLOAD_SIGNEDURL'"
            type="primary"
            icon="el-icon-top"
            size="medium"
            plain
            @click="handleAdd"
          >上传文件</el-button>
          <el-button
            icon="el-icon-refresh"
            size="medium"
            plain
            @click="handleUpdate"
          >刷新</el-button>
          <el-button
            v-auth="'CFS_S3_DIRS_CREATE'"
            type="warning"
            size="medium"
            @click="handleAddFolder"
          >新建文件夹</el-button>
        </div>
        <div class="flex">
          <div @keyup="searchFile">
            <el-input
              v-model="params.prefix"
              placeholder="请输入搜索前缀，只能搜索当前路径下的文件"
              prefix-icon="el-icon-search"
              size="medium"
              clearable
              class="width400"
            ></el-input>
          </div>
        </div>
      </div>
      <div class="marginB8 flex justify-content-between font14 color3">
        <div>
          <span>当前路径：</span>
          <span class="margin_r4">{{ folderName ? folderName : '' }}</span>
          <el-button
            icon="el-icon-back"
            size="mini"
            round
            plain
            @click="handleBack"
          >返回上一级</el-button>
        </div>
      </div>

      <el-table ref="fileTable" :data="tableData" size="medium">
        <el-table-column prop="key" label="文件名" width="200">
          <template slot-scope="scope">
            <div>
              <span
                v-if="scope.row.isDir"
                class="folder_key"
                @click="handleFolder(scope.row.key)"
              >
                <i class="el-icon-folder-opened margin_r4 folder_i"></i>
                <el-button type="text">{{
                  renderFolderKey(scope.row.key)
                }}</el-button>
              </span>
              <span v-else>
                <i class="co-icon-file margin_r4"></i>
                {{ renderFileKey(scope.row.key) }}
              </span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="mime_type" label="文件类型">
          <template slot-scope="scope">
            <div>
              <span
                v-if="scope.row.key.charAt(scope.row.key.length - 1) === '/'"
              >文件夹</span>
              <span v-else>{{ scope.row.mime_type }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="file_size" label="文件大小">
          <template slot-scope="scope">
            <div>
              <span
                v-if="scope.row.key.charAt(scope.row.key.length - 1) === '/'"
              >-</span>
              <span v-else>{{ renderSize(scope.row.file_size) }}</span>
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="put_time" label="文件上传时间">
          <template slot-scope="scope">{{
            renderTime(scope.row.put_time)
          }}</template>
        </el-table-column>
        <el-table-column label="操作" width="260">
          <template slot-scope="scope">
            <div v-if="scope.row.key.charAt(scope.row.key.length - 1) === '/'">
            </div>
            <div v-else>
              <el-button
                v-auth="'CFS_S3_FILES_DOWNLOAD_SIGNEDURL'"
                size="medium"
                type="text"
                @click="handleDownload(scope.row)"
              >下载</el-button>
            </div>
          </template>
        </el-table-column>
      </el-table>
      <div class="pagination-bm color6">
        <Pagination
          ref="pagination"
          :marker="params.marker"
          :page-size.sync="params.limit"
          @update="pageChange"
        >
          <template #help>
          </template>
          <div class="loading_view" @click="handleLoading">
            <i :class="iconType"></i>
            &nbsp;
            {{ loadTitleFun(iconType) }}
          </div>
        </Pagination>
      </div>
    </div>
    <UploadDialog ref="UploadDialog" @reload="reload" />
    <UploadFolder ref="UploadFolder" @reload="reload" />
  </div>
</template>

<script>
import UploadDialog from './components/UploadDialog'
import UploadFolder from './components/UploadFolder'
import Pagination from './components/Pagination'

import { getDownloadSignedUrl, getFileList } from '@/api/cfs/cluster'
import moment from 'moment'
import { mapGetters } from 'vuex'
import { renderSizeFile, readablizeUnit, download } from '@/utils'
import mixin from '@/pages/cfs/clusterOverview/mixin'

export default {
  components: {
    UploadDialog,
    UploadFolder,
    Pagination,
  },
  mixins: [mixin],
  data() {
    return {
      corsConfig: [], // 跨域相关的配置
      selectedArr: [], // 选中数据
      params: {
        volume_name: '',
        zone: '',
        delimiter: '/',
        prefix: '',
        limit: 100,
        marker: '',
      },
      folderName: '',
      tableData: [],
      iconType: 'el-icon-sort-down',
      btnFlag: false,
      gridData: {},
      isOverseas: false,
      filterTier: 1,
      operateLimit: true,
      isShowOriginTipForUpload: false,
    }
  },
  computed: {
  },
  created() {
    const { name: id, zone } = this.$route.query
    this.params.volume_name = id
    this.params.zone = zone
  },
  methods: {
    pageChange(marker) {
      this.params.marker = marker
      this.getFiles()
    },
    handleBack() {
      if (!this.folderName) return
      const pre = this.folderName.substring(0, this.folderName.length - 1)
      this.folderName =
        pre.substring(0, pre.lastIndexOf('/') + 1) === './'
          ? ''
          : pre.substring(0, pre.lastIndexOf('/') + 1)
      this.params.marker = ''
      this.params.prefix = ''
      this.getFiles()
    },
    reload() {
      this.params.marker = ''
      this.params.prefix = ''
      this.getFiles()
    },
    renderTime(ctime) {
      if (!ctime) return ''
      const t = Math.round(ctime / 10000)
      return moment(t).format('YYYY-MM-DD HH:mm:ss')
    },
    renderFolderKey(key) {
      const hasFolder = this.folderName
      const hasPrefix = this.params.prefix
      const hasBoth = hasFolder && hasPrefix
      if (hasBoth) {
        if (hasPrefix[hasPrefix.length - 1] === '/') {
          return key.substring(
            this.folderName.length + this.params.prefix.length,
          )
        } else {
          const hasPrefixLastIndex = hasPrefix.lastIndexOf('/')
          return key.substring(this.folderName.length + hasPrefixLastIndex + 1)
        }
      } else if (hasPrefix) {
        if (hasPrefix[hasPrefix.length - 1] === '/') {
          return key.substring(this.params.prefix.length)
        } else {
          const hasPrefixLastIndex = hasPrefix.lastIndexOf('/')
          return key.substring(hasPrefixLastIndex + 1)
        }
      } else if (hasFolder) {
        return key.substring(this.folderName.length)
      }
      return key.replace('./', '')
    },
    renderFileKey(key) {
      return key
    },
    async getFiles(boo, isSearch = false) {
      const temp = this.params.prefix.trim()
      const { volume_name, limit } = this.params
      const res = await getFileList({
        zone: this.clusterName,
        cluster_name: this.clusterName,
        vol: volume_name,
        limit: limit,
        prefix: this.folderName + temp,
        delimiter: this.params.delimiter,
        marker: this.params.marker,
        user: this.$route.query.owner,
      })
      const fileList = []

      // 获取所有文件夹
      const folders = res?.data?.common_prefixes || []
      folders.forEach((i) => {
        fileList.push({
          key: i,
          mime_type: '文件夹',
          isDir: true,
          type: 0,
        })
      })
      // 获取所有文件
      const files = (res?.data?.contents || []).map((it) => {
        return {
          ...it,
          key: it.name,
        }
      })
      files.forEach((i) => {
        i.isDir = false
        fileList.push(i)
      })
      // 文件文件夹整合
      if (boo === 'load') {
        // 加载更多拼接的
        fileList.forEach((i) => {
          this.tableData.push(i)
        })
      } else {
        this.tableData = fileList
      }
      // 排序文件
      this.tableData = this.sortFileFun(this.tableData)

      // 记录加载的点
      this.params.marker = res?.data?.marker || ''
      this.iconType = !this.params.marker
        ? 'el-icon-sort-up'
        : 'el-icon-sort-down'
    },
    sortFileFun(arr) {
      // 区分文件夹
      arr.sort((a, b) => {
        if (a.key.length < b.key.length) return -1
        if (a.key.length > b.key.length) return 1
        return 0
      })

      arr.sort((a, b) => {
        if (a.isDir < b.isDir) return 1
        if (a.isDir > b.isDir) return -1
        return 0
      })
      return arr
    },
    async handleFolder(key) {
      this.folderName = key
      this.params.prefix = ''
      this.tableData = [] // 避免渲染再次触发
      await this.searchFile(true)
    },
    renderSize(value) {
      return renderSizeFile(value)
    },
    async searchFile(isSearch = false) {
      this.params.marker = ''
      await this.getFiles('', isSearch)
    },
    handleUpdate() {
      this.params.prefix = ''
      this.params.marker = ''
      this.getFiles()
    },
    async handleAdd() {
      this.$refs.UploadDialog.openUploadView(
        this.$route.query.name,
        this.folderName ? this.folderName : '',
      )
    },
    handleAddFolder() {
      this.$refs.UploadFolder.openUploadView(
        this.$route.query.name,
        this.folderName,
      )
    },
    handleSelectionChange(val) {
      this.selectedArr = val
    },
    handleLoading() {
      if (!this.params.marker) return
      this.iconType = 'el-icon-loading'
      this.$refs.pagination.onClick(3)
    },
    loadTitleFun() {
      if (this.iconType === 'el-icon-sort-down') return '点击加载'
      if (this.iconType === 'el-icon-loading') return '加载中'
      if (this.iconType === 'el-icon-sort-up') return '没有更多数据了'
    },
    readablizeUnit(value) {
      return readablizeUnit(value)
    },
    async handleDownload({ key }) {
      const Aurl = await this.getImgUrl(key)
      if (!Aurl) return
      download(Aurl, key)
    },
    async getImgUrl(key) {
      const { name } = this.$route.query
      const res = await getDownloadSignedUrl({
        vol: name,
        prefix: this.folderName + this.params.prefix,
        file_name: key,
        cluster_name: this.clusterName,
        user: this.$route.query.owner,
      })
      // eslint-disable-next-line camelcase
      return res?.data?.signed_url
    },
    async handleMore(row) {
      const { fsize, key, mime_type: mimeType, putTime, type, hash } = row
      const obj = await {
        fsize: this.renderSize(fsize),
        key: key,
        mimeType,
        putTime: this.renderTime(putTime),
        type: type === 0 ? '标准存储' : '显示低频存储',
        download_url: row.download_url,
        hash,
      }
      this.$refs.MoreCom.init({ row, obj })
    },
    goDoc() {
      window.open()
    },
  },
}
</script>

<style lang="scss" scoped>
.file_aside {
  font-size: 14px;
  display: flex;
  align-items: center;
  .file_txt {
    color: rgba(0, 0, 0, 0.65) !important;
  }
}
.file_control {
  display: flex;
  justify-content: space-between;
  margin-bottom: 12px;
}
.loading_view {
  color: #2fc29b;
  font-size: 14px;
  height: 44px;
  text-align: center;
  line-height: 44px;
  cursor: pointer;
}
.margin_r4 {
  margin-right: 4px;
}
.folder_i {
  color: #ffcc33;
  font-size: 16px;
}
.marginB8 {
  margin-bottom: 8px;
}
.colorR {
  color: red;
}
.docsBtn {
  margin-left: 20px;
  color: #2fc29b;
  cursor: pointer;
}
.width400 {
  width: 400px;
}
.pagination-bm {
  display: flex;
  margin-top: 20px;
  align-items: center;
}
</style>
