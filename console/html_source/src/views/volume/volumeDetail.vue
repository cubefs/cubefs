<template>
  <div class="cluster volume">
    <crumb :crumbInfo="crumbInfo" :crumbItem="crumbItem"></crumb>
    <div>
      <div class="volume-left">
        <el-button type="primary" @click="uploadFileDialog = true"><i class="btn-icon upload-icon"></i>{{ $t('chubaoFS.volume.UploadFile') }}</el-button>
        <el-button type="primary" @click="createFolderDialog = true"><i class="btn-icon folder-icon"></i>{{ $t('chubaoFS.volume.CreateFolder') }}</el-button>
      </div>
      <div class="volume-right">
        <span>{{$t('chubaoFS.volume.FileFolderName')}}</span>
        <el-input v-model="searchVal" placeholder="" style="width: 255px;"></el-input>
        <el-button type="primary" class="ml5" @click="queryFileList">{{ $t('chubaoFS.tools.Search') }}</el-button>
      </div>
    </div>
    <div class="data-block" v-loading="resData.loading">
      <el-table
        :data="resData.resLists"
        class="mt20"
        style="width: 100%">
        <el-table-column type="index" label="#">
        </el-table-column>
        <el-table-column prop="name" :label="$t('chubaoFS.volume.FileFolderName')">
          <template slot-scope="scope">
            <div v-if="!scope.row.isFile" class="volume-name" @click="goFileList(scope.row)">
              <i class="table-icon"><img src="../../assets/images/folder.png" alt=""></i>
              {{scope.row.noPathName}}
            </div>
            <div v-else class="volume-name" style="color:#000; cursor: default;">
              <i class="table-icon"><img src="../../assets/images/file.png" alt=""></i>
              {{scope.row.noPathName}}
            </div>
          </template>
        </el-table-column>
        <el-table-column prop="size" :label="$t('chubaoFS.volume.Size')">
        </el-table-column>
        <el-table-column prop="modifyTime" :label="$t('chubaoFS.volume.LastModify')">
        </el-table-column>
        <el-table-column prop="createTime" :label="$t('chubaoFS.volume.CreateTime')">
        </el-table-column>
        <el-table-column :label="$t('chubaoFS.tools.Actions')" width="350">
          <template slot-scope="scope">
            <el-button v-show="scope.row.isFile" type="text" class="text-btn" @click="downLoad(scope.row)">{{$t('chubaoFS.tools.Download')}}</el-button>
            <el-button v-show="scope.row.isFile" type="text" class="text-btn" @click="openSignUrl(scope.row)">{{$t('chubaoFS.volume.SignURL')}}</el-button>
            <el-button v-show="scope.row.isFile" type="text" class="text-btn" @click="openDetails(scope.row)">{{$t('chubaoFS.tools.Details')}}</el-button>
            <el-button type="text" class="text-btn" @click="deleteInfo(scope.row)">{{$t('chubaoFS.tools.Delete')}}</el-button>
          </template>
        </el-table-column>
      </el-table>
      <div class="clearfix mt20">
        <el-pagination
          class="fr"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
          :page-sizes="resData.page.pageSizes"
          :page-size="resData.page.pageSize"
          layout="sizes, prev, pager, next"
          :total="resData.page.totalRecord">
        </el-pagination>
        <span class="fr page-tips pr10">{{ $t('chubaoFS.commonTxt.eachPageShows') }}</span>
      </div>
    </div>
    <!-- File Details -->
    <el-dialog
      :title="$t('chubaoFS.volume.FileDetails')"
      :visible.sync="fileDetailsDialog"
      width="30%">
      <div class="pl10 pb10">{{$t('chubaoFS.volume.FileName')}}: {{fileDetails.name}}</div>
      <div class="pl10 pb10">{{$t('chubaoFS.volume.VolumeName')}}: {{this.volumeName}}</div>
      <div class="pl10 pb10">{{$t('chubaoFS.volume.Size')}}: {{fileDetails.size}}</div>
      <div class="pl10 pb10">{{$t('chubaoFS.volume.MD5')}}: {{fileDetails.eTag}}</div>
      <div class="pl10 pb10">{{$t('chubaoFS.volume.CreateTime')}}: {{fileDetails.createTime}}</div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="fileDetailsDialog = false">{{$t('chubaoFS.tools.OK')}}</el-button>
        </span>
    </el-dialog>
    <!-- Create a sign URL -->
    <el-dialog
      :title="$t('chubaoFS.volume.CreateSignURL')"
      :visible.sync="signUrlDialog"
      width="35%"
      @close="closeSignUrl">
<!--      <div class="sign-url">Rules:</div>-->
<!--      <div class="sign-url">1. The time unit is second (s)</div>-->
<!--      <div class="sign-url">2. The minimum value is 1 and the maximum value is 2147483647</div>-->
      <el-form :label-position="labelPosition" :model="signUrlForm" class="mt20">
        <el-form-item :label="$t('chubaoFS.volume.ExpirationTime')">
          <el-input v-model="signUrlForm.time"></el-input>
        </el-form-item>
        <el-form-item :label="$t('chubaoFS.volume.SignURL')">
          <el-input type="textarea" :rows="5" v-model="signUrlForm.address"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="closeSignUrl">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="getSignUrl">{{$t('chubaoFS.tools.Create')}}</el-button>
        </span>
    </el-dialog>
    <!-- Create Folder -->
    <el-dialog
      :title="$t('chubaoFS.volume.CreateFolder')"
      :visible.sync="createFolderDialog"
      width="35%"
      @close="closeDialog">
      <el-form :label-position="labelPosition" ref="createFolderForm" :model="createFolderForm" :rules="rules">
        <el-form-item :label="$t('chubaoFS.volume.FolderName')" prop="folderName">
          <el-input v-model="createFolderForm.folderName"></el-input>
        </el-form-item>
      </el-form>
      <span slot="footer" class="dialog-footer">
        <el-button @click="closeDialog">{{$t('chubaoFS.tools.Cancel')}}</el-button>
        <el-button type="primary" @click="createFolder('createFolderForm')">{{$t('chubaoFS.tools.Create')}}</el-button>
        </span>
    </el-dialog>
    <!-- Upload a file -->
    <el-dialog
      :title="$t('chubaoFS.volume.UploadFile')"
      :visible.sync="uploadFileDialog"
      width="570px">
      <el-form :label-position="labelPosition" :model="uploadFileForm">
        <el-form-item :label="$t('chubaoFS.volume.VolumeName')">
          <el-input v-model="volumeName" disabled></el-input>
        </el-form-item>
      </el-form>
      <el-upload
        class="upload-demo"
        ref="upload"
        name="file"
        drag
        :action="fileUploadUrl"
        :file-list="fileUploadList"
        :on-exceed="uploadExceed"
        :on-change="handleChange"
        :on-remove="handleRemove"
        :on-error="handleError"
        :on-success="handleSuccess"
        :limit="1"
        style="width: 570px;"
        :auto-upload="false"
        multiple>
        <i class="el-icon-upload"></i>
        <div class="el-upload__text">{{$t('chubaoFS.volume.DragTip')}}<em>{{$t('chubaoFS.tools.Upload')}} </em></div>
      </el-upload>
      <div class="pb10 mt30">{{$t('chubaoFS.volume.DragTipPlease')}}</div>
      <div class="pb10">{{$t('chubaoFS.volume.Tips1')}}</div>
      <div class="pb10">{{$t('chubaoFS.volume.Tips2')}}</div>
      <div class="pb10">{{$t('chubaoFS.volume.Tips3')}}</div>
      <div class="pb10">{{$t('chubaoFS.volume.Tips4')}}</div>
      <span slot="footer" class="dialog-footer">
        <el-button @click="closeFile">{{ $t('chubaoFS.tools.No') }}</el-button>
        <el-button type="primary" @click="uploadFile">{{ $t('chubaoFS.tools.Yes') }}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import baseGql from '../../graphql/volume'
import { resolvingDate } from '../../utils/dateTime.js'
export default {
  name: 'volumeDetail',
  data () {
    return {
      labelPosition: 'top',
      searchVal: '',
      resData: {
        loading: true,
        page: {
          pageSizes: [10, 20, 30, 40],
          pageNo: 1,
          pageSize: 10,
          totalRecord: 0,
          totalPage: 1
        },
        resLists: []
      },
      volumeName: '',
      parentPath: '',
      fileDetailsDialog: false,
      signUrlDialog: false,
      createFolderDialog: false,
      uploadFileDialog: false,
      signUrlForm: {
        time: '',
        address: ''
      },
      createFolderForm: {
        folderName: ''
      },
      uploadFileForm: {
        name: ''
      },
      fileList: [],
      folderList: [],
      volumeDetailList: [],
      rules: {
        folderName: [
          { required: true, message: this.$t('chubaoFS.volume.FolderNameMsg'), trigger: 'blur' }
        ]
      },
      fileUploadList: [],
      fileUploadUrl: '',
      fileName: '',
      fileDetails: {},
      crumbItem: [],
      crumbInfo: '',
      screenWidth: document.body.clientWidth
    }
  },
  methods: {
    handleSizeChange (val) {
      this.resData.page.pageSize = val
      this.resData.page.pageNo = 1
      this.handleCurrentChange(1)
    },
    handleCurrentChange (val) {
      this.resData.page.pageNo = val
      const start = (val - 1) * this.resData.page.pageSize
      const end = val * this.resData.page.pageSize
      this.resData.resLists = this.volumeDetailList.slice(start, end)
    },
    queryFileList () {
      this.resData.loading = true
      const variables = {
        request: { prefix: this.parentPath, marker: '', delimiter: '/', maxKeys: 10000 },
        volName: this.volumeName
      }
      this.apollo.query(this.url.file, baseGql.queryFileList, variables).then((res) => {
        this.resData.loading = false
        if (res) {
          let dataList = []
          res.data.listFile.infos.forEach(file => {
            if (this.parentPath === file.path) {
              return
            }
            let obj = {}
            let arr = file.path.split('/')
            obj.noPathName = arr[arr.length - 1]
            obj.name = file.path
            obj.size = file.size
            obj.modifyTime = resolvingDate(file.modifyTime)
            obj.createTime = resolvingDate(file.createTime)
            obj.eTag = file.eTag
            obj.isFile = true
            dataList.push(obj)
          })
          res.data.listFile.prefixes.forEach(item => {
            let obj = {}
            let arr = item.split('/')
            obj.noPathName = arr[arr.length - 2]
            obj.name = item
            obj.namePath = item
            obj.size = '--'
            obj.modifyTime = '--'
            obj.createTime = '--'
            obj.isFile = false
            dataList.push(obj)
          })
          let finalDetailInfo = []
          if (this.searchVal && this.searchVal.trim() !== '') {
            dataList.forEach(user => {
              if (user.name.indexOf(this.searchVal.trim()) > -1) {
                finalDetailInfo.push(user)
              }
            })
          } else {
            finalDetailInfo = dataList
          }
          // 按创建时间倒序排列
          finalDetailInfo.sort(function (a, b) {
            if (a.modifyTime === '--') {
              return 1
            }
            let dateA = Date.parse(a.modifyTime)
            let dateB = Date.parse(b.modifyTime)
            return dateB > dateA ? 1 : -1
          })
          this.volumeDetailList = finalDetailInfo
          this.resData.page.totalRecord = this.volumeDetailList.length
          this.handleCurrentChange(1)
        }
      }).catch((error) => {
        this.resData.loading = false
        console.log(error)
      })
    },
    createFolder (formName) {
      this.$refs[formName].validate((valid) => {
        if (valid) {
          const variables = {
            path: this.parentPath + this.createFolderForm.folderName,
            volName: this.volumeName
          }
          this.apollo.mutation(this.url.file, baseGql.createFolder, variables).then((res) => {
            if (res.data) {
              this.createFolderDialog = false
              this.queryFileList()
              this.$message({
                message: 'success',
                type: 'success'
              })
            } else {
              this.$message.error(res.message)
            }
          })
        } else {
          return false
        }
      })
    },
    uploadFile () {
      if (this.fileUploadList.length === 0) {
        this.$message({
          message: 'Please select a file',
          type: 'warning'
        })
        return
      }
      this.$refs.upload.submit()
    },
    uploadExceed (files, fileList) {
      this.$set(fileList[0], 'raw', files[0])
      this.$set(fileList[0], 'name', files[0].name)
      this.$refs['upload'].clearFiles()// 清除文件
      this.$refs['upload'].handleStart(files[0])// 选择文件后的赋值方法
    },
    handleChange (file, fileList) {
      this.fileUploadList = fileList
      if (fileList.length > 0) {
        this.fileUploadList = [fileList[fileList.length - 1]]
      }
    },
    handleRemove (file, fileList) {
      for (let i = 0, len = fileList.length; i < len; i++) {
        if (fileList[i].name === file.name) {
          fileList.splice(i, 1)
          break
        }
      }
      this.fileUploadList = fileList
    },
    handleError () {
      this.$message({
        message: this.$t('chubaoFS.message.Error'),
        type: 'error'
      })
    },
    handleSuccess () {
      this.$message({
        message: this.$t('chubaoFS.message.Success'),
        type: 'success'
      })
      this.uploadFileDialog = false
      this.queryFileList()
      this.$refs['upload'].clearFiles()
    },
    downLoad (row) {
      let url = this.url.downFile + '?path=' + row.name + '&vol_name=' + this.volumeName + '&_authorization=' + sessionStorage.getItem('access_token')
      let link = document.createElement('a')
      link.style.display = 'none'
      link.href = url
      link.setAttribute('download', row.noPathName)
      document.body.appendChild(link)
      link.click()
    },
    getSignUrl () {
      const variables = {
        volName: this.volumeName,
        path: this.fileName,
        expireMinutes: parseInt(this.signUrlForm.time)
      }
      this.apollo.mutation(this.url.file, baseGql.signURL, variables).then((res) => {
        if (res.data) {
          // this.signUrlForm.address = 'http://' + window.location.host + '/' + res.data.signURL.message
          this.signUrlForm.address = res.data.signURL.message
        } else {
          this.$message.error(res.message)
        }
      }).catch((error) => {
        this.resData.loading = false
        console.log(error)
      })
    },
    openSignUrl (row) {
      this.signUrlDialog = true
      this.fileName = row.name
    },
    openDetails (row) {
      this.fileDetailsDialog = true
      this.fileDetails = Object.assign({}, row)
    },
    closeFile () {
      this.$refs['upload'].clearFiles()
      this.uploadFileDialog = false
    },
    closeSignUrl () {
      this.signUrlForm = {
        time: '',
        address: ''
      }
      this.signUrlDialog = false
    },
    closeDialog () {
      this.createFolderDialog = false
      this.createFolderForm = {}
      this.$refs['createFolderForm'].resetFields()
    },
    deleteInfo (row) {
      const variables = {
        path: row.name,
        volName: this.volumeName
      }
      this.$confirm(this.$t('chubaoFS.volume.deleteInfoMsg'), this.$t('chubaoFS.message.Warning'), {
        confirmButtonText: this.$t('chubaoFS.tools.Yes'),
        cancelButtonText: this.$t('chubaoFS.tools.No'),
        type: 'warning'
      }).then(() => {
        if (row.isFile) {
          this.apollo.mutation(this.url.file, baseGql.deleteFile, variables).then((res) => {
            if (res) {
              this.queryFileList()
              this.$message({
                message: this.$t('chubaoFS.message.Success'),
                type: 'success'
              })
            } else {
              this.$message.error(this.$t('chubaoFS.message.Error'))
            }
          })
        } else {
          this.apollo.mutation(this.url.file, baseGql.deleteDir, variables).then((res) => {
            if (res) {
              this.queryFileList()
              this.$message({
                message: this.$t('chubaoFS.message.Success'),
                type: 'success'
              })
            } else {
              this.$message.error(this.$t('chubaoFS.message.Error'))
            }
          })
        }
      }).catch(() => {
      })
    },
    // 页面所有路由跳转
    goFileList (row) {
      this.$router.push({
        name: 'volumeDetail',
        query: {
          path: row.namePath,
          volumeName: this.volumeName
        }
      })
    },
    init () {
      this.fileUploadUrl = this.url.uploadFile + '?vol_name=' + this.volumeName + '&path=' + this.parentPath + '&_authorization=' + sessionStorage.getItem('access_token')
      this.queryFileList()
    },
    setCrumbItem () {
      this.crumbItem = []
      if (this.parentPath && this.parentPath.trim() !== '') {
        // volumeName
        let obj = {}
        obj.title = this.volumeName
        obj.path = '/volumeDetail'
        obj.query = {
          volumeName: this.volumeName
        }
        this.crumbItem.push(obj)
        let pathArr = this.parentPath.substr(0, this.parentPath.length - 1).split('/')
        for (let i = 0; i < pathArr.length; i++) {
          if (pathArr[i].trim() === '') {
            continue
          }
          if (i === pathArr.length - 1) {
            this.crumbInfo = pathArr[i]
            continue
          }
          let parentPath = ''
          if (i > 0) {
            for (let j = 0; j < i; j++) {
              if (j === 0) {
                parentPath = pathArr[j]
              } else {
                parentPath = parentPath + pathArr[j]
              }
              parentPath += '/'
            }
          }
          let obj = {}
          obj.title = pathArr[i]
          obj.parentPath = parentPath
          obj.path = '/volumeDetail'
          obj.query = {
            path: obj.parentPath + obj.title + '/',
            volumeName: this.volumeName
          }
          this.crumbItem.push(obj)
        }
      } else {
        this.crumbInfo = this.volumeName
      }
    }
  },
  watch: {
    '$route' (to, from) {
      let query = this.$route.query
      this.volumeName = query.volumeName
      if (query.path) {
        this.parentPath = query.path
      } else {
        this.parentPath = ''
      }
      this.setCrumbItem()
      this.init()
    },
    screenWidth (val) {
      if (!this.timer) {
        this.screenWidth = val
        this.timer = true
        let that = this
        setTimeout(function () {
          that.setCrumbItem()
          that.timer = false
        }, 400)
      }
    }
  },
  mounted () {
    let query = this.$route.query
    this.volumeName = query.volumeName
    if (query.path) {
      this.parentPath = query.path
    }
    this.setCrumbItem()
    this.init()
    const that = this
    window.onresize = () => {
      return (() => {
        window.screenWidth = document.body.clientWidth
        that.screenWidth = window.screenWidth
      })()
    }
  }
}
</script>

<style scoped>
  .volume-left {
    display: inline-block;
  }
  .volume-right {
    display: inline-block;
    float: right;
  }
  .volume-right span {
    margin-right: 15px;
  }
  .volume-name{
    cursor: pointer;
    color: #466BE4;
  }
  .sign-url {
    padding-bottom: 5px;
    color: #07268D;
  }
  .table-icon {
    display: inline-block;
    padding-right: 3px;
    width: 16px;
    height: 16px;
  }
  .table-icon img {
    width: 100%;
    height: auto;
  }
  .btn-icon {
    display: inline-block;
    width: 18px;
    height: 18px;
    float: left;
  }
  .upload-icon {
    background: url("../../assets/images/upload-icon.png") no-repeat;
    background-size: 100% auto;
  }
  .folder-icon {
    background: url("../../assets/images/folder-icon.png") no-repeat;
    background-size: 100% auto;
  }
</style>
<style>
  .el-upload-list__item {
    width: 530px !important;
  }
  .volume-left .el-button span {
    padding-left: 5px;
    line-height: 18px;
  }
</style>
