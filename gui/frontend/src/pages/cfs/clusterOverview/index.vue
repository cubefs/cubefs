<template>
  <div>
    <el-card>
      <el-tabs v-model="activeName">
        <el-tab-pane label="集群列表" name="first">
          <el-button
            v-auth="'CLUSTER_CREATE'"
            class="fl-rt mg-bt-s"
            type="primary"
            @click="showDialog('add')"
          >上架集群</el-button>
          <o-page-table :columns="tableColumns" :form-data="formData" :data="tableData"></o-page-table>
        </el-tab-pane>
      </el-tabs>
      <el-dialog
        :title="`${dataId ? '修改' : '上架'}集群`"
        :visible.sync="dialogFormVisible"
        width="800px"

        @closed="clearData"
      >
        <o-form
          ref="form"
          class="mid-block"
          :form-value.sync="formValue"
          :form-list="formList"
          :label-width="200"
          content-width="70%"
          :options="{
            'validate-on-rule-change': false
          }"
          not-ctrl
        ></o-form>
        <div slot="footer" class="dialog-footer">
          <el-button ref="pol" type="primary" @click="doCheck">确 定</el-button>
          <el-button ref="pol" type="primary" @click="close">取 消</el-button>
        </div>
      </el-dialog>
    </el-card>
  </div>
</template>
<script>
import { getClusterList, upDateCluster, createCluster } from '@/api/cfs/cluster'
import { getClusterList as getEbsClusterList } from '@/api/ebs/ebs'
import { initCfsClusterRoute } from '@/router/index'
import { mapMutations } from 'vuex'
export default {
  name: '',
  components: {},
  data() {
    return {
      activeName: 'first',
      dataId: '',
      formData: {},
      formValue: {
        consul_addr: '',
      },
      tableData: [],
      dialogFormVisible: false,
    }
  },
  computed: {
    tableColumns() {
      return [
        {
          title: '集群名',
          key: 'name',
          render: (h, { row }) => {
            return (<a
              class="primary-color"
              onClick={() => {
                this.toDetail(row)
              }}
            >{row.name}</a>)
          },
        },
        {
          title: 'Meta总容量',
          key: 'meta_total',
        },
        {
          title: 'Meta使用量',
          key: 'meta_used',
        },
        {
          title: 'Meta使用率',
          key: 'meta_used_ratio',
        },
        {
          title: 'Data总容量',
          key: 'data_total',
        },
        {
          title: 'Data使用量',
          key: 'data_used',
        },
        {
          title: 'Data使用率',
          key: 'data_used_ratio',
        },
        {
          title: 'master域名',
          key: 'domain',
        },
        {
          title: 's3域名',
          key: 's3_endpoint',
        },
        {
          title: 'tag',
          key: 'tag',
        },
        {
          title: '操作',
          render: (h, { row }) => {
            return (
              <div>
                <el-button
                  v-auth="CLUSTER_UPDATE"
                  type="text"
                  size="medium"
                  class="ft-16"
                  icon="el-icon-edit-outline"
                  title="编辑"
                  onClick={() => {
                    this.showDialog(row)
                  }}>
                </el-button>
              </div>
            )
          },
        },
      ]
    },
    formList() {
      const consulAddr = {
        title: '纠删码(blobstore)地址',
        key: 'consul_addr',
        type: 'input',
        rule: {
          required: true,
          trigger: 'blur',
          validator: (rule, value, cb) => {
            const regIp =
                  /^https?:\/\/(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\:([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-5]{2}[0-3][0-5])$/
            if (!value) {
              cb(new Error('请输入blobstore地址'))
            } else {
              const ipArr = value.split('\n').filter((item) => {
                return !!item
              })
              for (const ip of ipArr) {
                if (!regIp.test(ip)) {
                  cb(new Error('输入的地址含有不合法的ip'))
                  break
                }
              }
              cb()
            }
            cb()
          },
        },
        props: {
          type: 'textarea',
          placeholder: '请输入blobstore地址,一定要加协议，例如(http|https)://ip:port',
          autosize: {
            minRows: 2,
            maxRows: 3,
          },
        },
      }
      return {
        children: [
          {
            title: '集群名:',
            key: 'name',
            type: 'input',
            rule: {
              required: true,
              message: '请输入集群名',
              trigger: 'blur',
            },
            props: {
              placeholder: !this.dataId ? '请输入集群名' : '',
            },
          },
          {
            title: 'Master地址:',
            key: 'master_addr',
            type: 'input',
            rule: {
              required: true,
              // message: '请输入Master地址',
              trigger: 'blur',
              validator: (rule, value, cb) => {
                const regIp =
                  /^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\:([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-5]{2}[0-3][0-5])$/
                if (!value) {
                  cb(new Error('请输入Master地址'))
                } else {
                  const ipArr = value.split('\n').filter((item) => {
                    return !!item
                  })
                  for (const ip of ipArr) {
                    if (!regIp.test(ip)) {
                      cb(new Error('输入的地址含有不合法的ip'))
                      break
                    }
                  }
                  cb()
                }
                cb()
              },
            },
            props: {
              type: 'textarea',
              placeholder: '请输入Master地址(多个以回车分割),只需要填ip:port就行，例如xxx.xxx.xxx.xxx:xxx',
              autosize: {
                minRows: 2,
                maxRows: 3,
              },
            },
          },
          {
            title: '是否关联纠删码集群',
            key: 'vol_type',
            renderContent: (h, item, formData) => {
              return (
                <el-radio-group v-model={formData.vol_type} disabled={Boolean(this.dataId)}>
                  <el-radio label={1}>是</el-radio>
                  <el-radio label={0}>否</el-radio>
                </el-radio-group>
              )
            },
            rule: {
              required: true,
              message: '请选择卷类型',
              trigger: 'change',
            },
            defaultValue: 0,
          },
          this.formValue.vol_type === 1 ? consulAddr : undefined,
          {
            title: '机房:',
            key: 'idc',
            type: 'input',
            rule: {
              required: true,
              message: '输入机房',
              trigger: 'blur',
            },
            props: {
              placeholder: !this.dataId ? '请输入机房' : '',
            },
          },
          {
            title: 'cli:',
            key: 'cli',
            type: 'input',
            rule: {
              required: false,
              message: '请输入cli',
              trigger: 'blur',
            },
            props: {
              placeholder: !this.dataId ? '请输入cli' : '',
            },
          },
          {
            title: 'master域名:',
            key: 'domain',
            type: 'input',
            rule: {
              required: false,
              message: '请输入域名',
              trigger: 'blur',
            },
            props: {
              placeholder: !this.dataId ? '请输入域名' : '',
            },
          },
          {
            title: 's3 endpoint:',
            key: 's3_endpoint',
            type: 'input',
            props: {
              type: 'textarea',
              placeholder: '请输入s3 endpoint，一定要加协议，例如(http|https)://ip:port, (http|https)://域名',
              autosize: {
                minRows: 2,
                maxRows: 3,
              },
            },
            rule: {
              trigger: 'blur',
              validator: (rule, value, cb) => {
                const regIp =
                  /^https?:\/\/(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\:([0-9]|[1-9]\d{1,3}|[1-5]\d{4}|6[0-5]{2}[0-3][0-5])$/
                  const regDomain = /^(?=^.{3,255}$)http(s)?:\/\/?(www\.)?[a-zA-Z0-9][-a-zA-Z0-9]{0,62}(\.[a-zA-Z0-9][-a-zA-Z0-9]{0,62})+(:\d+)*(\/\w+\.\w+)*$/
                if (!value) {
                  cb()
                } else {
                  const ipArr = value.split('\n').filter((item) => {
                    return !!item
                  })
                  for (const ip of ipArr) {
                    if (!regIp.test(ip) && !regDomain.test(ip) ) {
                      cb(new Error('输入的地址不正确'))
                      break
                    }
                  }
                  cb()
                }
                cb()
              },
            },
          },
        ],
      }
    },
  },
  watch: {},
  created() {
    this.getClusterList()
  },
  beforeMount() { },
  mounted() { },
  methods: {
    ...mapMutations('clusterInfoModule', ['setClusterInfo']),
    async getClusterList() {
      const res = await getClusterList()
      this.tableData = res.data.clusters || []
    },
    showDialog(type) {
      this.dialogFormVisible = true
      if (type !== 'add') {
        const {
          name,
          idc,
          master_addr: masterAddr,
          id,
          cli,
          domain,
          vol_type: volType,
          consul_addr: consulAddr,
          s3_endpoint: s3Endpoint,
        } = type
        this.initForm(
          {
            name,
            idc: idc,
            consul_addr: consulAddr,
            master_addr: masterAddr.join('\n'),
            cli,
            domain,
            vol_type: volType,
            s3_endpoint: s3Endpoint,
          },
          id,
        )
      }
    },
    async doCheck() {
      await this.$refs.form.validate()
      const {
        name,
        master_addr: masterAddr,
        idc, cli, domain,
        vol_type: volType,
        s3_endpoint: s3Endpoint,
        consul_addr: consulAddr
      } = this.formValue
      const params = {
        s3_endpoint: s3Endpoint,
        consul_addr: consulAddr,
        name,
        master_addr: masterAddr.split('\n').filter((item) => {
          return !!item
        }),
        idc,
        cli,
        domain,
        vol_type: volType,
      }
      let publishCluster = createCluster
      if (this.dataId) {
        params.id = this.dataId
        publishCluster = upDateCluster
      }
      await publishCluster(params)
      this.$message.success(`${this.dataId ? '修改' : '上架'}成功`)
      this.getClusterList()
      this.close()
    },
    initForm(val, id) {
      this.formValue = { ...val }
      this.dataId = id
    },
    handleClick() {

    },
    clearData() {
      this.dataId = null
      this.$refs.form.reset()
    },
    close() {
      this.clearData()
      this.dialogFormVisible = false
    },
    async getEbsClusterList(region) {
      const res = await getEbsClusterList({
        region,
      })
      return res.data
    },
    async toDetail(item) {
      let ebsClusterInfo = []
      try {
        const data = await this.getEbsClusterList(item.name)
        ebsClusterInfo = data
      } catch (e) {

      } finally {
        this.setClusterInfo({
          clusterName: item.name,
          masterAddr: item.master_addr,
          leaderAddr: item.leader_addr,
          cli: item.cli,
          domainName: item.domain,
          clusterInfo: item,
          ebsClusterInfo,
        })
        initCfsClusterRoute()
        this.$router.push({
          name: 'clusterInfo',
        })
      }
    },
  },
}
</script>
<style lang='scss' scoped>

</style>
