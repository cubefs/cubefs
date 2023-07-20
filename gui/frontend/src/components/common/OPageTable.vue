<script>
import { pickBy, looseEqual, type, debounce } from '@/utils'
import deepmerge from 'deepmerge'
import OSearch from './OSearch'
let oPageTableSettingAllList = {}
let oPageTableCacheSetting = {}
const allSettingStoragekey = 'OPageTableSettingAllList'
const oPageTableCacheSettingStoragekey = 'OPageTableCacheSetting'
try {
  const oPageTableSettingAllListString = localStorage.getItem(
    allSettingStoragekey,
  )
  if (oPageTableSettingAllListString) {
    oPageTableSettingAllList = JSON.parse(oPageTableSettingAllListString)
  }
} catch (error) {}

try {
  const oPageTableCacheSettingString = localStorage.getItem(
    oPageTableCacheSettingStoragekey,
  )
  if (oPageTableCacheSettingString) {
    oPageTableCacheSetting = JSON.parse(oPageTableCacheSettingString)
  }
} catch (error) {}

export default {
  name: 'OPageTable',
  props: {
    cachePageSize: {
      type: Boolean,
      default: false,
    },
    columns: {
      type: Array,
      default() {
        return []
      },
    },
    // 默认搜索条件的key
    defaultSearchKey: {
      type: String,
      default() {
        return ''
      },
    },
    // 是否启用聚合搜索
    osearch: {
      type: Boolean,
      default: false,
    },
    // 是否启用新额聚合搜索
    oCloudSearch: {
      type: Boolean,
      default: false,
    },
    // 聚合搜索的额外配置对象
    // 通常情况下聚合搜索的配置列表是自动生成来的
    // 通过此选项也允许主动传递一个额外的配置列表来拓展功能
    // 例如如果展示栏目不包含的内容，但是搜索条件又要包含的项目
    extendSearchList: {
      type: Array,
      default() {
        return []
      },
    },
    // 当设置OSearch的时候筛选框placeholder
    placeholder: {
      type: String,
      default: '请点击选择筛选内容',
    },
    // 是否展示文档按钮,以及文档的跳转地址
    docsUrl: {
      type: String,
      default: '',
    },
    // 是否展示自动刷新按钮
    refreshControl: {
      type: Boolean,
      default: false,
    },
    // 导出按钮控制及自定义导出函数
    exportControl: {
      type: [String, Function],
      default: '',
    },
    // 是否展示配置自定义列表的按钮
    settingsControl: {
      type: Boolean,
      default: false,
    },
    // 自定义列表存储在本地存储的key, 如果没有则自动使用当前页面的path, 当页面有多个table时有意义
    settingStoragekey: {
      type: String,
      default: '',
    },
    // 请求的方式
    method: {
      type: String,
      default() {
        return 'get'
      },
    },
    // 是否复杂表格
    treeTable: {
      type: Boolean,
      default: false,
    },
    // 透传给el-pagination的props
    paginationProps: {
      type: Object,
      default() {
        return {}
      },
    },
    // 透传给el-pagination的props
    tableProps: {
      type: Object,
      default() {
        return {}
      },
    },
    // 获取列表的接口url
    url: {
      type: String,
      default: '',
    },
    // 分页表格默认请求参数，当与o-search结合使用的时候表示一些额外的数据
    formData: {
      type: Object,
      default() {
        return {}
      },
    },
    // o-search的数据
    searchData: {
      type: Object,
      default() {
        return {}
      },
    },
    // 是否开启自动轮询
    loop: {
      type: Boolean,
      default: false,
    },
    // 自动轮询的时间间隔
    loopTime: {
      type: Number,
      default: 5000,
    },
    // 是否展示可以控制自动轮询状态的按钮
    loopControl: {
      type: Boolean,
      default: false,
    },
    currentPage: {
      type: Number,
      default: 1,
    },
    total: {
      type: Number,
      default: 0,
    },
    pageSize: {
      type: Number,
      default: 15,
    },
    // 表格默认数据
    data: {
      type: Array,
      default() {
        return []
      },
    },
    // 决定使用哪个字段作为data
    dataKey: {
      type: String,
      default: 'list',
    },
    // 决定使用哪个字段作为total
    totalKey: {
      type: String,
      default: 'total',
    },
    // 决定使用哪个字段作为page
    pageKey: {
      type: String,
      default: 'currentPage',
    },
    // 决定使用哪个字段作为pageSize
    pageSizeKey: {
      type: String,
      default: 'pageSize',
    },
    // 决定使用哪个字段作为orderKey
    orderByKey: {
      type: String,
      default: 'orderBy',
    },
    // 决定使用哪个字段作为sortKey
    sortKey: {
      type: String,
      default: 'sort',
    },
    // 发送前的钩子，用于调整请求参数
    // eslint-disable-next-line vue/require-default-prop
    beforeSendHook: {
      type: Function,
    },
    // 发送后的钩子，用于拿到请求后的数据
    // eslint-disable-next-line vue/require-default-prop
    afterSendHook: {
      type: Function,
    },
    // 表格默认选中
    defualtSelectTableData: {
      type: Array,
      default() {
        return []
      },
    },
    // 用于table数据设置默认选中那些数据，根据该key对应的value去判断
    checkDatakey: {
      type: String,
      default: 'key',
    },
    // 是否要分页
    hasPage: {
      type: Boolean,
      default() {
        return true
      },
    },
    // 是否记住当前搜索条件
    formDataMark: {
      type: Boolean,
      default: false,
    },
    // 在请求失败时候是否清除table数据
    isCatchClearData: {
      type: Boolean,
      default: true,
    },
    // 分页大小可编辑
    pageSizes: {
      type: Array,
      default() {
        return [10, 15, 30, 50, 100]
      },
    },
    // $ajax第三个参数
    ajaxConfig: {
      type: Object,
      default() {
        return {}
      },
    },
  },
  data() {
    const defaultCheckColumns = this.columns.length
      ? this.columns
      : this.getSettingColumns()
    const defaultCheckList = defaultCheckColumns
      .filter(v => {
        // 当settingsControl关闭的时候不需要启动列过滤功能
        if (!this.settingsControl) {
          return v
        } else {
          return v.key && v.settingExtend !== true
        }
      })
      .map(v => v.key)
    const currentSettingStoragekey = this.settingStoragekey || this.$route.path // 当前页面path加当前opagetable的url
    const list =
      (this.settingsControl &&
        oPageTableSettingAllList[currentSettingStoragekey]) ||
      defaultCheckList.slice() // 当settingsControl关闭的时候不需要启动列过滤功能
    return {
      tooltipContent: '',
      filterPopoverShow: '',
      loading: false,
      loopWaiting: false, // 展示loop倒计时功能
      currentSettingStoragekey,
      defaultCheckList, // 默认开发者设置的哪些可以配置的项目列表
      settingCheckList: list.slice(), // 当前勾选哪些可以配置的项目列表
      confirmSettingCheckList: list.slice(), // 最终生效哪些可以配置的项目列表
      currentFormData: this.formData,
      internalSearchData: this.searchData,
      nowPageTableDataKeys: [], // 当前列表中的所有key
      internalSelectedTableData: [], // 当前页实时勾选项
      tableData: [],
      internalPageSize: (this.$OELEMENT?.cachePageSize || this.cachePageSize) && oPageTableCacheSetting[currentSettingStoragekey]
        ? oPageTableCacheSetting[currentSettingStoragekey].pageSize
        : this.pageSize,
      internalTotal: 0,
      internalCurrentPage: 1,
      treeTableData: [],
      settingsDialogShow: false,
      tableCellNumData: [], // 保存每行的每一列的合并行数
      internalSortProp: {}, // 标记当前的排序方式
      oCloudSearchDefaultKey: this.defaultSearchKey, // oCloudSearch组件默认搜索条件的key
    }
  },
  computed: {
    settingColumns() {
      if (this.columns.length) {
        return this.columns
      } else {
        return this.getSettingColumns()
      }
    },
    // 哪一些列需要固定，不可配置
    settingFixedList() {
      return this.settingColumns.filter(v => v.settingFixed).map(v => v.key)
    },
    // 哪一些列作为拓展勾选列
    settingExtendList() {
      return this.settingColumns.filter(v => v.settingExtend).map(v => v.key)
    },
    allSettingFixedList() {
      const { settingFixedList } = this
      return this.settingColumns
        .filter(v => v.key && v.title)
        .map(v => ({
          ...v,
          disabled: settingFixedList.includes(v.key),
        }))
    },
    settingCheckAll: {
      get() {
        return this.settingCheckList.length === this.allSettingFixedList.length
      },
      set(val) {
        this.settingCheckList = val
          ? this.allSettingFixedList.map(v => v.key)
          : this.settingFixedList.slice()
      },
    },
    settingIsIndeterminate() {
      return this.settingCheckList.length < this.allSettingFixedList.length
    },
    searchList() {
      return this.settingColumns
        .filter(v => !!v.filter)
        .map(({ title, key, filter = {} }) => ({ title, key, ...filter }))
    },
  },
  watch: {
    loop(val) {
      if (val) {
        this.refresh()
      } else {
        if (this.t) {
          clearTimeout(this.t)
          this.loopStatus = 'end'
        }
      }
    },
    formData(newValue, oldValue) {
      this.search(newValue)
    },
    data: {
      handler: function(val, oldVal) {
        if (this.treeTable) {
          this.treeTableData = this.treeTableFormat(
            JSON.parse(JSON.stringify(val || [])),
          )
        } else {
          this.tableData = val
        }
      },
      deep: true,
      immediate: true,
    },
    searchData: {
      deep: true,
      immediate: true,
      handler(newValue, oldValue) {
        if (!looseEqual(newValue, this.internalSearchData)) {
          this.internalSearchData = { ...newValue }
        }
      },
    },
    internalSearchData(newVal, oldValue) {
      if (!looseEqual(newVal, oldValue)) {
        this.$emit('update:searchData', { ...newVal })
        this.search()
      }
    },
    currentPage: {
      immediate: true,
      handler(newValue, oldValue) {
        this.internalCurrentPage = newValue
      },
    },
    internalCurrentPage(newVal, oldValue) {
      this.$emit('update:currentPage', newVal)
      this.getTableData({
        ...this.currentFormData,
        [this.pageKey]: newVal,
      })
    },
    total: {
      immediate: true,
      handler(newValue, oldValue) {
        this.internalTotal = newValue
      },
    },
    internalTotal: {
      immediate: true,
      handler(newVal, oldValue) {
        this.$emit('update:total', newVal)
      },
    },
    internalPageSize(newVal, oldValue) {
      this.$emit('update:pageSize', newVal)
      this.getTableData({
        ...this.currentFormData,
        [this.pageKey]: this.internalCurrentPage,
        [this.pageSizeKey]: newVal,
      })
    },
    pageSize: {
      immediate: true,
      handler(newValue, oldValue) {
        // 第一次设置pageSize且使用缓存
        if (!oldValue && (this.$OELEMENT?.cachePageSize || this.cachePageSize) && oPageTableCacheSetting[this.currentSettingStoragekey]) {
          this.internalPageSize = oPageTableCacheSetting[this.currentSettingStoragekey].pageSize || newValue
        } else {
          this.internalPageSize = newValue
        }
      },
    },
    tableData(newValue, oldValue) {
      const { checkDatakey } = this
      if (!checkDatakey) return
      this.nowPageTableDataKeys = newValue.map(v => v[checkDatakey])
      this.rowCheckByKey()
    },
    defualtSelectTableData(newValue, oldValue) {
      if (!this.checkDatakey) return
      this.rowCheckByKey()
    },
    internalSelectedTableData(newVal, oldVal) {
      if (!looseEqual(newVal, oldVal)) {
        // 跨页勾选基本思想：非本页数据已“固化”，不需要关心，所以每次internalSelectedTableData改变，先过滤defualtSelectTableData已有的本页数据，再将newV与过滤结果合并
        const result = this.defualtSelectTableData
          .filter(v => {
            return !this.nowPageTableDataKeys.includes(v[this.checkDatakey])
          })
          .concat(newVal)
        this.$emit('update:defualtSelectTableData', result)
      }
    },
  },
  created() {
    // 如果已经设置了默认排序，不需要再执行search,因为handlerSortChange一定会执行，
    // 同时注意，由于默认排序的列可能被隐藏，会无法触发handlerSortChange所以这里增加默认排序列不在被隐藏列里
    const defaultSort =
      this.tableProps.defaultSort || this.tableProps['default-sort']
    if (
      defaultSort &&
      this.confirmSettingCheckList.includes(defaultSort.prop)
    ) {
      return
    }
    this.search()
  },
  mounted() {
    if (this.formDataMark) {
      this.setQuery(this.formData)
    }
  },
  beforeDestroy() {
    if (this.t) {
      window.clearTimeout(this.t)
    }
  },
  methods: {
    getSettingColumns() {
      const columns = Object.keys(this.$slots)
        .reduce((arr, key) => arr.concat(this.$slots[key]), [])
        .map(v => {
          const attr = (v.data && v.data.attrs && v.data.attrs) || {}
          const props =
            (v.componentOptions && v.componentOptions.propsData) || {}
          return {
            ...attr,
            ...props,
            title: props.label,
            key: props.prop,
          }
        })
        .filter(v => v.key && v.title)
        .map(attrObj => {
          Object.keys(attrObj).map(key => {
            const newKey = key.replace(/-(\w)/g, (all, letter) => {
              return letter.toUpperCase()
            })

            attrObj[newKey] = attrObj[key] === '' ? true : attrObj[key]
          })
          return attrObj
        })
      return columns
    },
    isEmptyObj(obj) {
      for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
          return false
        }
      }
      return true
    },
    setStorageSettingsList() {
      const key = this.currentSettingStoragekey
      oPageTableSettingAllList[key] = this.settingCheckList
      localStorage.setItem(
        allSettingStoragekey,
        JSON.stringify(oPageTableSettingAllList),
      )
    },
    setQuery(val) {
      this.$router.push({
        query: {
          ...this.$route.query,
          ...val,
        },
      })
    },
    handlerSortChange({ column, prop, order }) {
      this.internalSortProp = {}
      if (order) {
        this.internalSortProp = {
          [this.orderByKey]: prop,
          [this.sortKey]: order === 'descending' ? 'desc' : 'asc',
        }
      }
      this.search()
    },
    handleExport() {
      if (typeof this.exportControl === 'string') {
        window.open(this.exportControl)
      } else {
        this.exportControl()
      }
    },
    renderCopyIcon(h, params) {
      return [
        h('p', {
          class: 'copy-icon ion-ios-copy m-l-5',
          on: {
            click: event => {
              const input = document.createElement('textarea')
              input.value = event.srcElement.previousSibling.innerText.toString()
              document.body.appendChild(input)
              input.select()
              document.execCommand('copy', false)
                ? this.$message.success('复制成功')
                : this.$message.warning('复制失败，再试一次')
              input.remove()
            },
          },
        }),
      ]
    },
    renderHandlerTableColumn(h, params, option) {
      let textEllipsis = option.props.textEllipsis
      if (!textEllipsis || textEllipsis < 1 || textEllipsis > 3) {
        textEllipsis = 1
      }
      return (
        <p class="o-cel-tooltip cell-box" onMouseenter={this.handleTableCellMouseEnter} onMouseleave={this.handleTableCellMouseLeave}>
          <p class={ textEllipsis === 1 ? 'column-text-ellipsis-nowrap' : 'column-text-ellipsis-wrap' } style={{ '-webkit-line-clamp': textEllipsis }}>
            {option.props.render
              ? option.props.render(h, params)
              : params.row[params.column.property]}
          </p>
          {this.tooltipContent ? this.renderCopyIcon(h, params) : ''}
        </p>
      )
    },
    handleTableCellMouseEnter(e) {
      const target = e.target
      if (!target.innerText) {
        this.tooltipContent = ''
        return
      }
      this.tooltipContent = target.innerText
      const tooltip = this.$refs.tooltip
      tooltip.referenceElm = target
      tooltip.$refs.popper && (tooltip.$refs.popper.style.display = 'none')
      tooltip.doDestroy()
      tooltip.setExpectedState(true)
      this.handleShowPopper(tooltip)
    },
    handleTableCellMouseLeave() {
      const tooltip = this.$refs.tooltip
      if (tooltip) {
        tooltip.setExpectedState(false)
        tooltip.handleClosePopper()
      }
    },
    initTooltipHandler() {
      if (!this.handleShowPopper) {
        this.handleShowPopper = debounce(_tooltip => _tooltip.handleShowPopper(), 50)
      }
    },
    async rowCheckByKey() {
      await this.$nextTick()
      const checkDatakeyArr = this.defualtSelectTableData.map(
        v => v[this.checkDatakey],
      )
      const { checkDatakey } = this
      this.tableData.forEach(v => {
        if (checkDatakeyArr.includes(v[checkDatakey])) {
          this.$refs.table.toggleRowSelection(v, true)
        } else {
          this.$refs.table.toggleRowSelection(v, false)
        }
      })
    },
    // 这个方法搜索不会重置到第一页
    refresh(formData = this.currentFormData) {
      this.getTableData({
        ...formData,
        [this.pageKey]: this.internalCurrentPage,
      })
    },
    // 这个方法搜索会重置到第一页
    search(formData = this.formData) {
      this.getTableData({
        ...formData,
        [this.pageKey]: 1,
      })
      this.currentFormData = formData
      // FIXME: 这里会引起请求两次
      this.internalCurrentPage = 1
      if (this.formDataMark) {
        this.setQuery(formData)
      }
    },
    getTableData(data, showTableLoading = true) {
      if (!this.url) {
        return
      }
      if (this.t) {
        clearTimeout(this.t)
      }
      if (
        this.loopPadding &&
        this.loop &&
        !this._isDestroyed &&
        !showTableLoading
      ) {
        return
      }
      // 当前页面被隐藏以后，只做空轮询不发起请求
      if (!this.$isServer && document.hidden) {
        this.t = setTimeout(() => this.getTableData(data, false), this.loopTime)
        return
      }

      this[showTableLoading ? 'loading' : 'loopPadding'] = true
      const query = {
        ...this.internalSearchData,
        ...data,
        ...this.internalSortProp,
        [this.pageSizeKey]: this.internalPageSize,
      }
      if (this.beforeSendHook) {
        this.beforeSendHook(query)
      }
      let result = []
      let res = {}
      this.loopWaiting = false
      const _temp = this.defualtSelectTableData
      this.$ajax[this.method](this.url, query, this.ajaxConfig)
        .then(({ data }) => {
          if (!data) {
            return
          }
          res = data
          result =
            data &&
            (data[this.dataKey] || data.result || data.list || data || [])
          this.$emit('update:data', result)
          this.internalTotal =
            data[this.totalKey] || data.totalCount || data.total
          if (this.treeTable) {
            this.treeTableData = this.treeTableFormat(result || [])
          } else {
            this.tableData = result || []
          }
          this.$nextTick(() => {
            this.$emit('update:defualtSelectTableData', _temp)
          })
        })
        .catch(() => {
          if (this.isCatchClearData) {
            this.tableData = []
          }
        })
        .finally(() => {
          if (this.afterSendHook) {
            this.afterSendHook(result, res)
          }

          this[showTableLoading ? 'loading' : 'loopPadding'] = false
          // 当处于服务端渲染时也不要开启轮询，否则死循环
          if (this.loop && !this._isDestroyed && !this.$isServer) {
            if (this.t) {
              clearTimeout(this.t)
            }
            this.loopWaiting = true
            this.t = setTimeout(
              () => this.getTableData(data, false),
              this.loopTime,
            )
          }
        })
    },
    getCloumnKey() {
      const columnsMap = {}
      this.columns.forEach(item => {
        if (item.type !== 'index' && item.key) {
          columnsMap[item.key] = []
        }
      })
      columnsMap.isShow = true
      return columnsMap
    },
    treeTableFormat(result) {
      let data = []
      if (result.length) {
        data = result.map(item => {
          const { columnsKey, columnsChildrenNum } = this.columnsFormat(
            item,
            this.getCloumnKey(),
          )
          this.tableCellNumData.push(columnsChildrenNum)
          return columnsKey
        })
      }
      return data
    },
    columnsFormat(data, columnsKey) {
      let temp = [data]
      // 获取一个空的列信息，用来存储没列信息的child数量，用作单元格合并
      const columnsChildrenNum = this.getCloumnKey()
      while (temp.length > 0) {
        const currentNode = temp.pop()
        const currentNodeKeyList = Object.keys(currentNode)

        currentNodeKeyList.forEach(key => {
          if (key === 'children' || key === 'keyList' || key === 'isShow') {
            return
          }
          // *：代表当前key没有在列信息内， #：表示当前节点是叶子节点
          if (columnsKey[key]) {
            columnsKey[key].push(currentNode[key])
            // columnsChildrenNum
            columnsChildrenNum[key].push(
              currentNode.children ? currentNode.children.length : '#',
            )
          } else {
            columnsKey[key] = [currentNode[key]]
            columnsChildrenNum[key] = ['*']
          }
        })
        if (currentNode.children) {
          if (!currentNode.children.length) {
            // 将子节点没有的字段补上
            const childrenKey = {}
            const parentKeyStr =
              Object.keys(currentNode)
                .concat(currentNode.keyList)
                .join(',') + ','
            Object.keys(columnsChildrenNum).forEach(key => {
              if (parentKeyStr.indexOf(key + ',') === -1) {
                childrenKey[key] = ''
              }
            })
            currentNode.children.push(childrenKey)
          } else {
            // 给父节点的合并节点数增加，支持无线增加
            if (currentNode.keyList) {
              currentNode.keyList.forEach(key => {
                if (
                  key === 'children' ||
                  key === 'keyList' ||
                  key === 'isShow'
                ) {
                  return
                }
                const item = columnsChildrenNum[key]
                item[item.length - 1] += currentNode.children.length - 1
              })
            }
            // 将父节点的key传递给子节点
            currentNode.children.map(child => {
              child.keyList = currentNode.keyList
                ? currentNode.keyList.concat(currentNodeKeyList)
                : currentNodeKeyList
            })
          }

          temp = temp.concat(currentNode.children)
        } else {
          // 将子节点没有的字段补上
          Object.keys(columnsChildrenNum).forEach(key => {
            Object.keys(currentNode)
              .concat(currentNode.keyList || [])
              .forEach(subKey => {
                if (key !== subKey) {
                  currentNode[key] = ''
                }
              })
          })
        }
      }

      return {
        columnsKey,
        columnsChildrenNum,
      }
    },
    settings() {
      this.settingsDialogShow = true
    },
    renderSettingsDialog(h) {
      return (
        <el-dialog
          class="settings-dialog"
          visible={this.settingsDialogShow}
          {...{
            on: {
              'update:visible': v => {
                this.settingsDialogShow = false
              },
            },
          }}
          title="自定义列表"
          width="550px">
          <div>
            <el-alert
              class="alert"
              closable={false}
              title={`选中：${this.settingCheckList.length}/${this.allSettingFixedList.length}。清除缓存或更换浏览器，自定义列表失效。`}
              type="info"></el-alert>
            <div class="settings-header">
              <el-checkbox
                indeterminate={this.settingIsIndeterminate}
                value={this.settingCheckAll}
                onInput={v => {
                  this.settingCheckAll = v
                }}>
                全选
              </el-checkbox>
              <el-button
                onClick={() => {
                  this.settingCheckList = this.defaultCheckList.slice()
                }}
                class="reset-button"
                type="text">
                <o-icon type="ios-refresh" /> 重置到默认选项
              </el-button>
            </div>
            <el-checkbox-group
              value={this.settingCheckList}
              onInput={v => {
                this.settingCheckList = v
              }}>
              {this.allSettingFixedList.map(v => (
                <el-checkbox disabled={v.disabled} label={v.key} key={v.key}>
                  {v.title}
                </el-checkbox>
              ))}
            </el-checkbox-group>
          </div>
          <span slot="footer" class="dialog-footer">
            <el-button
              onClick={() => {
                this.settingCheckList = this.confirmSettingCheckList.slice()
                this.settingsDialogShow = false
              }}>
              取 消
            </el-button>
            <el-button
              type="primary"
              onClick={() => {
                this.confirmSettingCheckList = this.settingCheckList.slice()
                this.setStorageSettingsList()
                this.settingsDialogShow = false
              }}>
              确 定
            </el-button>
          </span>
        </el-dialog>
      )
    },
    renderLoopBorder(h) {
      return (
        <svg
          class="loop-border"
          viewBox="0 0 100 100"
          xmlns="http://www.w3.org/2000/svg">
          <rect
            class={[
              'rect-path',
              { 'rect-path-animate': this.loop && this.loopWaiting },
            ]}
            fill="none"
            width="100"
            height="100"
            rx="10"
            ry="10"
            style={{ animationDuration: `${this.loopTime}ms` }}
            stroke-linejoin="round"
            stroke-linecap="round"
            stroke="currentColor"
            stroke-width="10px"
          />
        </svg>
      )
    },
    renderOCloudSearch(h) {
      if (!this.oCloudSearch) return

      return (
        <div class="o-cloud-search-container">
          <o-cloud-search
            class="o-cloud-search"
            defaultSearchKey={this.oCloudSearchDefaultKey}
            placeholder={this.placeholder}
            search-value={this.internalSearchData}
            {...{
              scopedSlots: {
                toolTips: scope => {
                  return this.renderTools(h)
                },
              },
              on: {
                search: data => {
                  this.internalSearchData = data
                  this.search()
                },
                'update:searchValue': data => {
                  this.internalSearchData = data
                  this.search()
                },
              },
            }}
            search-list={[
              ...this.searchList,
              ...this.extendSearchList,
            ]}></o-cloud-search>
        </div>
      )
    },
    renderOSearch(h) {
      if (!this.osearch) return
      return (
        <OSearch
          class="o-search"
          placeholder={this.placeholder}
          search-list={[...this.searchList, ...this.extendSearchList]}
          search-value={this.internalSearchData}
          onClear={() => {
            this.filterPopoverShow = ''
          }}
          ref="osearch"
          {...{
            on: {
              'update:searchValue': v => {
                this.internalSearchData = pickBy(v, ([k, v]) => {
                  if (type(v) === 'array' && !v.length) return false
                  return v !== ''
                })
              },
            },
          }}></OSearch>
      )
    },
    renderTools(h) {
      return (
        <div class="tools-list">
          {this.settingsControl
            ? (
            <el-tooltip
              class="item"
              effect="dark"
              content="自定义列表"
              placement="top">
              <el-button
                onClick={this.settings}
                icon="ion-ios-settings"></el-button>
            </el-tooltip>
              )
            : (
                ''
              )}
          {this.refreshControl
            ? (
            <el-tooltip
              class="item"
              effect="dark"
              content="刷新"
              placement="top">
              <el-button
                onClick={() => this.refresh()}
                icon="ion-md-sync"></el-button>
            </el-tooltip>
              )
            : (
                ''
              )}
          {this.loopControl
            ? (
            <el-tooltip
              class="item"
              effect="dark"
              content={`${this.loop ? '停止' : '启动'}自动轮询`}
              placement="top">
              <div
                class="loop-control-btn"
                onClick={() => {
                  this.$emit('update:loop', !this.loop)
                }}>
                <el-button
                  onClick={() => this.refresh()}
                  icon={
                    this.loop ? 'ion-ios-pause' : 'ion-ios-play'
                  }></el-button>
                {this.renderLoopBorder(h)}
              </div>
            </el-tooltip>
              )
            : (
                ''
              )}
          {this.exportControl
            ? (
            <el-tooltip
              class="item"
              effect="dark"
              content="导出列表"
              placement="top">
              <el-button
                class="m-l-10"
                onClick={this.handleExport}
                icon="ion-md-cloud-download"></el-button>
            </el-tooltip>
              )
            : (
                ''
              )}
          {this.docsUrl
            ? (
            <el-tooltip
              class="item"
              effect="dark"
              content="文档"
              placement="top">
              <el-button
                onClick={() => {
                  window.open(this.docsUrl)
                }}
                icon="ion-ios-help-circle-outline"></el-button>
            </el-tooltip>
              )
            : (
                ''
              )}
        </div>
      )
    },
    renderButtonGroup(h) {
      return <div class="button-group-slot">{this.$slots.button}</div>
    },
    renderTable(h) {
      const slots = Object.keys(this.$slots).reduce(
        (arr, key) => arr.concat(this.$slots[key]),
        [],
      )
      const tableProps = deepmerge({
        'header-cell-style': { background: '#f8f9fc', color: '#606266', fontWeight: '550' },
      }, this.tableProps)
      const options = {
        props: {
          data: this.treeTable ? this.treeTableData : this.tableData,
          ...tableProps,
        },
        directives: [
          {
            name: 'loading',
            value: this.loading,
          },
        ],
        // 当每次自定义列表设置后都重建table，否则可能会出现错位的问题
        key: this.settingsControl
          ? this.confirmSettingCheckList.join('-')
          : 'table',
        ref: 'table',
        class: ['table', this.treeTable ? 'split-table' : 'common-table'],
        on: {
          'selection-change': rows => {
            this.internalSelectedTableData = rows
          },
          ...this.$listeners,
        },
      }
      // 如果columns里某项开启了sort，启动后端排序
      if (this.columns.some(v => v.sort)) {
        options.on = { 'sort-change': this.handlerSortChange, ...options.on }
      }
      return h(
        'el-table',
        options,
        this.columns.length
          ? this.renderTableColumn(h)
          : this.renderColumnSlots(slots, h),
      )
    },
    // 这里可以拦截列slots渲染
    renderColumnSlots(slots, h) {
      const { confirmSettingCheckList } = this
      const newSlots = slots
        .filter(item => {
          return !!item.componentOptions
        })
        .filter(item => {
          // componentOptions可以获取到el-table-column上组件内部props、title参数
          // data上可以获取在el-table-column传入的自定义参数(filter/setting-extend等)

          const {
            componentOptions: { propsData, children },
            data: { attrs },
          } = item
          // merge el组件props和自定义props
          const props = {
            ...propsData,
            ...attrs,
          }
          if (children && children.length) {
            children.map(columnChild => {
              // 遍历slots下面的header，取出i标签属性为filter部分，重新生成生成dom
              if (columnChild.data && columnChild.data.slot === 'header') {
                if (columnChild.children && columnChild.children.length) {
                  columnChild.children = columnChild.children.map(
                    headerChild => {
                      const { attrs = {} } = headerChild.data || {}
                      const { filter, desc } = attrs
                      // 如果该列需要过滤，则生成过滤漏斗icon，并绑定事件
                      if (filter === 'icon') {
                        // 如果有描述信息则可以通过该字段生成tip浮层
                        if (desc) {
                          return h(
                            'el-tooltip',
                            {
                              props: {
                                effect: 'dark',
                                content: desc,
                                placement: 'top-start',
                              },
                            },
                            [
                              this.createFileterIcon(
                                h,
                                headerChild,
                                attrs,
                                props,
                              ),
                            ],
                          )
                        } else {
                          return this.createFileterIcon(
                            h,
                            headerChild,
                            attrs,
                            props,
                          )
                        }
                      } else {
                        return headerChild
                      }
                    },
                  )
                }
              }
            })
          }
          // 如果改列是扩展字段则需要去local获取上一次设置的列数据然后展示
          return (
            !props.prop ||
            props.prop === 'operate' ||
            confirmSettingCheckList.includes(props.prop)
          )
        })
      return newSlots
    },
    createFileterIcon(h, headerChild, attrs, props) {
      return h('i', {
        ...headerChild.data,
        attrs: attrs,
        class: 'ion-ios-funnel filter-icon',
        on: {
          click: e => {
            e.stopPropagation()
            this.osearch &&
              this.$refs.osearch.keyFilter(props.filter.key || props.prop)
          },
        },
      })
    },
    renderFilterIcon(h, item) {
      if (!this.osearch && !this.oCloudSearch) {
        return
      }
      const key = item.filter.key || item.key
      const type = item.filter.type
      const isCurrentFilter = this.filterPopoverShow === key
      if (type === 'input') {
        return (
          <i
            onClick={e => {
              e.stopPropagation()
              if (this.osearch) {
                this.$refs.osearch.keyFilter(key, 'inner')
              } else if (this.oCloudSearch) {
                this.oCloudSearchDefaultKey = key
              }
            }}
            class="ion-ios-funnel filter-icon"></i>
        )
      }
      if (type === 'date') {
        return (
          <el-popover
            trigger="hover"
            popper-class="o-page-table-filter-popover">
            {this.renderDatePicker(h, item, key)}
            <i class="ion-ios-funnel filter-icon" slot="reference"></i>
          </el-popover>
        )
      }
      // 如果搜索条件是下拉的时候需要找到对应的下拉选项
      let options = []
      this.searchList.map(item => {
        if (item.key === key) {
          options = item.options
        }
      })
      const oSearchPopover = (
        <el-popover
          trigger="manual"
          popper-class="o-page-table-filter-popover"
          onInput={v => {
            this.filterPopoverShow = v ? key : ''
          }}
          value={isCurrentFilter}>
          {this.osearch && this.$refs.osearch.renderCurrentEditorPannal(h)}
          <i
            onClick={e => {
              e.stopPropagation()
              this.filterPopoverShow = key
              this.$refs.osearch.keyFilter(key)
            }}
            class="ion-ios-funnel filter-icon"
            slot="reference"></i>
        </el-popover>
      )
      const oCloudSearchPopver = (
        <el-popover trigger="hover" popper-class="o-page-table-filter-popover">
          {this.oCloudSearch && this.renderPopoverOptions(h, options, key)}
          <i class="ion-ios-funnel filter-icon" slot="reference"></i>
        </el-popover>
      )
      return this.osearch ? oSearchPopover : oCloudSearchPopver
    },
    // oCloudSearch支持日期控件快捷搜索
    renderDatePicker(h, item, key) {
      const props = {
        'value-format': 'yyyy/MM/dd',
        type: 'daterange',
        align: 'right',
        'unlink-panels': true,
        'start-placeholder': '开始日期',
        'end-placeholder': '结束日期',
        ...(item.filter.props || {}),
      }

      return (
        <div class="current-editor-datepicker">
          <el-date-picker
            value={this.internalSearchData[key]}
            onInput={v => {
              this.oCloudSearchDefaultKey = key
              // 支持用户自定义查询时间日期格式
              const timeData =
                item.filter.timeFormat && item.filter.timeFormat(v)
              this.internalSearchData = {
                ...this.internalSearchData,
                [key]: v.slice().join('-'),
                [item.filter.startTimeKey]:
                  (timeData && timeData[item.filter.startTimeKey]) || v[0],
                [item.filter.endTimeKey]:
                  (timeData && timeData[item.filter.endTimeKey]) || v[1],
              }
            }}
            {...{ props }}></el-date-picker>
        </div>
      )
    },
    // oCloudSearch支持下拉控件快捷搜索
    renderPopoverOptions(h, options, key) {
      return (
        <ul class="label-list">
          {options.map(item => {
            return (
              <li
                ke={item.value}
                onClick={e => {
                  this.oCloudSearchDefaultKey = key

                  this.internalSearchData = {
                    ...this.internalSearchData,
                    [key]: item.value,
                  }
                }}>
                <span>{item.text}</span>
              </li>
            )
          })}
        </ul>
      )
    },
    renderTableColumn(h) {
      const { confirmSettingCheckList } = this
      return this.columns
        .filter(v => v.condition !== false)
        .filter(v => !v.key || confirmSettingCheckList.includes(v.key))
        .map((v, i) => {
          const option = {
            props: {
              prop: v.key,
              label: v.title,
              ...v,
            },
          }
          // 如果开启这个参数，启动后端排序
          if (v.sort) {
            option.props.sortable = 'custom'
          }
          option.scopedSlots = v.scopedSlots || {}
          // 如果有show-overflow-tooltip属性，则需要点击复制;屏蔽el-table的自带tootip，采用自定义tooltip
          if (option.props['show-overflow-tooltip']) {
            this.$delete(option.props, 'show-overflow-tooltip')
            if (!this.handleShowPopper) this.initTooltipHandler()
            option.scopedSlots.default = props =>
              this.renderHandlerTableColumn(h, props, option)
          } else if (v.render || this.treeTable) {
            option.scopedSlots.default = props =>
              this.treeTable
                ? this.renderTreeTableColumn(h, props, v)
                : v.render(h, props)
          }

          // 自定义header优先
          if (!v.scopedSlots?.header && v.filter) {
            option.scopedSlots.header = () => (
              <span>
                {v.title}
                {this.renderFilterIcon(h, v)}
              </span>
            )
          }
          return h('el-table-column', option)
        })
    },

    renderTreeTableColumn(h, params, item) {
      const tdCellList = this.treeTableData[params.$index][item.key]
      const tdCellsNum = this.tableCellNumData[params.$index][item.key]
      if (!item.key && item.render) {
        return item.render(h, params)
      }
      return (
        <ul class={[item.control ? 'td-cell main-td' : 'td-cell child-td']}>
          {tdCellList.map((tdItem, trIndex) => {
            return (
              <li style={{ flex: tdCellsNum[trIndex] || 1 }}>
                {item.render ? item.render(h, params, trIndex) : tdItem}
              </li>
            )
          })}
        </ul>
      )
    },
    renderPagination(h) {
      const _this = this
      if (!this.hasPage) return
      return h('el-pagination', {
        props: {
          pageSizes: this.pageSizes,
          pageSize: this.internalPageSize,
          layout: 'total, sizes, prev, pager, next, jumper',
          total: this.internalTotal || 0,
          currentPage: this.internalCurrentPage,
          ...this.paginationProps,
        },
        class: {
          pagination: true,
        },
        on: {
          'size-change'(value) {
            const key = _this.currentSettingStoragekey
            oPageTableCacheSetting[key] = {
              pageSize: value,
            }
            _this.internalPageSize = value
            // 记录当前pageSize信息
            localStorage.setItem(
              oPageTableCacheSettingStoragekey,
              JSON.stringify(oPageTableCacheSetting),
            )
          },
          'update:currentPage'(val) {
            _this.internalCurrentPage = val || 1
          },
          ...this.$listeners,
        },
      })
    },
  },
  render(h) {
    return (
      <div class="table-page">
        {[
          <div
            class={
              'table-page-header ' +
              (this.oCloudSearch ? 'table-o-cloud-search' : 'table-o-search')
            }>
            <div class="table-page-left">
              {[
                this.renderOSearch(h),
                this.renderOCloudSearch(h),
                !this.oCloudSearch && this.renderTools(h),
              ]}
            </div>
            <div class="table-page-right">{this.renderButtonGroup(h)}</div>
          </div>,
          this.renderTable(h),
          <div class="pagination-box">{this.renderPagination(h)}</div>,
          <el-tooltip effect="dark" placement="top" ref="tooltip" content={ this.tooltipContent }></el-tooltip>,
          this.renderSettingsDialog(h),
        ]}
      </div>
    )
  },
}
</script>
<style lang="scss">
.el-popover.o-page-table-filter-popover {
  padding: 0;

  .label-list {
    li {
      cursor: pointer;
      line-height: 28px;

      &:hover {
        color: #2fc29b;
        border-color: #c1ede1;
        background-color: #eaf9f5;
      }

      span {
        margin-left: 6px;
      }
    }
  }

  .o-search-current-editor-pannal {
    box-shadow: none;
  }
}
</style>
<style lang="scss" scoped>
.cell-box {
  display: flex;
  .inner-text {
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
}
.cell-box:hover .copy-icon {
  display: block;
  cursor: pointer;
}
.table-page {
  .table {
    margin-bottom: 20px;
  }

  .table-page-header {
    margin-bottom: 10px;
    display: flex;
    align-items: center;

    &.table-o-cloud-search {
      align-items: end;
      .table-page-left {
        align-items: end;
        .o-cloud-search-container {
          display: flex;
        }
      }
    }

    .table-page-left,
    .table-page-right {
      flex-grow: 1;
      display: flex;
      align-items: center;
    }

    .table-page-right {
      justify-content: flex-end;
      .button-group-slot {
        ::v-deep {
          .el-button + .o-dropdown-arrow-button {
            margin-left: 10px;
          }
        }
      }
    }
  }

  .settings-dialog {
    .alert {
      margin-bottom: 15px;
    }
    .settings-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .reset-button {
      color: #999;
      margin-bottom: 15px;
      &:hover {
        color: $theme-color;
      }
    }
    ::v-deep {
      .el-checkbox {
        width: 25%;
        margin-right: 0;
        margin-bottom: 15px;
      }
    }
  }
  .loop-control-btn {
    width: 32px;
    height: 32px;
    position: relative;
    margin-left: 10px;
    cursor: pointer;
    .loop-border {
      color: #97e1cd;
      position: absolute;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
    }
    .rect-path {
      stroke-dasharray: 383;
      stroke-dashoffset: 383;
      animation-timing-function: linear;
      animation-fill-mode: both;
    }
    .rect-path-animate {
      animation-name: dash;
    }
  }
  @keyframes dash {
    to {
      stroke-dashoffset: 0;
    }
  }
  .tools-list {
    display: flex;
    font-size: 14px;
    ::v-deep {
      .el-button {
        padding: 5px 10px;
        font-size: 20px;
      }
    }
  }
  .pagination-box {
    display: flex;
    justify-content: flex-end;
  }
  .o-search {
    max-width: 400px;
    margin-right: 10px;
  }
  .filter-icon {
    margin-left: 5px;
    cursor: pointer;
    color: #c0c4cc;
  }
  ::v-deep {
    table {
      .el-button-group,
      .operate-btn-group {
        .el-button {
          padding: 0;
          color: $theme-color;
          border: 0;
          background: none;
          line-height: 22px;
          display: inline-block;
          margin: 0 6px 0 6px;
          font-size: 13px;
        }
        .el-button:after,
        .with-separate:after {
          content: '|';
          width: 1px;
          display: inline-block;
          margin-left: 6px;
          color: #ccc;
        }
        .el-button:last-child:after {
          display: none;
        }
      }

      .o-cel-tooltip {
        display: flex;
        justify-content: center;
        align-items: center;
        // 兼容自定义渲染触发tooltip显示
        width: calc(100% + 1px);
        padding-right: 1px;
      }
    }
    .column-text-ellipsis-nowrap {
      @include text-ellipsis(1);
      * {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        width: 100%;
      }
    }
    .column-text-ellipsis-wrap{
      @include text-ellipsis(2);
    }
    .copy-icon {
      display: none;
      width: 20px;
    }
    .split-table tbody td {
      min-height: 60px;
    }
    .split-table tbody .cell {
      height: 100%;
      display: flex;
      justify-content: center;
      align-items: center;
      padding: 0;
    }
  }

  .split-table .td-cell {
    width: 100%;
    padding: 0;
    height: 100%;
    display: flex;
    align-items: center;
    justify-content: center;
  }
  .split-table .td-cell {
    text-align: center;

    > li {
      border-bottom: 1px solid #eff0f0;
      min-height: 48px;

      &:last-child {
        border-bottom: 0;
      }
    }
  }
  // 多行合并表格
  .split-table {
    .td-cell {
      display: flex;
      flex-direction: column;
      height: 100%;

      > li {
        display: flex;
        align-items: center;
        vertical-align: middle;
        text-align: center;
        justify-content: center;
        width: 100%;
      }
    }
  }
}
</style>
