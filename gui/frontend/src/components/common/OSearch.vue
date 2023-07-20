<script>
import { traversBy, collectionToMap } from '@/utils'
export default {
  name: 'OSearch',
  props: {
    // 搜索项集合
    searchList: {
      type: Array,
      required: true,
      default: () => [],
    },
    // sync功能
    searchValue: {
      type: Object,
      default() {
        return {}
      },
    },
    // 占位提示
    placeholder: {
      type: String,
      default: '请点击选择筛选内容',
    },
    // tag最大长度
    tagMaxWidth: {
      type: Number,
      default: 150,
    },
  },
  data() {
    return {
      internalSearchValue: this.initValue(),
      searchText: '',
      isFoucs: false,
      currentEditor: null, // 当前正在搜索的配置对象
      currentEditorPlacement: 'inner', // 当前正在搜索的选择框框放置的位置
      selectChecked: [],
      dateValue: [],
      radioChecked: '',
    }
  },
  computed: {
    currentSelectEditorCheckAll: {
      get() {
        if (this.currentEditor && this.currentEditor.type === 'select') {
          return this.selectChecked.length === this.currentEditor.options.length
        }
        return false
      },
      set(val) {
        this.selectChecked = val
          ? this.currentEditor.options.map(v => v.text)
          : []
      },
    },
    // 通常情况下，用户必须是需要先选择要搜索的栏目且输入内容后回车才能确认搜索
    // 但是通常有时候用户想要一个默认搜索项，直接输入内容回车后如果无法匹配到当前其他的搜索条件情况下就采用默认搜索项
    // 我们将以searchList的第一个设置了defaultSearch的项目作为默认搜索项
    defaultEditor() {
      return this.searchList.find(v => v.defaultSearch)
    },
    // 快速通过key来找到配置对象
    searchListMap() {
      const res = {}
      this.searchList.forEach(v => {
        res[v.key] = v
      })
      return res
    },
    // 快速通过title来找到配置对象
    searchListTitleMap() {
      const res = {}
      this.searchList.forEach(v => {
        res[v.title] = v
      })
      return res
    },
    // 下拉选项列表
    dropdownList() {
      if (!this.searchText) return this.searchList
      return this.searchList.filter(v => v.title.indexOf(this.searchText) > -1)
    },
    // 鼠标放上输入框时高亮的提示的文本
    tagText() {
      if (!this.tagList.length) return '未选择'
      return this.tagList
        .map(([{ text: kk, value: kv }, { text: vk, value: vv }]) => {
          return `${kk}: ${vk}`
        })
        .join('\n')
    },
    // 当前已选择的项目的tag列表
    tagList() {
      return Object.entries(this.internalSearchValue)
        .filter(([k, v]) => !!v)

        .map(([k, v], i) => {
          const item = this.searchListMap[k]
          let vv = v
          let map
          switch (item.type) {
            case 'select':
              // 通过英文key找到配置里的中文
              map = collectionToMap(item.options, 'value', 'text')
              vv = this.changeSelectKv(map, v)
              break
            case 'radio':
              // 通过英文key找到配置里的中文
              map = collectionToMap(item.options, 'value', 'text')
              vv = map[v]
              break
            case 'date':
              // 通过英文key找到配置里的中文
              vv = v.join(' - ')
              break
          }
          return [
            // 键的英文key和中文映射
            { text: item.title, value: k },
            // 值的英文key和中文映射
            {
              text: vv,
              value: v,
            },
          ]
        })
    },
    // 除了当前正在编辑的tag以外剩下的tag列表
    noCurrentEditorTagList() {
      const { currentEditor } = this
      return this.tagList.filter(
        ([ko, vo]) => !currentEditor || ko.value !== currentEditor.key,
      )
    },
  },
  watch: {
    async isFoucs(val) {
      await this.$nextTick()
      this.$refs.input[val ? 'focus' : 'blur']()
    },
    selectChecked(val, oldval) {
      this.setSearchTextValue(val)
    },
    radioChecked(val, oldval) {
      this.setSearchTextValue(val)
    },
    dateValue(val, oldval) {
      this.setSearchTextValue(val.length ? '已选择' : '未选择')
    },
    searchValue: {
      deep: true,
      immediate: true,
      handler(val, oldVal) {
        this.internalSearchValue = { ...val }
      },
    },
  },
  mounted() {
    window.addEventListener('click', this.handlerWindowClick, false)
    this.$on('hook:beforeDestroy', () => {
      window.removeEventListener('click', this.handlerWindowClick, false)
    })
  },
  methods: {
    // 模拟blur效果
    handlerWindowClick(e) {
      if (this.isFoucs && !this.$el.contains(e.target)) {
        this.isFoucs = false
        this.clear()
      }
    },
    // 默认值
    initValue() {
      const map = {
        input: '',
        select: '',
        radio: '',
        date: [],
      }
      const res = {}
      traversBy(this.searchList, void 0, item => {
        let defaultValue = ''
        defaultValue =
          item.defaultValue !== undefined ? item.defaultValue : map[item.type]
        if (item.key) {
          res[item.key] = defaultValue
        }
      })
      this.$emit('update:searchValue', res)
      return res
    },
    renderTagsList(h) {
      return (
        <ul class="tag-list">
          {this.noCurrentEditorTagList.map(([ko, vo]) => {
            return (
              <li>
                <el-tag
                  size="mini"
                  closable
                  onClose={e => {
                    e.stopPropagation()
                    return this.tagClose(ko, vo)
                  }}
                  onClick={e => {
                    e.stopPropagation()
                    return this.tagClick(ko, vo)
                  }}>
                  <span
                    style={{ maxWidth: `${this.tagMaxWidth}px` }}
                    class="tag-inner">{`${ko.text}: ${vo.text}`}</span>
                </el-tag>
              </li>
            )
          })}
        </ul>
      )
    },
    renderCurrentEditor(h) {
      if (this.currentEditorPlacement !== 'inner') return
      return (
        <div class="current-editor">{this.renderCurrentEditorPannal(h)}</div>
      )
    },
    // 渲染当前的选择类型对应的编辑框架子
    renderCurrentEditorPannal(h) {
      if (!this.currentEditor) return
      const { type } = this.currentEditor
      if (type === 'input') return
      return (
        <div class="o-search-current-editor-pannal">
          <div class="body">{this.renderCurrentEditorInner(h)}</div>
          <div class="footer">
            <el-button
              onClick={e => {
                e.stopPropagation()
                return this[`${type}Submit`](this.searchText)
              }}
              size="mini"
              type="primary">
              确定
            </el-button>
            <el-button
              onClick={e => {
                e.stopPropagation()
                return this.clear()
              }}
              size="mini">
              取消
            </el-button>
          </div>
        </div>
      )
    },
    // 渲染当前的选择类型对应的编辑框内容
    renderCurrentEditorInner(h) {
      switch (this.currentEditor.type) {
        case 'select':
          return this.renderSelect(h)
        case 'radio':
          return this.renderRadio(h)
        case 'date':
          return this.renderDatePicker(h)
      }
    },
    renderSelect(h) {
      return (
        <div class="current-editor-select">
          <p>
            <el-checkbox
              value={this.currentSelectEditorCheckAll}
              onInput={v => (this.currentSelectEditorCheckAll = v)}>
              全选
            </el-checkbox>
          </p>
          <el-checkbox-group
            value={this.selectChecked}
            onInput={v => (this.selectChecked = v)}>
            {this.currentEditor.options.map(v => (
              <el-checkbox class="item" label={v.text}></el-checkbox>
            ))}
          </el-checkbox-group>
        </div>
      )
    },
    renderRadio(h) {
      return (
        <div class="current-editor-radio">
          <el-radio-group
            value={this.radioChecked}
            onInput={v => (this.radioChecked = v)}>
            {this.currentEditor.options.map(v => (
              <el-radio class="item" label={v.text}></el-radio>
            ))}
          </el-radio-group>
        </div>
      )
    },
    renderDatePicker(h) {
      const props = {
        'value-format': 'yyyy-MM-dd',
        type: 'daterange',
        align: 'right',
        'unlink-panels': true,
        'start-placeholder': '开始日期',
        'end-placeholder': '结束日期',
        ...(this.currentEditor.props || {}),
      }
      return (
        <div class="current-editor-datepicker">
          <el-date-picker
            value={this.dateValue}
            onInput={v => (this.dateValue = v || [])}
            {...{ props }}></el-date-picker>
        </div>
      )
    },
    renderDropdown(h) {
      /* eslint-disable */
      return this.isFoucs &&
        !/:/.test(this.searchText) &&
        this.dropdownList.length ? (
        <ul class="dropdown-list">
          <p>选择资源属性进行过滤</p>
          {this.dropdownList
            .filter(v => !this.searchValue[v.key])
            .map(v => {
              return (
                <li
                  onClick={e => {
                    e.stopPropagation()
                    return this.itemSearch(v)
                  }}>
                  {v.title}
                </li>
              )
            })}
        </ul>
      ) : (
        ''
      )
      /* eslint-enable */
    },
    renderSearchInput(h) {
      return (
        <el-popover
          popper-class="cloud-o-search-popover"
          class="real-search-input"
          style={{ display: this.isFoucs ? 'block' : 'none' }}
          onInput={value => (this.isFoucs = value)}
          trigger="manual"
          value={this.isFoucs}>
          <div slot="reference">
            <input
              ref="input"
              onKeyup={this.enterSearch}
              onInput={e => (this.searchText = e.target.value)}
              value={this.searchText}></input>
          </div>

          {[this.renderDropdown(h), this.renderCurrentEditor(h)]}
        </el-popover>
      )
    },
    tagClose(ko, vo) {
      this.setSearchValue(ko.value, '')
    },
    tagClick({ text: kk, value: kv }, { text: vk, value: vv }) {
      this.isFoucs = true
      const item = this.searchListMap[kv]
      this.itemSearch(item)
      let map
      switch (item.type) {
        case 'input':
          this.searchText += vk
          break
        case 'select':
          // 把英文key映射回中文显示
          map = collectionToMap(item.options, 'value', 'text')
          this.selectChecked = vv.split(',').map(v => map[v])
          break
        case 'radio':
          // 把英文key映射回中文显示
          map = collectionToMap(item.options, 'value', 'text')
          this.radioChecked = map[vv]
          break
        case 'date':
          this.dateValue = vv.slice()
          break
      }
    },
    // 适用于已经组装好配置对象来进行聚合搜索，并且是新搜索
    itemSearch(item) {
      this.searchText = `${item.title}: `
      this.currentEditor = item
      this.currentEditorPlacement = 'inner'
      this.$refs.input.focus()
    },
    // 对外提供的方法函数，通过传key值来触发聚合搜索，可以是新搜索也可以是已有搜索的修改
    // placement： inner表示把选择框渲染到里面，outer表示把选择框渲染到外面
    keyFilter(key, placement = 'outer') {
      this.isFoucs = true
      if (this.searchValue[key]) {
        const tag = this.tagList.find(([ko]) => ko.value === key)
        this.tagClick(tag[0], tag[1])
      } else {
        const item = this.searchList.find(v => v.key === key)
        this.itemSearch(item, placement)
      }
      this.currentEditorPlacement = placement
    },
    commonSubmit(text, formatData) {
      let key
      let val
      if (/([^:]+):(.+)/.test(text)) {
        const exec = /([^:]+):(.+)/.exec(text)
        key = exec[1]
        val = exec[2].trim()
      } else if (this.defaultEditor) {
        key = this.defaultEditor.title
        val = text
      }
      if (key && val) {
        const item = this.searchListTitleMap[key]
        if (item) {
          let resVal = val
          if (formatData) {
            resVal = formatData(val)
          }
          this.setSearchValue(item.key, resVal)
          this.clear()
        }
      }
    },
    inputSubmit(text) {
      return this.commonSubmit(text)
    },
    radioSubmit(text) {
      return this.commonSubmit(text, val => {
        const { options } = this.currentEditor
        const map = collectionToMap(options, 'text', 'value')
        return map[val]
      })
    },
    selectSubmit(text) {
      return this.commonSubmit(text, val => {
        const { options } = this.currentEditor
        const map = collectionToMap(options, 'text', 'value')
        return this.changeSelectKv(map, val)
      })
    },
    dateSubmit(text) {
      const { dateValue } = this
      if (!dateValue.length) return
      return this.commonSubmit(text, val => {
        return dateValue
      })
    },
    // 通过回车的方式来搜索
    enterSearch(e) {
      if (e.keyCode !== 13) return
      this.confirmSearch()
    },
    // 通过点击搜索按钮来搜索
    buttonSearch(e) {
      e.stopPropagation()
      this.confirmSearch()
    },
    // 确认搜索
    confirmSearch() {
      const value = this.searchText
      if (!value) return
      if (!this.currentEditor && this.defaultEditor) {
        this.currentEditor = this.defaultEditor
      }
      if (!this.currentEditor) return
      return this[`${this.currentEditor.type}Submit`](value)
    },
    renderToolList(h) {
      return (
        <ul class="tool-list">
          <li>
            <i
              onClick={e => {
                e.stopPropagation()
                return this.reset()
              }}
              class="el-icon-circle-close"></i>
          </li>
          <li>
            <i onClick={this.buttonSearch} class="el-icon-search"></i>
          </li>
        </ul>
      )
    },
    renderPlaceholder(h) {
      if (this.placeholder && !this.tagList.length && !this.isFoucs) {
        return (
          <span
            onClick={e => {
              e.stopPropagation()
              this.isFoucs = true
            }}
            class="placeholder">
            {this.placeholder}
          </span>
        )
      }
    },
    clear() {
      this.searchText = ''
      this.isFoucs = false
      this.currentEditor = null
      this.selectChecked = []
      this.radioChecked = ''
      this.dateValue = []
      this.$emit('clear')
    },
    reset() {
      this.clear()
      this.$emit('update:searchValue', {})
    },
    // 把一组值根据映射规则缓过来
    // 例如running,maintain 换为运行中，维护中
    changeSelectKv(map, val) {
      return val
        .split(',')
        .map(v => map[v])
        .join(',')
    },
    setSearchValue(k, v) {
      this.$emit('update:searchValue', {
        ...this.searchValue,
        [k]: v,
      })
    },
    // 设置搜索文字的值的部分的文字
    setSearchTextValue(value) {
      this.searchText = this.searchText.replace(
        /([^:]+):\s*(.+)/,
        `$1: ${value}`,
      )
    },
  },
  render(h) {
    return (
      <el-tooltip
        popper-class="o-search-namespace"
        placement="top"
        content={this.tagText}>
        <div
          class={{ active: this.isFoucs, 'o-search': true }}
          onClick={() => {
            this.isFoucs = true
          }}>
          {[
            this.renderPlaceholder(h),
            this.renderTagsList(h),
            this.renderSearchInput(h),
            this.renderToolList(h),
          ]}
        </div>
      </el-tooltip>
    )
  },
}
</script>
<style lang="scss">
.o-search-namespace {
  white-space: pre-wrap;
}
.o-search-current-editor-pannal {
  min-width: 150px;
  border: 1px solid #ebeef5;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
  .footer {
    padding: 0 10px;
    display: flex;
    justify-content: center;
    align-items: center;
    height: 48px;
    border-top: 1px solid #ddd;
  }
  .current-editor-datepicker {
    padding: 10px;
  }
  .el-radio,
  .el-checkbox {
    width: 100%;
    margin-right: 0px;
    padding: 0 10px;
    line-height: 40px;
    &:hover {
      background-color: #f2f2f2;
    }
  }
  .el-radio-group,
  .el-checkbox-group {
    display: flex;
    flex-direction: column;
  }
}
</style>
<style lang="scss" scoped>
.o-search {
  position: relative;
  -webkit-appearance: none;
  background-color: #fff;
  background-image: none;
  border-radius: 4px;
  border: 1px solid #dcdfe6;
  -webkit-box-sizing: border-box;
  box-sizing: border-box;
  color: #606266;
  display: inline-block;
  font-size: inherit;
  height: 32px;
  outline: 0;
  padding: 1px 55px 1px 15px;
  -webkit-transition: border-color 0.2s cubic-bezier(0.645, 0.045, 0.355, 1);
  transition: border-color 0.2s cubic-bezier(0.645, 0.045, 0.355, 1);
  width: 100%;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  overflow-y: hidden;
  .placeholder {
    color: #999;
  }
  &.active {
    overflow-y: initial;
    min-height: 32px;
    height: auto;
    border-color: $theme-color;
  }
  .tool-list {
    display: flex;
    position: absolute;
    right: 0;
    align-items: center;
    font-size: 20px;
    cursor: pointer;
    li {
      margin-right: 8px;
    }
  }
  .current-editor {
    position: absolute;
    z-index: 10;
    border: 1px solid #ddd;
    background-color: #fff;
    box-shadow: 0 2px 3px 0 rgba(0, 0, 0, 0.2);
    transform: translateY(10px);
  }
  .real-search-input {
    margin-bottom: 4px;
  }

  .tag-list {
    display: flex;
    flex-wrap: wrap;
    ::v-deep {
      .el-tag {
        display: flex;
        align-items: center;
      }
      .el-tag__close {
        top: 0;
      }
    }
    .tag-inner {
      cursor: pointer;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    li {
      display: flex;
      align-items: center;
      margin-right: 5px;
      margin-top: 4px;
      margin-bottom: 4px;
    }
  }
}
.search-input {
  position: relative;
  margin-bottom: 0;
  input {
    border: 0;
    outline: none;
  }
}

.dropdown-list {
  background-color: #fff;
  border: 1px solid #ebeef5;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
  min-width: 150px;
  p {
    background-color: transparent;
    color: #bbb;
    line-height: 20px;
    font-size: 12px;
    padding: 5px 10px;
  }
  li {
    margin-bottom: 0;
    font-size: 12px;
    padding: 6px 10px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    color: #000;
    display: block;
    cursor: pointer;
    line-height: 24px;
    &:hover {
      background-color: #f2f2f2;
    }
  }
}
</style>
<style lang="scss">
.cloud-o-search-popover {
  box-shadow: none;
  border: none;
  > .popper__arrow {
    display: none;
  }
}
</style>
