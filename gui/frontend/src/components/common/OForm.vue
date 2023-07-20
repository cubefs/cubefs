<script>
const getPrefix = (tag, lib) => {
  const iviewMap = {
    form: 'i-form',
    'form-item': 'form-item',
    input: 'i-input',
    select: 'i-select',
    option: 'i-option',
    checkbox: 'checkbox',
    'checkbox-group': 'checkbox-group',
    'date-picker': 'date-picker',
    'time-picker': 'time-picker',
    radio: 'radio',
    'radio-group': 'radio-group',
    switch: 'i-switch',
    slider: 'slider',
    button: 'i-button',
    row: 'row',
    col: 'i-col',
    'input-number': 'input-number',
    cascader: 'cascader',
    text: 'text'
  }
  const elementMap = {
    form: 'el-form',
    'form-item': 'el-form-item',
    input: 'el-input',
    select: 'el-select',
    option: 'el-option',
    checkbox: 'el-checkbox',
    'checkbox-group': 'el-checkbox-group',
    'date-picker': 'el-date-picker',
    'time-picker': 'el-time-picker',
    radio: 'el-radio',
    'radio-group': 'el-radio-group',
    switch: 'el-switch',
    slider: 'el-slider',
    button: 'el-button',
    row: 'el-row',
    col: 'el-col',
    'input-number': 'el-input-number',
    cascader: 'el-cascader',
    text: 'text'
  }
  return lib === 'iview' ? iviewMap[tag] : elementMap[tag]
}
export default {
  name: 'OForm',
  props: {
    // grid 间距
    gutter: {
      type: Number,
      default: 30
    },
    // span 间距
    span: {
      type: Number,
      default: 8
    },
    // formItem 项
    formList: {
      type: [Array, Object],
      required: true,
      default: () => []
    },
    // 是否显示整个控制按钮
    notCtrl: {
      type: Boolean,
      default: false
    },
    // 是否开启 input 标签默认
    enterSubmit: {
      type: Boolean,
      default: false
    },
    // 默认 ui 库
    lib: {
      type: String,
      default: 'element'
    },
    // 默认标签宽度
    labelWidth: {
      type: [Number, String],
      default: 120
    },
    // 默认内容宽度
    // eslint-disable-next-line vue/require-default-prop
    contentWidth: {
      type: [Number, String]
      // default: 240
    },
    // submit 按钮文本
    submitText: {
      type: String,
      default: '提交'
    },
    // 重置按钮文本
    resetText: {
      type: String,
      default: '重置'
    },
    // 是否拥有 提交 按钮
    hasSubmitBtn: {
      type: Boolean,
      default: true
    },
    // 是否拥有 重置 按钮
    hasResetBtn: {
      type: Boolean,
      default: true
    },
    // 原生 form 标签上的 props
    options: {
      type: Object,
      default() {
        return {}
      }
    },
    // 开启全局 clearable
    clearable: {
      type: Boolean,
      default: true
    },
    // 文本框默认字符个数
    // eslint-disable-next-line vue/require-default-prop
    maxlength: {
      type: [Number, String]
    },
    // 多行文本框默认字符个数
    textareaMaxlength: {
      type: Number,
      default: 256
    },
    // 是否全局 disabled
    disabled: {
      type: Boolean,
      default: false
    },
    // sync功能
    formValue: {
      type: Object,
      default() {
        return {}
      }
    },
    // 表单实例对象
    formRef: {
      type: Object,
      default() {
        return {}
      }
    }
  },
  data() {
    return {
      form: this.initForm(),
      rules: this.initRules()
    }
  },
  watch: {
    formValue: {
      deep: true,
      immediate: true,
      handler(val) {
        this.setForm(val)
      }
    },
    formList: {
      deep: true,
      handler(val) {
        this.rules = this.initRules()
      }
    }
  },
  mounted() {
    this.$emit('update:formRef', this)
  },
  methods: {
    isUndefined(o) {
      return o === void 0
    },
    isArray(o) {
      return Object.prototype.toString.call(o) === '[object Array]'
    },
    initRules() {
      const rules = {}
      this.traversBy(this.formList, item => {
        if (item.rule !== undefined) {
          rules[item.key] = item.rule
        }
      })
      return rules
    },
    // 默认值
    initForm() {
      const map = {
        input: '',
        select: null,
        checkbox: false,
        'checkbox-group': [],
        date: new Date(),
        datetime: new Date(),
        daterange: [],
        datetimerange: [],
        time: '',
        radio: false,
        'radio-group': '',
        slider: 0,
        switch: false,
        'input-number': 0,
        cascader: [],
        text: ''
      }
      const res = {}
      this.traversBy(this.formList, item => {
        let defaultValue = ''
        defaultValue =
          item.defaultValue !== undefined ? item.defaultValue : map[item.type]
        if (item.key) {
          res[item.key] = defaultValue
        }
      })
      this.$emit('update:formValue', res)
      return res
    },
    traversBy(item, fn) {
      if (typeof item !== 'object') return
      if (this.isArray(item) || (item.children && !item.type)) {
        for (const key in item) {
          if (item.hasOwnProperty(key)) {
            this.traversBy(item[key], fn)
          }
        }
      } else {
        fn(item)
      }
    },
    getHypeScript() {
      return this.$parent.$createElement
    },
    renderFormList(h, formList, isRoot) {
      if (typeof formList !== 'object') return h()
      if (!this.isArray(formList)) {
        const formListChildren = formList.children
        if (formListChildren && !formList.type) {
          if (!this.isArray(formListChildren)) return h()
          return formListChildren.map(v => this.renderFormList(h, v))
        } else {
          return this.getConditionItem(
            h,
            formList,
            this.getFormItem(h, formList, this.getContent(h, formList))
          )
        }
      } else {
        return this.getRow(
          h,
          formList.map(item => {
            if (this.isArray(item)) {
              return this.renderFormList(h, item, false)
            } else {
              const childrenItem = this.getFormItem(
                h,
                item,
                this.getContent(h, item)
              )
              const childrenParts = h(
                getPrefix('col', this.lib),
                {
                  props: {
                    span: item.span || this.span
                  }
                },
                [childrenItem]
              )
              return this.getConditionItem(h, item, childrenParts)
            }
          })
        )
      }
    },
    getConditionItem(h, item, node) {
      return this.isUndefined(item.condition) || !!item.condition === true
        ? node
        : h()
    },
    getEmptyVNode(h) {
      return h()
    },
    getFormList(h) {
      return this.formList.map(item => {
        return this.getFormItem(h, item, this.getContent(h, item))
      })
    },
    getRow(h, childrenList) {
      return h(
        getPrefix('row', this.lib),
        {
          props: {
            gutter: this.gutter
          }
        },
        childrenList
      )
    },
    getContent(h, item) {
      let content
      switch (item.type) {
        case 'input':
          content = this.renderInput(h, item)
          break
        case 'select':
          content = this.renderSelect(h, item)
          break
        case 'checkbox':
          content = this.renderCheckbox(h, item)
          break
        case 'checkbox-group':
          content = this.renderCheckboxGroup(h, item)
          break
        case 'date':
          content = this.renderDatePicker(h, item)
          break
        case 'datetime':
          content = this.renderDatePicker(h, item)
          break
        case 'daterange':
          content = this.renderDateRange(h, item)
          break
        case 'datetimerange':
          content = this.renderDateRange(h, item)
          break
        case 'time':
          content = this.renderTimePicker(h, item)
          break
        case 'radio':
          content = this.renderRadio(h, item)
          break
        case 'radio-group':
          content = this.renderRadioGroup(h, item)
          break
        case 'switch':
          content = this.renderSwitch(h, item)
          break
        case 'slider':
          content = this.renderSlider(h, item)
          break
        case 'input-number':
          content = this.renderInputNumber(h, item)
          break
        case 'cascader':
          content = this.renderCascader(h, item)
          break
        case 'text':
          content = this.renderText(h, item)
          break
        default:
          if (typeof item.renderContent === 'function') {
            content = item.renderContent(this.getHypeScript(), item, this.form)
          }
      }
      return content
    },
    getFormItem(h, item, content) {
      if (typeof item.render === 'function') {
        return item.render(this.getHypeScript(), item, this.form)
      } else {
        const settings = {
          props: {
            prop: item.key
          }
        }
        return h(
          getPrefix('form-item', this.lib),
          Object.assign(
            settings,
            {
              class: [item.showTips ? 'has-tips-icon' : '']
            },
            item.settings
          ),
          [
            this.renderTitle(h, item, this.form),
            h(
              'div',
              {
                class: 'form-item-content-inner'
              },
              [content]
            ),
            item.showTips ? this.renderTips(item.tipsText, item, this.form) : ''
          ]
        )
      }
    },
    // 先表单项后面渲染小问号
    renderTips(title, item) {
      return item.tipsType === 'bottom' ? (
        <p class="form-item-bottom-tips">{title}</p>
      ) : (
        <el-tooltip placement="top" content={title} class="question">
          <i class="el-icon-question" />
        </el-tooltip>
      )
    },
    // 渲染 title
    renderTitle(h, item) {
      return (
        <span slot="label">
          {item.required === true ? <span style="color: font">*</span> : ''}
          {typeof item.renderTitle === 'function' ? (
            <span>{item.renderTitle(h, item, this.form)}</span>
          ) : (
            <span>{item.title}</span>
          )}
        </span>
      )
    },
    renderText(h, item) {
      return <span class="text">{this.form[item.key]}</span>
    },
    // 渲染提交 按钮
    renderSubmit(h) {
      const btns = []
      if (this.hasSubmitBtn) {
        btns.push(
          h(
            getPrefix('button', this.lib),
            {
              props: {
                type: 'primary'
              },
              on: {
                click: this.submit
              }
            },
            this.submitText
          )
        )
      }
      if (this.hasResetBtn) {
        btns.push(
          h(
            getPrefix('button', this.lib),
            {
              style: {
                'margin-left': '10px'
              },
              on: {
                click: this.reset
              }
            },
            this.resetText
          )
        )
      }
      return h(getPrefix('form-item', this.lib), btns)
    },
    // 渲染 input
    renderInput(h, item) {
      const props = item.props || {}
      const attrs = item.attrs || {}
      // 让 element-ui 在 props 里也可以设置 placeholder
      if (props.placeholder) {
        attrs.placeholder = props.placeholder
      }
      // 让 element-ui 在 props 里也可以设置 maxlength
      if (props.type !== 'textarea') {
        attrs.maxlength = +props.maxlength || +this.maxlength
      } else {
        // textarea 长度
        attrs.maxlength = +props.maxlength || +this.textareaMaxlength
      }
      item.attrs = attrs
      const tag = {
        h,
        item,
        tagName: getPrefix('input', this.lib),
        props: {
          clearable: this.clearable,
          ...props
        },
        nativeOn: {
          keydown: e => {
            if (
              e.keyCode === 13 &&
              this.enterSubmit &&
              props.type !== 'textarea'
            ) {
              this.submit()
            }
          }
        }
      }
      return this.generateTag(tag)
    },
    // 渲染 select
    renderSelect(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('select', this.lib),
        props: {
          clearable: this.clearable,
          ...(item.props || {})
        },
        children: item.options.map(option => {
          return h(
            getPrefix('option', this.lib),
            {
              props: {
                label: option.text,
                value: option.value,
                ...(item.optionProps || {})
              }
            },
            [
              typeof item.renderOption === 'function'
                ? item.renderOption(h, option, item)
                : item.text
            ]
          )
        })
      }
      return this.generateTag(tag)
    },
    // 渲染 单个checkbox
    renderCheckbox(h, item) {
      const props = item.props || {}
      if (item.border) {
        props.border = true
      }
      const tag = {
        h,
        item,
        tagName: getPrefix('checkbox', this.lib),
        props,
        children: item.text
      }
      return this.generateTag(tag)
    },
    // 渲染 checkbox group
    renderCheckboxGroup(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('checkbox-group', this.lib),
        props: item.props || {},
        children: item.options.map(option => {
          return h(
            getPrefix('checkbox', this.lib),
            {
              props: {
                border: item.border,
                label: option.value
              }
            },
            option.text
          )
        })
      }
      return this.generateTag(tag)
    },
    // 渲染 datepicker
    renderDatePicker(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('date-picker', this.lib),
        props: {
          clearable: this.clearable,
          type: item.type,
          ...(item.props || {})
        }
      }
      return this.generateTag(tag)
    },
    // 渲染范围的 daterange
    renderDateRange(h, item) {
      // 处理 datetimerange 可能宽度不够的问题
      if (item.type === 'datetimerange') {
        item.width = item.width || 360
      }
      const tag = {
        h,
        item,
        tagName: getPrefix('date-picker', this.lib),
        props: {
          clearable: this.clearable,
          type: item.type,
          ...(item.props || {})
        }
      }
      return this.generateTag(tag)
    },
    renderTimePicker(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('time-picker', this.lib),
        props: {
          clearable: this.clearable,
          type: item.type,
          ...(item.props || {})
        }
      }
      return this.generateTag(tag)
    },
    // 渲染 radio
    renderRadio(h, item) {
      const props = item.props || {}
      if (item.border) {
        props.border = true
      }
      const tag = {
        h,
        item,
        tagName: getPrefix('radio', this.lib),
        props,
        children: item.text
      }
      return this.generateTag(tag)
    },
    // 渲染 radio group
    renderRadioGroup(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('radio-group', this.lib),
        props: item.props || {},
        children: item.options.map(option => {
          return h(
            getPrefix('radio', this.lib),
            {
              props: {
                border: item.border,
                label: option.value
              }
            },
            option.text
          )
        })
      }
      return this.generateTag(tag)
    },
    // 渲染 switch
    renderSwitch(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('switch', this.lib),
        props: item.props || {}
      }
      return this.generateTag(tag)
    },
    // 渲染 slider
    renderSlider(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('slider', this.lib),
        props: item.props || {}
      }
      return this.generateTag(tag)
    },
    // 渲染 slider
    renderInputNumber(h, item) {
      const tag = {
        h,
        item,
        tagName: getPrefix('input-number', this.lib),
        props: item.props || {}
      }
      return this.generateTag(tag)
    },
    // 渲染 cascader
    renderCascader(h, item) {
      const props = item.props || {}
      const tag = {
        h,
        item,
        tagName: getPrefix('cascader', this.lib)
      }
      if (this.lib === 'iview') {
        props.data = this.getCascaderOptions(item.options)
      } else {
        props.options = this.getCascaderOptions(item.options)
      }
      tag.props = props
      return this.generateTag(tag)
    },
    // 转换 cascader options
    getCascaderOptions(options = []) {
      let list = JSON.stringify(options)
      list = list.replace(/"text":/g, '"label":')
      return JSON.parse(list)
    },
    // 生产 tag
    generateTag({ h, item, tagName, props, children, on = {}, nativeOn = {} }) {
      const currProps = {
        value: this.form[item.key],
        min: 0,
        ...props,
        disabled: this.disabled || item.disabled
      }
      const attrs = item.attrs || {}
      let width = null
      const itemOn = item.on || {}
      const itemNativeOn = item.nativeOn || {}
      // 忽略这些标签的宽度设置
      const ignoreMap = {
        switch: true,
        checkbox: true,
        'checkbox-group': true,
        radio: true,
        'radio-group': true,
        'input-number': true
      }
      if (!ignoreMap[item.type]) {
        const w = item.width || this['contentWidth']
        if (typeof w === 'string' && (w.indexOf('%') >= 0 || w === 'auto')) {
          width = w
        } else {
          width = w + 'px'
        }
      }
      return h(
        tagName,
        {
          props: currProps,
          attrs,
          class: item.class || {},
          key: item.key,
          style: {
            width,
            ...(item.style || {})
          },
          on: {
            ...itemOn,
            input: value => {
              value = this.formatDateValue(value, item)
              this.form[item.key] =
                typeof value === 'string' &&
                item.trim !== false &&
                props.type !== 'textarea'
                  ? value.trim()
                  : value
              this.emitInput(value, item)
            },
            ...on
          },
          nativeOn: {
            ...itemNativeOn,
            ...nativeOn
          }
        },
        children
      )
    },
    // 格式化日期返回，避免 null 的出现
    formatDateValue(value, item) {
      switch (item.type) {
        case 'date':
        case 'datetitme':
          if (!value) {
            value = ''
          }
          break
        case 'daterange':
        case 'datetimerange':
          if (!value) {
            value = ['', '']
          }
          break
      }
      return value
    },
    async validate(...arg) {
      try {
        return await this.$refs.form.validate(...arg)
      } catch (e) {
        this.scrollToField()
        return Promise.reject(false)
      }
    },
    // 触发 item onInput 事件
    emitInput(value, item) {
      if (typeof item.onInput === 'function') {
        item.onInput(value, item, this.form)
      }
    },
    // 验证表单的时候将页面滚动到校验失败的是field
    scrollToField() {
      try {
        this.$refs.form.$el.querySelector('.is-error').scrollIntoView()
      } catch (e) {}
    },
    validateField(field) {
      const res = this.$refs.form.validateField(field)
      !res && this.scrollToField()
      return res
    },
    // 提交事件
    submit() {
      this.$refs.form.validate(valid => {
        this.scrollToField()
        this.$emit('submit', this.getForm(), valid)
      })
    },
    // 清空 form 表单
    reset() {
      this.clear()
      this.form = this.initForm()
      this.$refs.form.resetFields()
    },
    // 清空验证
    clear() {
      this.$refs.form.resetFields()
    },
    // 根据 key 获取 value
    getFormBykey(key) {
      return this.form[key]
    },
    // 获取整个 form
    getForm() {
      return {
        ...this.form
      }
    },
    // 设值
    setForm(form) {
      for (const key in form) {
        this.$set(this.form, key, form[key])
      }
      this.$emit('update:formValue', this.form)
    }
  },
  render(h) {
    return h(
      getPrefix('form', this.lib),
      {
        props: {
          model: this.form,
          rules: this.rules,
          'label-width':
            this.lib === 'iview'
              ? +this['labelWidth']
              : +this['labelWidth'] + 'px',
          ...this.options
        },
        ref: 'form',
        nativeOn: {
          submit(e) {
            e.preventDefault()
            e.stopPropagation()
          }
        }
      },
      [
        this.$slots.prepend,
        this.renderFormList(h, this.formList, true /* 根节点 */),
        !this.notCtrl && this.renderSubmit(h),
        this.$slots.default
      ]
    )
  }
}
</script>

<style lang="scss" scoped>
::v-deep {
  .is-error {
    .form-item-bottom-tips {
      display: none;
    }
  }
}
.form-item-bottom-tips {
  font-size: 12px;
  color: #c0c4cc;
  line-height: 1.3;
  position: absolute;
  top: 100%;
  left: 0;
}
.has-tips-icon {
  position: relative;
  ::v-deep {
    .el-tooltip {
      position: absolute;
      right: -25px;
      font-size: 20px;
      top: 50%;
      transform: translateY(-50%);
    }
  }
}
</style>
