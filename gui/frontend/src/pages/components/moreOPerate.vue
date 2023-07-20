<script>
const renderLi = (h, list) => {
  return list.map(item => {
    return h('li', {}, [item])
  })
}
export default {
  functional: true,
  props: {
    count: {
      type: Number,
      default: 4
    },
    title: {
      type: String,
      default: '更多操作'
    },
    type: {
      type: String,
      default: 'text'
    },
    poperClass: {
      type: String,
      default: ''
    },
    width: {
      type: [String, Number],
      default: 'auto'
    },
    visibleArrow: {
      type: Boolean,
      default: true
    }
  },
  render(h, context) {
    const { count, title, type, poperClass, width, visibleArrow } = context.props
    context.children = context.children.filter(item => {
      return item.tag
    })
    const children = context.children
    const childrenLen = children.length
    return h(
      'div',
      {},
      [
        ...children.slice(0, childrenLen > count ? count - 1 : count),
        childrenLen > count
          ? h('o-dropdown', {
            class: 'm-l o-dropdown-arrow-text',
            props: {
              type, poperClass, width, visibleArrow
            },
            scopedSlots: {
              header: props => h('span', title),
              list: props =>
                h('ul', {}, [...renderLi(h, children.slice(count - 1))])
            }
          })
          : ''
      ]
    )
  }
}
</script>
<style lang="scss" scoped>
.m-l {
  margin-left: 5px;
}
</style>
