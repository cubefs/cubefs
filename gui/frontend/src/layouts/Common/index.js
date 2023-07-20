import DashboardLayout from './DashboardLayout'
const layouts = {
  DashboardLayout,
}
export function layoutMixin(Vue) {
  // 全局注册组件
  Object.values(layouts).forEach(layout => {
    Vue.component(layout.name, layout)
  })
}
export const Common = {
  components: {
    DashboardLayout,
  },
  props: {
    hideSidebar: {
      type: Boolean,
      default: false,
    },
    appName: {
      type: String,
      default: 'Protal',
    },
  },
  render(h) {
    const component = 'DashboardLayout'
    return h(
      component,
      {
        props: this.$props,
      },
    )
  },
}
