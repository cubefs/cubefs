import Vue from 'vue'
import ElementUI from 'element-ui'
import 'element-ui/lib/theme-chalk/index.css'
import OIcon from '@/components/common/OIcon'
import OPageTable from '@/components/common/OPageTable.vue'
import OForm from '@/components/common/OForm.vue'
import ODropdown from '@/components/common/ODropdown.vue'
import App from './App.vue'
import router, { initMenu } from './router'
import store from './store'
import i18n from './i18n'
import '@/assets/styles/index.scss'

import { ajaxMixin } from './utils/ajax.js'
import { authMixin } from './utils/auth.js'

const components = {
  OIcon,
  OPageTable,
  OForm,
  ODropdown,
}

// 全局注册组件
Object.values(components).forEach(component => {
  Vue.component(component.name, component)
})

Vue.config.productionTip = false
Vue.use(ElementUI, {
  size: 'small',
})

initMenu()

ajaxMixin(Vue, { router })
authMixin(Vue, { router, store })

new Vue({
  router,
  store,
  i18n,
  render: h => h(App),
}).$mount('#app')
