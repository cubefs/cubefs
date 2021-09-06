// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import router from './router'
import url from './utils/process'
import App from './App'
import Crumb from './components/crumb.vue'
import 'element-ui/lib/theme-chalk/index.css'
import './assets/css/reset.css'
import './assets/css/common.css'
import apollo from './utils/apollo'
import Vue2OrgTree from 'vue2-org-tree'
import i18n from './i18n'
const md5 = require('md5')
const sha1 = require('sha1')

Vue.use(Vue2OrgTree)
Vue.component('Crumb', Crumb)

Vue.config.productionTip = false
Vue.prototype.url = url // 页面引用 this.url
Vue.prototype.apollo = apollo // 页面引用 this.apollo
Vue.prototype.sha1 = sha1
Vue.prototype.md5 = md5

/* eslint-disable no-new */
new Vue({
  el: '#app',
  router,
  i18n,
  components: { App },
  template: '<App/>'
})
