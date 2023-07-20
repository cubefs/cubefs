import Vue from 'vue'
const base = () => process.env.NODE_ENV === 'production' ? window.location.origin : ''
export default {
  async get(url, params, opt, flag = false) {
    return Vue.prototype.$ajax.get(base() + url, params, opt)
  },
  async post(url, params, opt, flag = false) {
    return Vue.prototype.$ajax.post(base() + url, params, opt)
  },
  async delete(url, params, opt) {
    return Vue.prototype.$ajax.delete(base() + url, params, opt)
  },
  async put(url, params, opt) {
    return Vue.prototype.$ajax.put(base() + url, params, opt)
  },
}
