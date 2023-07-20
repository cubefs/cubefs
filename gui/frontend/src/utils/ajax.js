import axios from 'axios'
import { uuid } from '@/utils'
import { Message, Loading } from 'element-ui'

let loadInstance = null

export function ajaxMixin (Vue, { router }) {
  axios.defaults.validateStatus = function (status) {
    return status >= 200 && status <= 204 // default
  }
  axios.defaults.withCredentials = true
  /**
   * 请求模块
   * @param {string} url 请求路径
   * @param {Object} data 请求参数对象
   * @param {Object} ext ext的其他参数，提供给axios的配置参数 https://github.com/axios/axios
   * @param {boolean} ext._ignoreMsg 是否忽略错误弹窗，改为自行捕获
   * @param {boolean} ext._loading 是否在请求时使用loading
   * @param {string} ext._type 把post put patch请求的数据包装成什么格式，默认json
   */
  const request = function (type) {
    return async function (url, data = {}, ext = {}) {
      const {
        _ignoreMsg = false,
        _type = 'json',
        _loading = true,
        _beforeSendHook,
        ...axiosOptions
      } = ext
      const config = {}
      // 生成Traceid供发送请求以及抛出错误时使用
      const Traceid = uuid().replace(/-/g, '')

      Object.assign(config, axiosOptions)
      config.method = type
      config.headers = config.headers || {}
      config.headers.Traceid = Traceid
      config.url = url
      if (['put', 'post', 'patch'].includes(type)) {
        if (_type === 'form' && !(data instanceof FormData)) {
          const formData = new FormData()
          for (const [k, v] of Object.entries(data)) {
            formData.append(k, v)
          }
          config.data = formData
        } else {
          config.data = data
        }
      } else {
        config.params = data
      }
      config.params = {
        ...config.params,
      }
      if (_beforeSendHook) {
        _beforeSendHook(config.params)
      }
      try {
        if (_loading) {
          loadInstance = Loading.service({
            lock: true,
            text: '正在加载...',
            spinner: 'el-icon-lock',
            background: 'rgba(0, 0, 0, 0.3)',
          })
        }
        const res = await axios(config)
        loadInstance.close()
        const { status } = res
        if (status !== 200) {
          throw res.data
        }

        const { code } = res.data
        if (code !== 200) {
          throw res.data
        }
        return res.data
      } catch (err) {
        loadInstance.close()
        let error = err
        if (typeof error === 'string') error = { msg: error }
        if (error.code === 'ECONNABORTED') error.msg = '请求超时'
        if (error.code === 401) {
          Message.error('登录已过期，请重新登陆!')
          router.push({ name: 'loginPage' })
        } else {
          if (!url.includes('/api/cubefs/console/auth/user/permission')) {
            error.Traceid = Traceid
            if (!_ignoreMsg) {
              this.$message.error(
                error.msg || '系统错误',
              )
            }
          }
        }
        return Promise.reject(error)
      } finally {}
    }
  }

  const prototype = Vue.prototype
  prototype.$ajax = {}
  ;['get', 'post', 'put', 'patch', 'delete'].forEach(v => {
    prototype.$ajax[v] = request(v).bind(prototype)
  })
}
