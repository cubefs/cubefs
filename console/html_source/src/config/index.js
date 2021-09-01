export default {
  /**
   * @description 登录cookie
   */
  cookie: {
    path: '/', // cookie路径
    domain: '', // cookie域名
    key: '', // cookie关键字
    expires: 1 // token在Cookie中存储的天数，默认1天
  },
  /**
   * @description api请求基础路径
   */
  baseUrl: {
    dev: '',
    pro: '',
    test: ''
  },
  /**
   * @description 登录跳转路径
   */
  loginUrl: {
    dev: '',
    pro: '',
    test: ''
  }
}
