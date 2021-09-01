import axios from 'axios'

const axiosInstance = axios.create({
  timeout: 1000 * 60 * 5
})

axiosInstance.interceptors.response.use(response => {
  // 接口数据返回之后response拦截
  if (response.status !== 200) {
    let message = response.toString() || response.status + ':请求失败'
    return Promise.reject(message)
  }
  if (response.errors && response.errors !== null) {
    let rst = JSON.parse(response.errors)
    if (rst && (rst.code === 401 || rst.code === 407)) {
      sessionStorage.setItem('returnUrl', window.location.href)
      window.location.href = window.location.origin + '/login'
    }
  }
  return response.data
}, err => {
  return Promise.reject(err)
})

export default {
  get (url, params, config) {
    const options = Object.assign({}, config, {
      method: 'get',
      url,
      params
      // timeout: 10000,
    })
    return axiosInstance(options).then(response => {
      return response
    }).catch(error => {
      return Promise.reject(error)
    })
  },
  post (url, params, data, config) {
    const options = Object.assign({}, config, {
      method: 'post',
      url,
      params,
      data
    })
    return axiosInstance(options).then(response => {
      return response
    }).catch(error => {
      return Promise.reject(error)
    })
  },
  put (url, params, data, config) {
    const options = Object.assign({}, config, {
      method: 'put',
      url,
      params,
      data
    })
    return axiosInstance(options).then(response => {
      return response
    }).catch(error => {
      return Promise.reject(error)
    })
  },
  delete (url, params, data, config) {
    const options = Object.assign({}, config, {
      method: 'delete',
      url,
      params,
      data
    })
    return axiosInstance(options).then(response => {
      return response
    }).catch(error => {
      return Promise.reject(error)
    })
  },
  all (...array) {
    return Promise.all(array).then(resList => {
      return resList
    }).catch(error => {
      return Promise.reject(error)
    })
  }
}
