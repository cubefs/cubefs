import moment from 'moment'
import { Parser } from 'json2csv'
// 公共函数
export const debounce = (fn, ms = 0) => {
  let timeoutId
  return function (...args) {
    clearTimeout(timeoutId)
    timeoutId = setTimeout(() => fn.apply(this, args), ms)
  }
}

export const throttle = (fn, wait) => {
  let updateTime = Date.now()
  return (...args) => {
    const now = Date.now()
    if (now - updateTime > wait) {
      fn.apply(this, args)
      updateTime = now
    }
  }
}
export const fmapStatus = (map, value) => {
  if (map[value]) {
    return map[value]
  } else {
    const keys = Object.keys(map)
    for (let i = 0; i < keys.length; i++) {
      if (map[keys[i]] === value) {
        return keys[i]
      }
    }
  }
}
export const approvalStatusMap = (value) => {
  return fmapStatus({
    NotApproved: '未审批',
    Pass: '已通过',
    NotPass: '审批未通过',
  }, value)
}
export const formatDate = (date1 = new Date(), fmt = 'yyyy-MM-dd hh:mm:ss') => {
  const date = new Date(date1)
  const o = {
    'M+': date.getMonth() + 1, // 月份
    'd+': date.getDate(), // 日
    'h+': date.getHours(), // 小时
    'm+': date.getMinutes(), // 分
    's+': date.getSeconds(), // 秒
    'q+': Math.floor((date.getMonth() + 3) / 3), // 季度
    S: date.getMilliseconds(), // 毫秒
  }
  if (/(y+)/.test(fmt)) {
    fmt = fmt.replace(RegExp.$1, (date.getFullYear() + '').substr(4 - RegExp.$1.length))
  }
  for (const k in o) {
    if (new RegExp('(' + k + ')').test(fmt)) {
      fmt = fmt.replace(RegExp.$1, (RegExp.$1.length == 1) ? (o[k]) : (('00' + o[k]).substr(('' + o[k]).length)))
    }
  }
  return fmt
}
export const renderSizeFile = (value) => {
  if (value === 0 || !value || value === '0' || value < 0) return '0 Bytes'
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  let c = Math.floor(Math.log(value) / Math.log(k))
  if (c <= -1) c = 0
  value =
    value / Math.pow(k, c) === 1024
      ? 1 + sizes[c + 1]
      : (value / Math.pow(k, c)).toFixed(2) + sizes[c]
  return value
}
export const toByte = (value, unit) => {
  const obj = {
    B: Math.pow(1024, 0),
    KB: Math.pow(1024, 1),
    MB: Math.pow(1024, 2),
    GB: Math.pow(1024, 3),
    TB: Math.pow(1024, 4),
    PB: Math.pow(1024, 5),
    EB: Math.pow(1024, 6),
    ZB: Math.pow(1024, 7),
    YB: Math.pow(1024, 8),
  }
  return value * obj[unit]
}
export const renderSize = (value, fixed = 2) => {
  if (value === 0 || !value || value === '0') return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  let c = Math.floor(Math.log(value) / Math.log(k))
  if (c <= -1) c = 0
  value =
    value / Math.pow(k, c) === 1024
      ? 1 + sizes[c + 1]
      : (value / Math.pow(k, c)).toFixed(fixed) + sizes[c]
  return value
}
export const getSizeAndUnit = (value) => {
  const size = parseFloat(value)
  const unit = value.replace(size + '', '')
  return {
    size,
    unit,
  }
}
export const readablizeBytes = (value) => {
  let flag = ''
  if (value < 0) {
    value = Math.abs(value)
    flag = '-'
  }
  // 换算流量的
  if (value === 0 || !value || value === '0') return '0 Bytes'
  let newVal = Number(value)
  const k = 1024
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  let c = Math.floor(Math.log(newVal) / Math.log(k))
  if (c <= -1) c = 0
  newVal =
    newVal / Math.pow(k, c) === 1024
      ? 1 + sizes[c + 1]
      : (newVal / Math.pow(k, c)).toFixed(2) + sizes[c]
  return flag + newVal
}
// 默认最小以GB为单位进行转换
export const getGBSize = (value = 1, unit = 'GB') => {
  const sizes = ['GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const index = sizes.indexOf(unit)
  return value * Math.pow(1024, index)
}
// 默认最小以GB为单位进行转换(原来为B)
export const getGBSize_B = (value = 1, unit = 'GB') => {
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const index = sizes.indexOf(unit)
  return (value / Math.pow(1024, index))
}
// 带单位排序
export const sortSizeWithUnit = (a, b) => {
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
  const aSize = parseInt(a)
  const aUnit = sizes.indexOf(a.split(aSize)[1] || 'GB')
  const bSize = parseInt(b)
  const bUnit = sizes.indexOf(b.split(bSize)[1] || 'GB')
  if (aUnit < bUnit) return -1
  if (aUnit > bUnit) return 1
  if (aUnit === bUnit) {
    if (aSize < bSize) return -1
    if (aSize > bSize) return 1
    if (aSize === bSize) return 0
  }
  return 0
}
const getBlob = (url) => {
  return new Promise((resolve) => {
    const xhr = new XMLHttpRequest()

    xhr.open('GET', url, true)
    xhr.responseType = 'blob'
    xhr.onload = () => {
      if (xhr.status === 200) {
        resolve(xhr.response)
      }
    }

    xhr.send()
  })
}

const saveAs = (blob, filename) => {
  if (window.navigator.msSaveOrOpenBlob) {
    navigator.msSaveBlob(blob, filename)
  } else {
    const link = document.createElement('a')
    const body = document.querySelector('body')

    link.href = window.URL.createObjectURL(blob) // 创建对象url
    link.download = filename

    // fix Firefox
    link.style.display = 'none'
    body.appendChild(link)

    link.click()
    body.removeChild(link)

    window.URL.revokeObjectURL(link.href) // 通过调用 URL.createObjectURL() 创建的 URL 对象
  }
}

export const download = (url, filename = '') => {
  if (
    /\.(png|jpg|gif|jpeg|webp|bmp|pdf|txt|doc|docx|xls|xlsx|mp3|wma|rm|wav|mid|mpg|mpeg|avi|rmvb|mov|wmv|asf|dat|mpe)$/.test(filename)
  ) {
    getBlob(url).then((blob) => {
      saveAs(blob, filename)
    })
  } else {
    window.location.href = url
  }
}
export function objSort(item1, item2) {
  // "use strict";
  const props = []
  for (let _i = 2; _i < arguments.length; _i++) {
    props[_i - 2] = arguments[_i]
  }
  const cps = [] // 存储排序属性比较结果。
  // 如果未指定排序属性，则按照全属性升序排序。
  let asc = true
  if (props.length < 1) {
    for (const p in item1) {
      if (item1[p] > item2[p]) {
        cps.push(1)
        break // 大于时跳出循环。
      } else if (item1[p] === item2[p]) {
        cps.push(0)
      } else {
        cps.push(-1)
        break // 小于时跳出循环。
      }
    }
  } else {
    for (let i = 0; i < props.length; i++) {
      const prop = props[i]
      for (const o in prop) {
        asc = prop[o] === 'ascending' ? 1 : -1
        if (typeof item1[o] === 'string') {
          try {
            if (item1[o].localeCompare(item2[o], 'zh-CN', { numeric: true }) > 0) {
              return asc
            } else if (item1[o].localeCompare(item2[o], 'zh-CN', { numeric: true }) < 0) {
              return -asc
            }
          } catch (error) {
            if (item1[o] > item2[o]) {
              return asc
            } else if (item1[o] < item2[o]) {
              return -asc
            }
          }
        } else {
          if (item1[o] > item2[o]) {
            return asc
          } else if (item1[o] < item2[o]) {
            return -asc
          }
        }
      }
    }
    return 0
  }
  for (let j = 0; j < cps.length; j++) {
    if (cps[j] === 1 || cps[j] === -1) {
      return cps[j]
    }
  }

  return false
};
export const getType = (val) => {
  return Object.prototype.toString.call(val).slice(8, -1).toLowerCase()
}
export const parseTime = (time, type) => {
  if (type === 1) {
    return moment(time).format('YYYY-MM-DD')
  }
  return moment(time).format('YYYY-MM-DD HH:mm:ss')
}
// 判断是否IE浏览器
export const MyBrowserIsIE = () => {
  let isIE = false
  if (
    navigator.userAgent.indexOf('compatible') > -1 &&
    navigator.userAgent.indexOf('MSIE') > -1
  ) {
    // ie浏览器
    isIE = true
  }
  if (navigator.userAgent.indexOf('Trident') > -1) {
    // edge 浏览器
    isIE = true
  }
  return isIE
}
export const generateEXCEL = (tableProp, tableDatas, search, title) => {
  try {
    // 表头别名
    const fields = [...Object.values(tableProp)]
    // 导出需要导出所有的数据
    // eslint-disable-next-line camelcase
    const tableData = tableDatas
    const json2csv = new Parser({
      fields,
      excelStrings: true,
      // delimiter: '\t',
      quote: '',
    })
    const result = json2csv.parse(tableData)
    // 文件命名
    // eslint-disable-next-line camelcase
    const { region, state } = search || {}

    const fileName = search.state ? `${region}-${state}-${title}` : `${region}-${title}`
    if (MyBrowserIsIE()) {
      // IE10以及Edge浏览器
      const BOM = '\uFEFF'
      // 文件转Blob格式
      const csvData = new Blob([BOM + result], { type: 'text/csv' })
      navigator.msSaveBlob(csvData, `${fileName}.csv`)
    } else {
      const csvContent = 'data:text/csv;charset=utf-8,\uFEFF' + result
      // 非ie 浏览器
      createDownLoadClick(csvContent, `${fileName}.csv`)
    }
  } catch (err) {
    alert(err)
  }
}
export const createDownLoadClick = (content, fileName) => {
  const link = document.createElement('a')
  if (String.prototype.hasOwnProperty('replaceAll')) {
    link.href = encodeURI(content).replaceAll('#', '%23') // 不对#转义,存在数据被截断的情况
  } else {
    link.href = encodeURI(content).replace(/#/g, '%23') // 不对#转义,存在数据被截断的情况
  }
  link.download = fileName
  document.body.appendChild(link)
  link.click()
  document.body.removeChild(link)
}
export const dealStrArray = (str) => {
  if (!str) return []
  const temp = str.slice(1, str.length - 1)
  return temp.split(' ').reverse()
}
/**
 * 检查两个对象是否值相等
 * @param {*} a 对象a
 * @param {*} b 对象b
 */
export function looseEqual(a, b) {
  if (a === b) return true
  const isObjectA = typeof a === 'object'
  const isObjectB = typeof b === 'object'
  if (isObjectA && isObjectB) {
    try {
      const isArrayA = Array.isArray(a)
      const isArrayB = Array.isArray(b)
      if (isArrayA && isArrayB) {
        return (
          a.length === b.length &&
          a.every((e, i) => {
            return looseEqual(e, b[i])
          })
        )
      } else if (a instanceof Date && b instanceof Date) {
        return a.getTime() === b.getTime()
      } else if (!isArrayA && !isArrayB) {
        const keysA = Object.keys(a)
        const keysB = Object.keys(b)
        return (
          keysA.length === keysB.length &&
          keysA.every(key => {
            return looseEqual(a[key], b[key])
          })
        )
      } else {
        return false
      }
    } catch (e) {
      return false
    }
  } else if (!isObjectA && !isObjectB) {
    return String(a) === String(b)
  } else {
    return false
  }
}

export function uuid () {
  const s = []
  const hexDigits = '0123456789abcdef'
  for (let i = 0; i < 36; i++) {
    s[i] = hexDigits.substr(Math.floor(Math.random() * 0x10), 1)
  }
  // bits 12-15 of the time_hi_and_version field to 0010
  s[14] = '4'
  // bits 6-7 of the clock_seq_hi_and_reserved to 01
  s[19] = hexDigits.substr((s[19] & 0x3) | 0x8, 1)
  s[8] = s[13] = s[18] = s[23] = '-'
  const uuid = s.join('')
  return uuid
}
/**
 * 选择性的保留某些属性
 * @param {*} obj 源对象
 * @param {*} fn 迭代器
 */
export function pickBy(obj, fn) {
  const res = {}
  Object.entries(obj).forEach(item => {
    const [k, v] = item
    if (fn(item)) {
      res[k] = v
    }
  })
  return res
}

const class2type = {}

// 生成class2type映射
'Boolean Number String Function Array Date RegExp Object Error'
  .split(' ')
  .forEach(function(item, index) {
    class2type['[object ' + item + ']'] = item.toLowerCase()
  })
/**
 * 判断类型
 * @param {any} obj
 */
export function type(obj) {
  if (obj == null) {
    return obj + ''
  }
  return typeof obj === 'object' || typeof obj === 'function'
    ? class2type[Object.prototype.toString.call(obj)] || 'object'
    : typeof obj
}
/**
 * 递归遍历集合对象并执行回调函数
 * @param {array} item 数据源
 * @param {string} childrenKey 子项的key
 * @param {function} fn 回调函数
 */
export function traversBy(item, childrenKey = 'children', fn) {
  if (typeof item !== 'object') return
  if (type(item) === 'object') {
    fn(item)
    if (item[childrenKey]) {
      traversBy(item[childrenKey], childrenKey, fn)
    }
  } else if (type(item) === 'array') {
    item.forEach(v => traversBy(v, childrenKey, fn))
  }
}

/**
 * 对象转集合
 * @param {*} map
 * @param {*} key
 * @param {*} valueKey
 */
export function mapToCollection(map, key = 'key', valueKey = 'value') {
  if (typeof map !== 'object') return []
  return Object.entries(map).map(([k, v]) => ({
    [key]: k,
    [valueKey]: v,
  }))
}

/**
 * 集合转对象
 * @param {*} collection
 * @param {*} key
 * @param {*} valueKey
 */
export function collectionToMap(collection, key = 'key', valueKey = 'value') {
  const map = {}
  if (collection.forEach) {
    collection.forEach(v => {
      const _key = v[key]
      if (_key) {
        map[_key] = v[valueKey]
      }
    })
  }
  return map
}

export const codeMap = {
  1: 'EC15P12',
  2: 'EC6P6',
  3: 'EC16P20L2',
  4: 'EC6P10L2',
  5: 'EC6P3L3',
  6: 'EC6P6Align0',
  7: 'EC6P6Align512',
  8: 'EC4P4L2',
  9: 'EC12P4',
  10: 'EC16P4',
  11: 'EC3P3',
  12: 'EC10P4',
  13: 'EC6P3',
  14: 'EC12P9',
}
