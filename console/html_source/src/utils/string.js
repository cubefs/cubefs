/**
 * @param Str
 * @constructor
 */
export function stringFormat () {
  if (arguments.length === 0) { return null }
  let str = arguments[0]
  for (let i = 1; i < arguments.length; i++) {
    let re = new RegExp('\\{' + (i - 1) + '\\}', 'gm')
    str = str.replace(re, arguments[i])
  }
  return str
}
export function formatTime (val, isShowUnit) {
  if (val === 0) return 0 + ' ns'
  if (val > 60000000) {
    const unit = ['min', 'h']
    const k = 60000000
    const newVal = parseInt(val / k)
    const i = newVal > 60 ? 1 : 0
    return isShowUnit ? (newVal + ' ' + unit[i]) : newVal
  } else {
    const unit = ['ns', 'μs', 'ms', 's']
    const k = 1000
    const i = Math.floor(Math.log(val) / Math.log(k))
    const newVal = parseInt((val / Math.pow(k, i)))
    return isShowUnit ? (newVal + ' ' + unit[i]) : newVal
  }
}
const byteUnit = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
const biByte = ['iB', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
export function formatSize (bytes, decimals) {
  if (bytes === 0) return '0 Bytes'
  const unit = decimals === 2 ? biByte : byteUnit
  const k = decimals === 2 ? 1024 : 1000
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(3)) + ' ' + unit[i]
}

export function formatStatus(status){
  return status==1?"ReadOnly":status==2?"ReadWrite":"Unavailable"
}