/**
 * @param Str
 * @constructor
 */
export function getTimeObj (time) {
  let obj = time !== null ? new Date(time) : new Date()
  return {
    year: obj.getFullYear(),
    month: obj.getMonth(),
    date: obj.getDate(),
    hour: obj.getHours(),
    minute: obj.getMinutes(),
    second: obj.getSeconds(),
    milli: obj.getMilliseconds()
  }
}
export function date2Str (time, split) {
  split = split || '-'
  let obj = Object.prototype.toString.call(time) === '[object Object]' ? time : getTimeObj(time)
  let m = obj.month + 1 < 10 ? '0' + (obj.month + 1) : (obj.month + 1)
  let d = obj.date < 10 ? '0' + obj.date : obj.date
  return obj.year + split + m + split + d
}
export function time2Str (time, split) {
  split = split || ':'
  let obj = Object.prototype.toString.call(time) === '[object Object]' ? time : getTimeObj(time)
  let h = obj.hour < 10 ? '0' + obj.hour : obj.hour
  let m = obj.minute < 10 ? '0' + obj.minute : obj.minute
  let s = obj.second < 10 ? '0' + obj.second : obj.second
  return h + split + m + split + s
}
export function resolvingDate (date) {
  let timeObj = getTimeObj(date)
  let times = date2Str(timeObj) + ' ' + time2Str(timeObj)
  return times
}
