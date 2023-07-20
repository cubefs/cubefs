import { getContentType } from './contenttype'
import SparkMD5 from 'spark-md5'

export function putPresignature(params) {
  return new Promise((resolve, reject) => {
    const { putSignUrl, env, zone, vol, prefix, file_name, content_type, part_num, user } = params
    if (!putSignUrl) {
      const err = new Error('params入参中缺少上传预签名url字段: putSignUrl ')
      reject(err)
      return
    }
    const options = {
      method: 'get',
    }
    request(`${putSignUrl}?&vol=${vol}&user=${user}&prefix=${prefix}&file_name=${file_name}&content_type=${content_type}` + (part_num !== 1 ? `&part_number=${part_num}` : ''), options)
      .then(res => resolve(res?.data), err => reject(err))
  })
}
export function transformMD5(fileName, filePart) {
  return new Promise((resolve, reject) => {
    const contenttype = getContentType(fileName)
    let reader = new FileReader()
    reader.readAsArrayBuffer(filePart)
    reader.addEventListener('error', e => {
      reject(e)
      reader.removeEventListener('error', () => { })
      reader.removeEventListener('load', () => { })
      reader = null
    })
    reader.addEventListener('load', async (e) => {
      let spark = new SparkMD5.ArrayBuffer()
      spark.append(e.target.result)
      let buf = Buffer.from(spark.end(), 'hex')
      const options = {
        method: 'put',
        body: reader.result,
        headers: {
          'Content-type': contenttype,
          // 'Content-Md5': buf.toString('base64')
        }
      }
      resolve(options)
      spark.destroy()
      spark = null
      buf = null
      reader.removeEventListener('error', () => { })
      reader.removeEventListener('load', () => { })
      reader = null
    })
  })
}

export async function upload(urls, fileList, xhrOptions, prefix, chunkSize) {
  return new Promise((resolve, reject) => {
    let partIndex = 0
    let errorCount = 0 // 分片错误重试一次
    const resData = []
    const max = fileList.length
    const contenttype = getContentType(fileList[0].name)
    const options = {
      method: 'put',
      body: fileList[partIndex],
      headers: {
        'Content-type': contenttype,
      },
      ...xhrOptions
    }
    function wrapRequestFn(resolve, reject) {
      request(urls[partIndex], options, fileList[partIndex].name, partIndex, max, chunkSize).then(res => {
        resData.push(res)
        partIndex++
        errorCount = 0
        if (partIndex < max) {
          wrapRequestFn(resolve, reject)
        } else {
          resolve(resData)
        }
      }, e => {
        errorCount++
        if (errorCount < 2 && e !== '手动取消') {
          wrapRequestFn(resolve, reject) // 重试
        } else {
          reject(e)
        }
        reject(e)
      }).catch(e => reject(e))
    }
    wrapRequestFn(resolve, reject)
  })
}

export function completemultiFun(params) {
  // 合并分片
  const options = {
    method: 'POST',
    body: JSON.stringify(params)
  }
  return request(params.completeUrl, options)
}

export function request(url, options, fileName, partIndex, max, chunkSize) {
  return new Promise((resolve, reject) => {
    // eslint-disable-next-line no-undef
    let xhr = window.XMLHttpRequest ? new XMLHttpRequest() : new ActiveXObject('Microsoft.XMLHTTP')
    xhr.open(options.method, url)

    if (options.onCreate) {
      options.onCreate(xhr)
    }
    if (options.headers) {
      Object.keys(options.headers).forEach(k =>
        xhr.setRequestHeader(k, options.headers[k])
      )
    }

    xhr.upload.addEventListener('progress', evt => {
      if (evt.lengthComputable && options.onProgress) {
        options.onProgress({ loaded: evt.loaded, chunkSize, fileName, partIndex, max })
      }
    })

    xhr.onreadystatechange = () => {
      const responseText = xhr.responseText
      if (xhr.readyState !== 4) {
        return
      }
      let reqId = null
      try {
        reqId = xhr.getResponseHeader('x-amz-request-id') || xhr.getResponseHeader('X-Reqid') || ''
      } catch (error) { }
      if (xhr.status !== 200 || (responseText && JSON.parse(responseText)?.code !== 200)) {
        let message = `xhr request failed, code: ${xhr.status};`
        if (responseText) {
          message = message + ` response: ${responseText}`
        }
        reject({ code: xhr.status, message: message, reqId: reqId, isRequestError: true })
      }
      resolve({ code: xhr.status, data: responseText ? JSON.parse(responseText) : 'OK', reqId: reqId, fileName })
    }

    xhr.send(options.body)
    xhr.onloadend = function () {
      xhr.upload.removeEventListener('progress', () => { })
      xhr = null
    }
    // 取消上传
    if (options.onAbort) {
      options.onAbort(xhr, reject)
    }
  })
}
export const getChunkSize = (size) => {
  if (size < 1024 * 1024 * 10) return 1024 * 1024 * 10
  if (size < 1024 * 1024 * 100) return 1024 * 1024 * 8
  return 1024 * 1024 * (Math.ceil(size / (1024 * 1024 * 100)) * 8)
}

export async function uploadSingleFile(url, file, xhrOptions, prefix) {
  return new Promise((resolve, reject) => {
    const contenttype = getContentType(file.file.name)
    let reader = new FileReader()
    reader.addEventListener('load', async (e) => {
      // let spark = new SparkMD5.ArrayBuffer()
      // spark.append(e.target.result)
      // let buf = Buffer.from(spark.end(), 'hex')
      const options = {
        method: 'put',
        body: reader.result,
        headers: {
          'Content-type': contenttype,
          // 'Content-Md5': buf.toString('base64')
        },
        ...xhrOptions
      }
      // spark.destroy()
      // spark = null
      // buf = null
      request(url, options, prefix + file.file.name, 0, file.file.size, file.file.size)
        .then(res => resolve(res), e => {
          reader.removeEventListener('load', () => { })
          reader = null
          reject(e)
        })
    })
    reader.readAsArrayBuffer(file)
  })
}