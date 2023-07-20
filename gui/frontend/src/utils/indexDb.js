export function openDB(dbName, storeName, version = 1) {
  return new Promise((resolve, reject) => {
    const indexedDB = window.indexedDB
    let db
    const request = indexedDB.open(dbName, version)
    request.onsuccess = function (event) {
      db = event.target.result // 数据库对象
      resolve(db)
    }

    request.onerror = function (event) {
      reject(event)
    }
    request.onupgradeneeded = function (event) {
      // 数据库创建或升级的时候会触发
      db = event.target.result // 数据库对象
      if (!db.objectStoreNames.contains(storeName)) {
        db.createObjectStore(storeName, { keyPath: 'id' }) // 创建表
        // objectStore.createIndex('name', 'name', { unique: true }) // 创建索引 可以让你搜索任意字段
      }
    }
  })
}

export function addData(db, storeName, data) {
  return new Promise((resolve, reject) => {
    const request = db
      .transaction([storeName], 'readwrite') // 事务对象 指定表格名称和操作模式（"只读"或"读写"）
      .objectStore(storeName) // 仓库对象
      .add(data)
    request.onsuccess = function (event) {
      resolve(event)
    }
    request.onerror = function (event) {
      reject(event)
    }
  })
}
export function getDataByKey(db, storeName, key) {
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([storeName]) // 事务
    const objectStore = transaction.objectStore(storeName) // 仓库对象
    const request = objectStore.get(key)

    request.onerror = function (event) {
      reject(event)
    }

    request.onsuccess = function (event) {
      resolve(request.result)
    }
  })
}

export function deleteDB(db, storeName, id) {
  const request = db
    .transaction([storeName], 'readwrite')
    .objectStore(storeName)
    .delete(id)

  return new Promise((resolve, reject) => {
    request.onsuccess = function (ev) {
      resolve(ev)
    }

    request.onerror = function (ev) {
      resolve(ev)
    }
  })
}

export function closeDB(db) {
  db.close()
}
