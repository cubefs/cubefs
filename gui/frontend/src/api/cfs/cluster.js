import Ajax from '../ajax'

const prefix = ({ cluster_name: clusterName }) => `/api/cubefs/console/cfs/${clusterName}/`
const clusterPrefix = '/api/cubefs/console/'
// -----------  集群相关  --------------

export const getRegions = (param) => {
  return Ajax.get('/api/cubefs/regions', param)
}
// 上架集群
export const createCluster = (param) => {
  return Ajax.post(clusterPrefix + 'clusters/create', param)
}
// 修改集群
export const upDateCluster = (param) => {
  return Ajax.put(clusterPrefix + 'clusters/update', param)
}
// 集群列表
export const getClusterList = (param) => {
  return Ajax.get(clusterPrefix + 'clusters/list', param)
}
// 获取集群是否可以设置故障域
export const getClusterIsErrArea = (param) => {
  return Ajax.get(prefix(param) + 'domains/info', param)
}

// -----------  卷模块  --------------
// 卷列表
export const getVolList = (param) => {
  return Ajax.get(prefix(param) + 'vols/list', param)
}
// 创建卷
export const createVol = (param) => {
  return Ajax.post(prefix(param) + 'vols/create', param)
}
// 扩容
export const expandVol = (param) => {
  return Ajax.put(prefix(param) + 'vols/expand', param)
}
// 缩容
export const shrinkVol = (param) => {
  return Ajax.put(prefix(param) + 'vols/shrink', param)
}
// 获取卷详情
export const getVolDetail = (param) => {
  return Ajax.get(prefix(param) + 'vols/info', param)
}
export const updateVol = (param) => {
  return Ajax.put(prefix(param) + 'vols/update', param)
}

// -----------  用户  --------------
// 创建用户
export const createUser = (param) => {
  return Ajax.post(prefix(param) + 'users/create', param)
}
// 用户列表
export const getUserList = (param) => {
  return Ajax.get(prefix(param) + 'users/list', param)
}
// 用户授权
export const updateUserPolicy = (param) => {
  return Ajax.post(prefix(param) + 'users/policies', param)
}
// 获取用户名称列表
export const getUserNameList = (param) => {
  return Ajax.get(prefix(param) + 'users/names', param)
}

// -----------  元数据节点  --------------
// 元数据节点信息
export const getMetaNodeList = (param) => {
  return Ajax.get(prefix(param) + 'metaNode/list', param)
}
// 元数据节点下线
export const offLineMetaNodes = (param) => {
  return Ajax.post(prefix(param) + 'metaNode/decommission', param)
}
// 元数据节点分区详情
export const getMetaNodeInfoList = (param) => {
  return Ajax.get(prefix(param) + 'metaNode/partitions', param)
}
// 元数据节点分区下线
export const offLineMetaNodePartitions = (param) => {
  return Ajax.post(prefix(param) + 'metaPartition/decommission', param)
}
// 原数据节点迁移
export const migrateMetaNode = (param) => {
  return Ajax.post(prefix(param) + 'metaNode/migrate', param)
}

// -----------  数据节点  --------------
export const getBadDataP = (param) => {
  return Ajax.get(prefix(param) + 'dataPartition/diagnosis')
}
export const getBadMetaP = (param) => {
  return Ajax.get(prefix(param) + 'metaPartition/diagnosis')
}
// 数据节点信息
export const getDataNodeList = (param) => {
  return Ajax.get(prefix(param) + 'dataNode/list', param)
}
// 数据节点下线
export const offLineDataNodes = (param) => {
  return Ajax.post(prefix(param) + 'dataNode/decommission', param)
}
// 数据节点迁移
export const migrateDataNode = (param) => {
  return Ajax.post(prefix(param) + 'dataNode/migrate', param)
}
// 数据节点磁盘信息
export const getDataNodeDiskList = (param) => {
  return Ajax.get(prefix(param) + 'disks/list', param)
}
// 数据节点磁盘下线
export const offLineDisks = (param) => {
  return Ajax.post(prefix(param) + 'disks/decommission', param)
}
// 数据节点分区
export const getDataNodePartitionList = (param) => {
  return Ajax.get(prefix(param) + 'dataNode/partitions', param)
}
// 数据节点分区下线
export const offLineDataNodePartitions = (param) => {
  return Ajax.post(prefix(param) + 'dataPartition/decommission', param)
}

// -----------  元数据分区  --------------
// 元数据分区信息
export const getMetaPartitionList = (param) => {
  return Ajax.get(prefix(param) + 'metaPartition/list', param)
}
// load
export const loadMetaPartition = (param) => {
  return Ajax.get(prefix(param) + 'metaPartition/load', param)
}
// 创建
export const createMetaPartition = (param) => {
  return Ajax.post(prefix(param) + 'metaPartition/create', param)
}

// -----------  数据分区  --------------
// 数据分区信息
export const getDataPartitionList = (param) => {
  return Ajax.get(prefix(param) + 'dataPartition/list', param)
}
// load
export const loadDataPartition = (param) => {
  return Ajax.get(prefix(param) + 'dataPartition/load', param)
}
// 创建
export const createDataPartition = (param) => {
  return Ajax.post(prefix(param) + 'dataPartition/create', param)
}

// -------------- 故障域 --------------
// 创建
export const getDomainInfoList = (param) => {
  return Ajax.get(prefix(param) + 'get/DomainInfo', param)
}
// 扩容
export const expansionDomain = (param, type) => {
  return Ajax.get(prefix(param) + 'add/' + type, param)
}

// 文件管理
export const getFileList = ({ cluster_name: clusterName, ...param }) => {
  return Ajax.get(prefix({ cluster_name: clusterName }) + 's3/files/list', param)
}

export const getDownloadSignedUrl = (param) => {
  return Ajax.get(prefix(param) + 's3/files/download/signedUrl', param)
}

export const getUploadSignedUrl = (param) => {
  return prefix(param) + 's3/files/upload/signedUrl'
}

export const getMultipartUploadSignedUrl = (param) => {
  return prefix(param) + 's3/files/upload/multipart/signedUrl'
}

export const multipartUploadComplete = (param) => {
  return prefix(param) + 's3/files/upload/multipart/complete'
}

export const dirsCreate = (param) => {
  return Ajax.post(prefix(param) + 's3/dirs/create', param)
}

export const getCors = (param) => {
  return Ajax.get(prefix(param) + 's3/vols/cors/get', param)
}
export const setCors = (param) => {
  return Ajax.put(prefix(param) + 's3/vols/cors/set', param)
}
export const deleteCors = (param) => {
  return Ajax.delete(prefix(param) + 's3/vols/cors/delete', param)
}
