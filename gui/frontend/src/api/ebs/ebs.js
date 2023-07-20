import ajax from '../ajax'
const base = ({ region, clusterId }) => `/api/cubefs/console/blobstore/${region}/${clusterId}/`
const clusterPrefix = '/api/cubefs/console/blobstore/'
// 获取所有集群
// 获取集群概览
export const getClusterOview = ({ region }) => ajax.get(base({ region }) + 'stat', {}, { canShowErrorMessage: false })
// 获取集群
export const getClusterList = ({ region, ...params }) => ajax.get(clusterPrefix + `${region}/clusters/list`, params, { _ignoreMsg: true })

// -------- 节点管理 -----------------
// 列表
export const getNodeList = ({ region, clusterId, ...params }) => ajax.get(base({ region, clusterId }) + 'nodes/list', params)
// 下线
export const offLineNodeList = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'nodes/drop', params)
// 列表
export const changeRWNodeList = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'nodes/access', params)
// 节点服务下线
export const offlineService = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId, }) + 'nodes/offline', params)
export const nodeConfigReload = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'nodes/config/reload', params)

// 列表
// 节点详情
export const getNodeInfo = ({ region, clusterId, ...params }) => ajax.get(base({ region, clusterId }) + 'disks/list', params, { canShowErrorMessage: false })
// 节点切只读
export const changeNodeRW = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + '', params)
// 节点详情下线
export const offLineNodeInfo = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'disks/drop', params)
// 节点详情下线列表
export const offLineDropNodeList = ({ region, clusterId, ...params }) => ajax.get(base({ region, clusterId }) + 'disks/dropping/list', params, { canShowErrorMessage: false })
// 节点详情切只读
export const changeNodeInfoRW = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'disks/access', params)
// 节点详情设置坏盘
export const setNodeInfoBad = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'disks/set', params)
// 详情 注册
export const registerDisk = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'disks/probe', params)

// -------- 卷管理 -----------------
// 卷列表
// 节点列表
export const getVolList = ({ region, clusterId }) => {
  return (params) => ajax.get(base({ region, clusterId }) + 'volumes/list', params, { canShowErrorMessage: false })
}
// 正在写入的卷
export const getWrittingVolList = ({ region, clusterId }) => {
  return (params) => ajax.get(base({ region, clusterId }) + 'volumes/writing/list', params, { canShowErrorMessage: false })
}
// 节点id查询
export const getVolListById = ({ region, clusterId }) => {
  return (params) => ajax.get(base({ region, clusterId }) + 'volumes/get', params, { canShowErrorMessage: false })
}
// 节点状态查询
export const getVolListByStatus = ({ region, clusterId }) => {
  return (params) => ajax.get(base({ region, clusterId }) + 'volumes/v2/list', params, { canShowErrorMessage: false })
}

// -------- 后台任务 -----------------
// 列表
export const getBackTaskList = ({ region, clusterId, ...params }) => ajax.get(base({ region, clusterId }) + 'config/list', params)
// 开启/关闭
export const setBackTaskStatus = ({ region, clusterId, ...params }) => ajax.post(base({ region, clusterId }) + 'config/set', params)
