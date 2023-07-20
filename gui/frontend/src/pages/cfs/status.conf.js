export const nodeStatusMap = {
  normal: 1,
  broken: 2,
  repairing: 3,
  repaired: 4,
  dropped: 5,
}
export const nodeStatusList = Object.entries(nodeStatusMap).map(([key, value]) => {
  return {
    label: key,
    value: value,
  }
})
export const volStatusMap = {
  idle: 1,
  active: 2,
  lock: 3,
  unlocking: 4,
}
export const volStatusList = Object.entries(volStatusMap).map(([key, value]) => {
  return {
    label: key,
    value: value,
  }
})

export const orderTypeList = [
  {
    label: '磁盘下线',
    value: 'DISK_DROP',
  },
  {
    label: '设置坏盘',
    value: 'DISK_SET',
  },
  {
    label: '节点下线',
    value: 'NODE_DROP',
  },
  {
    label: '服务下线',
    value: 'NODE_SERVER_OFFLINE',
  },
]
export const orderStatusList = [
  {
    label: '全部',
    value: '',
  },
  {
    label: '同意',
    value: 'AGREE',
  },
  {
    label: '创建',
    value: 'CREATE',
  },
  {
    label: '拒绝',
    value: 'REFUSE',
  },
  {
    label: '撤销',
    value: 'REVOKE',
  },
]
