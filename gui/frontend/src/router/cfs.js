const RouterViewHoc = {
  name: 'RouterViewHoc',
  render(h) {
    return h('router-view')
  },
}

export const clusterDefaultRoutes = {
  path: 'cluster',
  name: 'cluster',
  meta: {
    title: '集群管理',
  },
  component: RouterViewHoc,
  children: [
    {
      path: 'list',
      name: 'clusterList',
      component: () => import('@/pages/cfs/clusterOverview'),
      meta: {
        title: '集群列表',
      },
    },
  ],
}

export const clusterDetailRoutes = {
  path: 'clusterDetail',
  name: 'clusterDetail',
  meta: {
    title: '集群详情',
  },
  component: RouterViewHoc,
  children: [],
}

export const clusterDetailChildren = [
  {
    path: 'clusterInfo',
    name: 'clusterInfo',
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo'),
    meta: {
      title: '集群概览',
    },
  },
  {
    path: 'volManage',
    name: 'volManage',
    meta: {
      title: '卷管理',
    },
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo/volumn/index.vue'),
  },
  {
    path: 'dataManage',
    name: 'dataManage',
    meta: {
      title: '数据管理',
    },
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo/dataManage/index.vue'),
  },
  {
    path: 'metaDataManage',
    name: 'metaDataManage',
    meta: {
      title: '元数据管理',
    },
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo/metaDataManage/metaPartition.vue'),
  },
  {
    path: 'resourceManage',
    name: 'resourceManage',
    meta: {
      title: '节点管理',
    },
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo/resourceManage/index.vue'),
  },
  {
    path: 'fileManage',
    name: 'fileManage',
    meta: {
      title: '文件管理',
      menuLinkGroup: true,
    },
    component: RouterViewHoc,
    children: [
      {
        path: '',
        name: 'fileManageList',
        meta: {
          title: '卷列表',
        },
        component: () => import('@/pages/cfs/fileManage/index'),
      },
      {
        path: 'fileList',
        name: 'fileList',
        meta: {
          title: '文件上传列表',
        },
        component: () => import('@/pages/cfs/fileManage/fileList/index'),
      },
    ],
  },
  {
    path: 'clusterEvent',
    name: 'clusterEvent',
    meta: {
      title: '集群事件',
    },
    component: () => import('@/pages/cfs/clusterOverview/clusterInfo/events/index.vue'),
  },
]

export default [
  clusterDefaultRoutes,
  clusterDetailRoutes,
]
