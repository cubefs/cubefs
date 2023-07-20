import { Message } from 'element-ui'

function checkValue(arr, key, value) {
  for (let i = 0; i < arr.length; i++) {
    if (arr[i][key] === value) {
      return true
    }
  }
  return false
}

export function authMixin (Vue, { router, store }) {
  router.beforeEach((to, from, next) => {
    if (to.name === 'loginPage') {
      next()
    } else if (!localStorage.getItem('userInfo')) {
      next({ name: 'loginPage' })
    } else {
      const { allAuth } = store.state.moduleUser
      if (!allAuth) {
        store.dispatch('moduleUser/setAuth').then(() => {
          next()
        }).catch(() => {
          next({ name: 'loginPage' })
        })
      } else {
        next()
      }
    }
  })
  Vue.directive('auth', {
    inserted: (el, binding) => {
      const { allAuth } = store.state.moduleUser
      const frontEndAuth = allAuth?.filter(item => item.is_check)
      if (!checkValue(frontEndAuth, 'auth_code', binding.value)) {
        el.parentNode.removeChild(el)
      }
    },
  })
}

export const codeList = [
  {
    title: '集群管理',
    children: ['CLUSTER_CREATE', 'CLUSTER_UPDATE'],
  },
  {
    title: '卷管理',
    children: ['CFS_VOLS_CREATE', 'CFS_VOLS_UPDATE', 'CFS_VOLS_EXPAND', 'CFS_VOLS_SHRINK'],
  },
  {
    title: '多副本节点',
    children: ['CFS_DATANODE_DECOMMISSION', 'CFS_DATANODE_MIGRATE', 'CFS_DISKS_DECOMMISSION', 'CFS_DATAPARTITION_DECOMMISSION'],
  },
  {
    title: '纠删码节点',
    children: ['BLOBSTORE_NODES_ACCESS', 'BLOBSTORE_DISKS_ACCESS', 'BLOBSTORE_DISKS_SET', 'BLOBSTORE_DISKS_PROBE'],
  },
  {
    title: '元数据节点',
    children: ['CFS_METANODE_DECOMMISSION', 'CFS_METANODE_MIGRATE', 'CFS_METAPARTITION_DECOMMISSION'],
  },
  {
    title: '文件管理',
    children: ['CFS_S3_DIRS_CREATE', 'CFS_S3_FILES_DOWNLOAD_SIGNEDURL', 'CFS_S3_FILES_UPLOAD_SIGNEDURL'],
  },
  {
    title: '用户管理',
    children: ['AUTH_USER_UPDATE', 'AUTH_USER_DELETE', 'AUTH_USER_PASSWORD_UPDATE'],
  },
  {
    title: '角色管理',
    children: ['AUTH_ROLE_CREATE', 'AUTH_ROLE_UPDATE', 'AUTH_ROLE_DELETE'],
  },
]

export const backendAuthids = [3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 22, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 41, 42, 43, 44, 45, 48, 49, 50, 53, 54, 55, 57, 58, 59, 60, 62, 63, 64, 66, 67, 70, 71, 91, 92, 93, 94]
