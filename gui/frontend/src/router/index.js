import Vue from 'vue'
import Router from 'vue-router'
import cfsRoutes, { clusterDetailChildren } from './cfs'
import ErrorPage from '@/components/common/ErrorPage'
import store from '@/store/index'
const namespace = require('../../package.json').namespace

const originalPush = Router.prototype.push
Router.prototype.push = function push(location) {
  return originalPush.call(this, location).catch(err => err)
}
const originalReplace = Router.prototype.replace
Router.prototype.replace = function replace(location) {
  return originalReplace.call(this, location).catch(err => err)
}

Vue.use(Router)

const RouterViewHoc = {
  name: 'RouterViewHoc',
  render(h) {
    return h('router-view')
  },
}

const router = new Router({
  mode: 'history',
  base: '',
  scrollBehavior(to, from, savedPosition) {
    if (savedPosition) {
      return savedPosition
    } else {
      return { x: 0, y: 0 }
    }
  },
  routes: [
    {
      path: '/',
      redirect: `/${namespace}/`,
    },
    {
      path: `/${namespace}/`,
      children: [
        ...cfsRoutes,
        {
          path: 'authManage',
          name: 'authManage',
          meta: {
            title: '权限管理',
          },
          component: RouterViewHoc,
          children: [
            {
              path: 'userManage',
              name: 'userManage',
              meta: {
                title: '用户管理',
              },
              component: () => import('@/pages/authManage/user/index'),
            },
            {
              path: 'roleManage',
              name: 'roleManage',
              meta: {
                title: '角色管理',
              },
              component: () => import('@/pages/authManage/role/index'),
            },
          ],
        },
      ],
      component: RouterViewHoc,
      redirect: `/${namespace}/cluster/list`,
      meta: {
        penetrate: true,
      },
    },
    {
      path: `/${namespace}/login`,
      name: 'loginPage',
      component: () => import('@/pages/login'),
      meta: {
        layout: 'Noop',
        menuIgnore: true,
      },
    },
    {
      path: '*',
      name: 'errorPage',
      component: ErrorPage,
      meta: {
        layout: 'Noop',
        menuIgnore: true,
      },
    },
  ],
})
export const initMenu = () => {
  store.commit('setRoutes', router.options.routes)
  initCfsClusterRoute()
}

export const initCfsClusterRoute = () => {
  if (store.getters['clusterInfoModule/clusterInfog']) {
    router.options.routes[1].children[1].children = clusterDetailChildren
    router.addRoutes(router.options.routes)
    store.commit('setRoutes', router.options.routes)
  }
}

export default router
