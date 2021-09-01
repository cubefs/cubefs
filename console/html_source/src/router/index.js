import Vue from 'vue'
import Router from 'vue-router'

Vue.use(Router)

const router = new Router({
  scrollBehavior (to, from, savedPosition) {
    return {x: 0, y: 0}
  },
  linkActiveClass: 'is-active',
  mode: 'history',
  routes: [
    {
      path: '/',
      redirect: '/overview'
    },
    {
      path: '/overview',
      name: 'overview',
      component: () => import('@/views/overview/overview.vue')
    },
    {
      path: '/login',
      name: 'login',
      component: () => import('@/views/login.vue')
    },
    {
      path: '/userDetails',
      name: 'userDetails',
      component: () => import('@/views/overview/userDetails.vue')
    },
    {
      path: '/servers',
      name: 'servers',
      component: () => import('@/views/servers/servers.vue')
    },
    {
      path: '/serverList',
      name: 'serverList',
      meta: {
        crumbName: 'chubaoFS.crumb.Server',
        crumbPath: ['servers']
      },
      component: () => import('@/views/servers/serverList.vue')
    },
    {
      path: '/serverDetail',
      name: 'serverDetail',
      meta: {
        crumbName: 'chubaoFS.crumb.ServerList',
        crumbPath: ['server', 'serverList']
      },
      component: () => import('@/views/servers/serverDetail.vue')
    },
    {
      path: '/dashboard',
      name: 'dashboard',
      component: () => import('@/views/dashboard/dashboard.vue')
    },
    {
      path: '/volumeList',
      name: 'volumeList',
      component: () => import('@/views/volume/volumeList.vue')
    },
    {
      path: '/volumeDetail',
      name: 'volumeDetail',
      meta: {
        crumbName: 'chubaoFS.crumb.VolumeList',
        crumbPath: ['volumeList']
      },
      component: () => import('@/views/volume/volumeDetail.vue')
    },
    {
      path: '/operations',
      name: 'Operations',
      meta: {
        crumbName: ['Operations List'],
        crumbPath: ['operations']
      },
      component: () => import('@/views/operations/operations.vue')
    },
    {
      path: '/alarm',
      name: 'alarm',
      component: () => import('@/views/alarm.vue')
    },
    {
      path: '/health',
      name: 'health',
      component: () => import('@/views/health.vue')
    },
    {
      path: '/authorization',
      name: 'authorization',
      component: () => import('@/views/authorization.vue')
    },
    {
      path: '*',
      component: () => import('@/views/404.vue')
    }
  ]
})

export default router
