<template>
  <el-container>
    <el-header class="header">
      <div class="expand-icon-container" @click="sidebarExpand = !sidebarExpand">
        <svg v-if="sidebarExpand" width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
          <path fill-rule="evenodd" clip-rule="evenodd" d="M13 2H1V4H13V2ZM11 7H1V9H11V7ZM1 12H9V14H1V12ZM12.0641 10.9359L13.8284 9.17157L14.8915 10.2347L13.1272 11.999L14.8925 13.7643L13.8284 14.8284L12.0631 13.0631L11 12L12.0641 10.9359Z" fill="white" />
        </svg>
        <svg v-else width="16" height="16" viewBox="0 0 16 16" xmlns="http://www.w3.org/2000/svg">
          <path fill-rule="evenodd" clip-rule="evenodd" d="M13 2H1V4H13V2ZM11 7H1V9H11V7ZM1 12H9V14H1V12ZM15.5928 10.9359L13.8284 9.17157L12.7653 10.2347L14.5297 11.999L12.7643 13.7643L13.8284 14.8284L15.5938 13.0631L16.6569 12L15.5928 10.9359Z" fill="white" />
        </svg>
      </div>
      <el-image
        :src="require('@/assets/images/nav-logo.png')"
        style="height: 30px"
      />
      <div class="role">当前角色:
        <el-tooltip effect="dark" :content="roleList.join()" placement="bottom">
          <span>{{ roleList[0] }}</span>
        </el-tooltip>
      </div>
      <el-dropdown class="header-dropdown" @command="handleCommand">
        <span class="el-dropdown-link">
          {{ userInfo?.user_name }}<i class="el-icon-arrow-down el-icon--right"></i>
        </span>
        <el-dropdown-menu slot="dropdown">
          <el-dropdown-item command="docs">使用文档</el-dropdown-item>
          <el-dropdown-item command="logout">退出登录</el-dropdown-item>
        </el-dropdown-menu>
      </el-dropdown>
    </el-header>
    <el-container class="body dashboard-body">
      <el-aside
        v-if="!hideSidebar"
        class="sidebar"
        :width="`${sidebarExpand ? width : 0}px`"
      >
        <div :style="{ width: `${width}px` }" class="sidebar-content">
          <el-scrollbar wrap-class="scrollbar-wrapper">
            <!-- <div class="sidebar-header">
              <div class="app-name">{{ appName }}</div>
            </div> -->
            <menu-tree :menus="menus"></menu-tree>
          </el-scrollbar>
        </div>
      </el-aside>
      <el-main class="main">
        <!-- <div
          v-if="!hideSidebar"
          class="collapse-btn"
          @click="sidebarExpand = !sidebarExpand"
        >
          <o-icon class="icon" type="ios-arrow-forward" />
        </div> -->
        <div class="main-inner">
          <div class="main-top">
            <breadcrumb v-if="!$route.meta.breadcrumbHide" class="m-b-15"></breadcrumb>
            <p class="current-cluster" v-if="!['clusterList', 'authManage'].includes($route.name)">当前集群: {{ clusterName || '无' }}</p>
          </div>
          <main-content></main-content>
        </div>
      </el-main>
    </el-container>
  </el-container>
</template>
<script>
import MenuTree from './MenuTree'
import Breadcrumb from './Breadcrumb'
import MainContent from '@/layouts/Common/components/MainContent'
import { userLogout } from '@/api/auth'
import mixin from '@/pages/cfs/clusterOverview/mixin'

export default {
  name: 'DashboardLayout',
  mixins: [mixin],
  components: {
    MenuTree, // 管理控制台菜单树
    Breadcrumb,
    MainContent,
  },
  props: {
    hideSidebar: {
      type: Boolean,
      default: false,
    },
    appName: {
      type: String,
      default: 'Protal',
    },
  },
  data() {
    return {
      sidebarExpand: true,
      width: 195,
      roleList: [],
    }
  },
  computed: {
    menus() {
      return this.formatMenus(this.$store.getters.getRoutes)
    },
    userInfo() {
      return JSON.parse(localStorage.getItem('userInfo'))
    },
  },
  async mounted() {
    const { data } = await this.$ajax.get('/api/cubefs/console/auth/user/list', { 
      user_name_like: this.userInfo?.user_name,
      page: 1,
      page_size: 15,
    })
    if (data.users.length) {
      this.roleList = data.users[0].roles.map(item => item.role_name)
    }
  },
  methods: {
    formatMenuLinkGroup(menu) {
      const { meta = {} } = menu.options
      // eslint-disable-next-line no-void
      if (meta.menuLinkGroup === void 0) return false
      if (meta.menuLinkGroup === true) return menu.path
      if (meta.menuLinkGroup.length !== 0) return meta.menuLinkGroup
    },
    formatMenuLinkItem(menu) {
      const { meta = {} } = menu.options
      // eslint-disable-next-line no-void
      if (meta.menuLinkItem === void 0) return false
      if (meta.menuLinkItem === true) return menu.path
    },
    formatMenus(menus = [], prefix = '/') {
      // 先处理是否存在路由配置穿透
      const filterMenus = []
      menus.forEach(v => {
        if (v.meta && v.meta.penetrate && v.children) {
          prefix = (prefix + v.path + '/').replace(/\/+/g, '/')
          v.children.forEach(vv => {
            filterMenus.push(vv)
          })
        } else {
          filterMenus.push(v)
        }
      })

      return filterMenus
        .filter(({ meta = {}, redirect }) => {
          // 1. 未被忽略的
          // 2. 不是重定向的
          return (
            !meta.menuIgnore &&
            !redirect
          )
        })
        .map(v => {
          const { meta = {}, children, path, name } = v
          const item = {
            title: meta.title || '未命名',
            name,
            icon: meta.menuIcon,
            expand: meta.menuExpandDefault,
            path: (prefix + path).replace(/\/+/g, '/'),
            options: v,
          }
          item.linkGroup = this.formatMenuLinkGroup(item)
          item.menuLinkItem = this.formatMenuLinkItem(item)
          if (children) {
            // 路径从上一层拼凑而来
            if (!meta.menuLinkGroup) {
              item.children = this.formatMenus(children, prefix + path + '/')
            }
          }
          return item
        })
    },
    handleCommand(command) {
      switch (command) {
        case 'docs':
          window.open('https://cubefs.io/zh/docs/master/overview/introduction.html')
          break
        case 'logout':
          userLogout().then(() => {
            this.$message.success('注销成功')
            localStorage.removeItem('userInfo')
            this.$router.push({ name: 'loginPage' })
          })
          break
      }
    },
  },
}
</script>
<style lang="scss" scoped>
.header {
  height: 48px !important;
  background: #1a123f;
  display: flex;
  align-items: center;
  padding: 0;
}
.expand-icon-container {
  height: 100%;
  width: 48px;
  background: #352e56;
  display: flex;
  justify-content: center;
  align-items: center;
  cursor: pointer;
  margin-right: 20px;
}
.header-dropdown {
  padding: 2px 4px;
  margin-right: 20px;
  color: #fff;
  border-radius: 4px;
  cursor: pointer;
}
.header-dropdown:hover {
  background: #5c5776;
}
.role {
  margin-left: auto;
  margin-right:5px;
}
// 菜单栏样式
.sidebar {
  transition: width 0.5s ease;
  background: #ffffff;
  padding-left: 0;
  padding-right: 0;
  position: relative;
  overflow-x: hidden;

  .sidebar-header {
    background: #1a1d20;
  }

  .app-name {
    line-height: 80px;
    color: white;
    font-weight: bold;
    padding-left: 20px;
    font-size: 22px;
  }
}
.body {
  height: calc(100vh - 60px);
  .sidebar-content {
    height: 100%;
  }
  .main {
    position: relative;
    height: 100%;
    overflow-y: hidden;
    padding: 0;
    .main-inner {
      height: 100%;
      overflow-y: scroll;
      padding: 20px;
    }
    .main-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .current-cluster {
      margin-bottom: 15px;
      font-size: 14px;
      color: #000;
    }
    .collapse-btn {
      height: 50px;
      width: 0;
      top: 50%;
      position: absolute;
      border-bottom: 9px solid transparent;
      border-right: none;
      transform: translateY(-50%);
      left: -0;
      border-left: 13px solid #e6e6e7;
      border-top: 9px solid transparent;
      z-index: 1;
      cursor: pointer;
      display: flex;
      justify-content: center;
      align-items: center;
      .icon {
        position: relative;
        left: -7px;
        font-size: 12px;
        color: #999;
      }
      &:hover {
        height: 60px;
        transition: height 0.1s ease;
        border-left-color: #c3c2c2;
        .icon {
          font-size: 14px;
          color: #666;
        }
      }
    }
  }
}
::v-deep {
  .sidebar-content {
    .el-scrollbar,
    .scrollbar-wrapper {
      height: 100%;
      overflow-x: hidden;
    }
    .scrollbar-wrapper {
      margin-bottom: 0 !important;
    }
  }
}
</style>
