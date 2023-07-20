<template>
  <ul class="tree-list">
    <li v-for="(menu, i) in menus" :key="menu.path" class="tree-item">
      <div
        v-if="!menu.children || menu.linkGroup || menu.menuLinkItem"
        :style="{ paddingLeft: (deep + 1) * 20 + 'px' }"
        :class="{ 'is-root': menu.path === '/' ,
                  active : (menu.menuLinkItem && $route.path === menu.path) || (!menu.menuLinkItem && $route.path.includes(menu.path)) ||
                    $route.path.includes(menu.linkGroup)

        }"
        class="link"
        @click="routeJump(menu)"
      >
        <i v-if="menu.icon" class="menu-icon" :class="menu.icon"></i>
        <div style="display: flex;">
          <SvgIcon
            :icon="menu.title"
            :class="{
              activeIcon : (menu.menuLinkItem && $route.path === menu.path) || (!menu.menuLinkItem && $route.path.includes(menu.path)) || $route.path.includes(menu.linkGroup)
            }"
          />
          <div>{{ menu.title }}</div>
        </div>
      </div>
      <span
        v-else
        :style="{ paddingLeft: (deep + 1) * 20 + 'px' }"
        :class="{ expand: clone[i]?.expand }"
        class="button"
        @click="toggle(i)"
      >
        {{ menu.title }}
        <i class="expand-icon el-icon-arrow-right"></i>
      </span>
      <el-collapse-transition>
        <MenuTreeItem
          v-show="clone[i]?.expand"
          v-if="menu.children"
          :class="{ expand: clone[i]?.expand }"
          :menus="menu.children"
          :deep="deep + 1"
        />
      </el-collapse-transition>
    </li>
  </ul>
</template>
<script>
import SvgIcon from '@/assets/svg/SvgIcon.vue'
export default {
  name: 'MenuTreeItem',
  components: {
    SvgIcon,
  },
  props: {
    menus: {
      type: Array,
      default() {
        return []
      },
    },
    deep: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      active: '',
      clone: this.menus.map(v => ({
        expand: v.expand,
      })),
    }
  },
  watch: {
    $route: {
      handler(val) {
        this.menus.forEach((v, i) => {
          const cloneItem = this.clone[i]
          if (!cloneItem?.expand && val.path.indexOf(v.path) === 0) {
            if (cloneItem) {
              cloneItem.expand = true
            }
          }
        })
      },
      immediate: true,
    },
  },
  methods: {
    toggle(index) {
      const item = this.clone[index]
      item.expand = !item.expand
    },
    routeJump(menu) {
      const menuLink = menu.options.meta.menuLink
      const menuLinkTarget = menu.options.meta.menuLinkTarget
      const menuLinkGroup = menu.linkGroup
      if (menuLink) {
        window.open(menuLink, menuLinkTarget)
      } else {
        this.$router.push(
          menuLinkGroup || menu.menuLinkItem ||
            (menu.name ? { name: menu.name } : { path: menu.path }),
        )
      }
    },
  },
}
</script>
<style lang="scss" scoped>
.tree-list {
  transition: all 0.2s;
  transform-origin: top;
}
.button,
.link {
  font-size: 14px;
  line-height: 48px;
  font-family: 'OPlusSans 3.0';
  color: rgba(0, 0, 0, 0.8);
  cursor: pointer;
  background-color: #fff;
  &:hover:not(.active) {
    color: #008059;
    background-color: #f2f8f6;
  }
}
.menu-icon {
  margin-right: 5px;
}

.expand-icon {
  font-weight: 700;
  margin-right: 10px;
}
.button {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-right: 10px;
  .expand-icon {
    transition: transform 0.2s;
  }
}
.button.expand {
  color: rgba(0, 0, 0, 0.8);
  .expand-icon {
    transform: rotate(90deg);
  }
}
.deep-2 .link {
  background-color: #2e3136;
}
.link.is-root-active.is-root,
.link.router-link-active:not(.is-root) {
  background-color: $theme-color;
  color: white;
}
.active {
  background: #f2f8f6;

  color: #008059;
}
.activeIcon {
  color: #008059;
}
</style>
