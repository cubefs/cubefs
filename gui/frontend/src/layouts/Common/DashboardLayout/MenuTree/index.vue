<template>
  <div class="tree-view-menu">
    <MenuTreeItem :menus="menus" />
  </div>
</template>
<script>
import MenuTreeItem from './MenuTreeItem'
export default {
  name: 'MenuTree',
  components: {
    MenuTreeItem,
  },
  props: {
    menus: {
      type: Array,
      default() {
        return []
      },
    },
  },
}
</script>
