<template>
  <el-breadcrumb
    v-if="$route.path !== '/dashboard'"
    class="app-breadcrumb"
    separator=">"
  >
    <el-breadcrumb-item
      v-for="v of matched"
      :key="v.path"
      :to="v.meta.breadcrumbLink"
    >
      <breadcrumb-render :matcher="v"></breadcrumb-render>
    </el-breadcrumb-item>
  </el-breadcrumb>
</template>
<script>
export default {
  components: {
    BreadcrumbRender: {
      functional: true,
      name: 'BreadcrumbRender',
      render(
        h,
        {
          parent,
          props: { matcher },
        },
      ) {
        const {
          meta: { breadcrumbRender, title },
        } = matcher
        return breadcrumbRender
          ? (
              breadcrumbRender(h, { route: parent.$route })
            )
          : (
          <span>{title || '未命名'}</span>
            )
      },
    },
  },
  computed: {
    matched() {
      // 穿透的路由也不要出现在面包屑里
      return this.$route.matched.filter(v => !v.meta.penetrate)
    },
  },
}
</script>
