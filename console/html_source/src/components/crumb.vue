<template>
  <div id="crumb" class="crumb mb20">
    <el-breadcrumb separator-class="el-icon-arrow-right">
      <el-breadcrumb-item :to="'/' + item" :key="index" v-for="(item, index) in crumbPath">{{crumbName[index]}}</el-breadcrumb-item>
      <el-breadcrumb-item v-show="showCutTag"><span>...</span></el-breadcrumb-item>
      <el-breadcrumb-item :to="{path: item.path ,query: item.query}" :key="crumbPath.length + index" v-for="(item, index) in showCrumbItem">{{item.title}}</el-breadcrumb-item>
      <el-breadcrumb-item><span v-if="crumbInfo">{{crumbInfo}}</span></el-breadcrumb-item>
    </el-breadcrumb>
  </div>
</template>

<script>
export default {
  name: 'crumb',
  data () {
    return {
      crumbName: [],
      crumbPath: [],
      signHeight: 0,
      showCutTag: false,
      showCrumbItem: []
    }
  },
  computed: {
  },
  props: {
    crumbInfo: {
      type: String,
      default: ''
    },
    crumbItem: {
      type: Array
    }
  },
  methods: {
    cutCrumbItem () {
      const that = this
      let currentHeight = document.getElementById('crumb').offsetHeight
      // 出现折行
      if (currentHeight > 14) {
        setTimeout(function () {
          that.showCrumbItem.shift()
          that.showCutTag = true
        }, 5)
      }
    }
  },
  watch: {
    '$i18n.locale' () {
      const crumbName = this.$route.meta.crumbName
      this.crumbName = this.$t(crumbName) || []
    },
    crumbItem (newVal, oldVal) {
      const that = this
      that.showCutTag = false
      this.showCrumbItem = newVal.concat()
      setTimeout(function () {
        that.cutCrumbItem()
      }, 5)
    },
    showCrumbItem (newVal, oldVal) {
      const that = this
      setTimeout(function () {
        that.cutCrumbItem()
      }, 5)
    }
  },
  mounted () {
    const crumbName = this.$route.meta.crumbName
    this.crumbName = this.$t(crumbName) || []
    this.crumbPath = this.$route.meta.crumbPath || []
  }
}
</script>

<style scoped>
</style>
