<template>
  <div class="layout">
    <el-container v-if="isLogin">
      <router-view></router-view>
    </el-container>
    <el-container v-else style="min-height:100%;">
      <left-menu :isCollapse="isCollapse"></left-menu>
      <el-container class="container">
        <main-header>
          <span class="changeCollapse"
                :class="[isCollapse? 'fold' : 'unfold']"
                @click="isCollapse=!isCollapse"
          ></span>
        </main-header>
        <main class="main">
          <router-view></router-view>
        </main>
        <main-footer></main-footer>
      </el-container>
    </el-container>
  </div>
</template>

<script>
import mainHeader from '@/components/header'
import leftMenu from '@/components/menu'
import mainFooter from '@/components/footer'
export default {
  name: 'index',
  components: {
    mainHeader,
    leftMenu,
    mainFooter
  },
  data () {
    return {
      isLogin: false,
      isCollapse: false
    }
  },
  computed: {
    lang () {
      return this.$i18n.locale
    }
  },
  watch: {
    $route (to, from) {
      this.isLogin = (window.location.pathname === '/login')
    }
  },
  mounted () {
    this.isLogin = (window.location.pathname === '/login')
  }
}
</script>

<style scoped>
@import "assets/css/reset.css";
@import "assets/css/common.css";
  .layout{
    height: 100%;
  }
  .container{
    -webkit-box-orient: vertical;
    flex-direction: column;
  }
  .container .main{
    display: block;
    flex: 1;
    flex-basis: auto;
    padding: 20px;
  }
  .changeCollapse{
    display: inline-block;
    width: 20px;
    height: 60px;
    background: url('./assets/images/fold.png') center no-repeat;
    float: left;
    cursor: pointer;
  }
  .changeCollapse.unfold{
    background: url('./assets/images/unfold.png') center no-repeat;
  }
</style>
