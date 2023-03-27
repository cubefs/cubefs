<template>
  <div class="left-menu">
    <el-menu class="el-menu-vertical-demo menu-wrapper"
             :default-active="activePath"
             :unique-opened="true"
             :collapse="isCollapse" router>
      <div class="logo"></div>
      <el-menu-item index="/overview">
        <i class="menuIcon1"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Overview') }}</span>
      </el-menu-item>
      <el-menu-item index="/servers">
        <i class="menuIcon2"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Servers') }}</span>
      </el-menu-item>
      <!--<el-menu-item index="/dashboard">-->
      <!--<i class="menuIcon3"></i>-->
      <!--<span slot="title">Dashboard</span>-->
      <!--</el-menu-item>-->
      <el-menu-item index="/volumeList">
        <i class="menuIcon4"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Volume') }}</span>
      </el-menu-item>
      <el-menu-item index="/operations">
        <i class="menuIcon5"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Operations') }}</span>
      </el-menu-item>
      <el-menu-item index="/alarm">
        <i class="menuIcon6"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Alarm') }}</span>
      </el-menu-item>
      <el-menu-item index="/authorization">
        <i class="menuIcon7"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Authorization') }}</span>
      </el-menu-item>
      <el-menu-item index="/health">
        <i class="menuIcon3"></i>
        <span slot="title">{{ $t('chubaoFS.nav.Health') }}</span>
      </el-menu-item>
      <div class="menu-link">
        <a class="github" href="https://github.com/cubefs/cubefs" target="_blank"></a>
        <a class="twitter" href="https://twitter.com/chubaofs" target="_blank"></a>
        <a class="slack" href="https://chubaofs.slack.com" target="_blank"></a>
      </div>
    </el-menu>
  </div>
</template>

<script>
import baseGql from '../graphql/baseGql'
export default {
  name: 'leftMenu',
  data () {
    return {
      unique: true,
      activePath: this.$route.path,
      dashboardUrl: null
    }
  },
  props: {
    isCollapse: {
      type: Boolean,
      default: false
    }
  },
  methods: {

  },
  created () {
    this.dashboardUrl = sessionStorage.getItem('dashboard_url')
    console.log(this.dashboardUrl)
    this.activePath = this.$route.path
    if (this.$route.path === '/') {
      const activePath = window.location.pathname
      switch (activePath) {
        case '/serverList':
          this.activePath = '/servers'
          break
        case '/serverDetail':
          this.activePath = '/servers'
          break
        case '/userDetails':
          this.activePath = '/overview'
          break
        default:
          this.activePath = activePath
          break
      }
    }
  }
}
</script>

<style scoped>
  .left-menu {
    height: auto;
    background-color: #2E384D;
  }
  .left-menu .menu-wrapper {
    width: 200px;
    overflow: hidden;
    font-size: 14px;
    background-color: #2E384D;
    border-right: none;
  }
  .menu-wrapper .logo {
    height: 60px;
    background: #2E384D url("../assets/images/logo.png") center no-repeat;
    background-size: 160px 36px;
    margin-bottom: 20px;
  }
  .menu-wrapper.el-menu--collapse{
    padding: 0;
    width: 64px;
  }
  .menu-wrapper.el-menu--collapse .logo{
    height: 60px;
    background: #2E384D url('../assets/images/logo_small.png') center no-repeat;
    background-size: 35px auto;
  }
  .menu-wrapper.el-menu--collapse>.el-menu-item:focus, .menu-wrapper.el-menu--collapse>.el-menu-item:hover, .menu-wrapper.el-menu--collapse>.el-menu-item.is-active {
    margin-bottom: 10px;
    background-color: #fff;
    border-radius: 15px;
    color: #07268D;
  }
  .menu-wrapper.el-menu--collapse>.el-menu-item i, .menu-wrapper.el-menu--collapse .el-submenu__title i {
    padding-right: 0;
    font-size: 24px;
  }
  .menu-wrapper.el-menu--collapse .menu-link a {
    margin-right: 0;
    margin-bottom: 5px;
  }
  .left-menu .menu-link {
    margin-top: 20px;
    padding-top: 20px;
    text-align: center;
    border-top: 1px solid rgba(255, 255, 255, .17);
  }
  .left-menu .menu-link a {
    position: relative;
    display: inline-block;
    width: 36px;
    height: 36px;
    margin-right: 10px;
  }
  .left-menu .menu-link a.github {
    background: url("../assets/images/github.png") no-repeat;
    background-size: 36px 36px;
  }
  .left-menu .menu-link a.github:hover {
    background: url("../assets/images/github-top.png") no-repeat;
    background-size: 36px 36px;
  }
  .left-menu .menu-link a.twitter {
    background: url("../assets/images/twitter.png") no-repeat;
    background-size: 36px 36px;
  }
  .left-menu .menu-link a.twitter:hover {
    background: url("../assets/images/twitter-top.png") no-repeat;
    background-size: 36px 36px;
  }
  .left-menu .menu-link a.slack {
    background: url("../assets/images/slack.png") no-repeat;
    background-size: 36px 36px;
  }
  .left-menu .menu-link a.slack:hover {
    background: url("../assets/images/slack-top.png") no-repeat;
    background-size: 36px 36px;
  }
  .menu-wrapper i {
    display: inline-block;
    width: 25px;
    height: 100%;
  }
  .menu-wrapper i.menuIcon1{
    background: url('../assets/images/menuIcon1.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon2{
    background: url('../assets/images/menuIcon2.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon3{
    background: url('../assets/images/menuIcon3.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon4{
    background: url('../assets/images/menuIcon4.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon5{
    background: url('../assets/images/menuIcon5.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon6{
    background: url('../assets/images/menuIcon6.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper i.menuIcon7{
    background: url('../assets/images/menuIcon7.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon1 {
    background: url('../assets/images/menuActive1.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon2 {
    background: url('../assets/images/menuActive2.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon3 {
    background: url('../assets/images/menuActive3.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon4 {
    background: url('../assets/images/menuActive4.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon5 {
    background: url('../assets/images/menuActive5.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon6 {
    background: url('../assets/images/menuActive6.png') no-repeat left center;
    background-size: 18px auto;
  }
  .el-menu-item.is-active i.menuIcon7 {
    background: url('../assets/images/menuActive7.png') no-repeat left center;
    background-size: 18px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon1{
    background: url('../assets/images/menuIcon1.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon2{
    background: url('../assets/images/menuIcon2.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon3{
    background: url('../assets/images/menuIcon3.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon4{
    background: url('../assets/images/menuIcon4.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon5{
    background: url('../assets/images/menuIcon5.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon6{
    background: url('../assets/images/menuIcon6.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse i.menuIcon7{
    background: url('../assets/images/menuIcon7.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon1, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon1 {
    background: url('../assets/images/menuActive1.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon2, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon2 {
    background: url('../assets/images/menuActive2.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon3, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon3 {
    background: url('../assets/images/menuActive3.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon4, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon4 {
    background: url('../assets/images/menuActive4.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon5, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon5 {
    background: url('../assets/images/menuActive5.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon6, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon6 {
    background: url('../assets/images/menuActive6.png') no-repeat left center;
    background-size: 23px auto;
  }
  .menu-wrapper.el-menu--collapse .el-menu-item:hover i.menuIcon7, .menu-wrapper.el-menu--collapse .el-menu-item.is-active i.menuIcon7 {
    background: url('../assets/images/menuActive7.png') no-repeat left center;
    background-size: 23px auto;
  }
</style>
