<template>
  <div class="header pr">
    <!--<slot></slot>-->
    <div class="right">
      <div class="link">
        <div class="fl mr30">
          <el-dropdown>
            <i :class="lang==='en'? 'en-icon' : 'zh-icon'"></i>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item><p @click="switchLang('en')" :class="lang==='en'? 'chubaofs-en-icon lang-active' : 'chubaofs-en-icon'">English</p></el-dropdown-item>
              <el-dropdown-item><p @click="switchLang('zh')" :class="lang==='zh'? 'chubaofs-zh-icon lang-active' : 'chubaofs-zh-icon'">中 文</p></el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
        </div>
        <!--<i class="en-icon"></i>-->
        <!--<i class="file-icon"></i>-->
        <router-link to='/alarm'><i class="warn-icon" ></i></router-link>
      </div>
      <div class="user ml30">
        <el-dropdown>
                <span class="el-dropdown-link">
                    <i class="head-icon">
                        <img src="../assets/images/portrait.png" alt="">
                    </i>
                    {{userID}}
                    <i class="el-icon-arrow-down el-icon--right"></i>
                </span>
          <el-dropdown-menu slot="dropdown">
            <!--<el-dropdown-item>-->
              <!--<i class="el-icon-edit-outline"></i>-->
              <!--Change password-->
            <!--</el-dropdown-item>-->
            <el-dropdown-item><p @click="logOut()">{{$t('chubaoFS.login.SignOut')}}</p></el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
      </div>
    </div>
  </div>
</template>

<script>
import { mapGetters } from 'vuex'
export default {
  name: 'mainHeader',
  data () {
    return {
      userID: null
    }
  },
  computed: {
    ...mapGetters([
      'loginUserName'
    ]),
    lang () {
      return this.$i18n.locale
    }
  },
  methods: {
    logOut () {
      sessionStorage.clear()
      sessionStorage.setItem('returnUrl', window.location.href)
      this.$router.push({ path: '/login' })
    },
    switchLang (val) {
      if (val === this.lang) return
      this.$i18n.locale = val
      window.localStorage.setItem('ChubaoFS-language', val)
    }
  },
  mounted () {
    this.userID = sessionStorage.getItem('access_userID')
    if (!this.userID) {
      this.$router.push({ path: '/login' })
    }
  }
}
</script>

<style>
  .chubaofs-en-icon {
    height: 40px;
    line-height: 40px;
    padding-left: 35px;
    background: url("../assets/images/en-img.png") left center no-repeat;
    background-size: 24px 24px;
  }
  .chubaofs-zh-icon {
    height: 40px;
    line-height: 40px;
    padding-left: 35px;
    background: url("../assets/images/zh-img.png") left center no-repeat;
    background-size: 24px 24px;
  }
  .lang-active{
    color:#409eff;
  }
</style>
