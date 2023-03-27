<template>
  <div class="login login-bg">
    <h1 class="login-logo">ChubaoFS</h1>
    <div class="login-con clearfix">
      <div class="login-show-text">
        <h3 class="login-welcom">{{ $t('chubaoFS.login.welcome') }}</h3>
        <div class="underline mt10"></div>
        <p class="login-welcom-subtit">{{ $t('chubaoFS.login.subWelcome') }}</p>
      </div>
      <div class="login-block">
        <h3 class="mb10">{{ $t('chubaoFS.login.seeMore') }}</h3>
        <p class="h3-subtit">{{ $t('chubaoFS.login.subSeeMore') }}</p>
        <div class="mt60">
          <p class="login-form-tit mb5">{{ $t('chubaoFS.login.name') }}</p>
          <el-input v-model="name" placeholder="Please enter name" class="login-form-txt" @keyup.enter.native="login" ></el-input>
          <p class="login-form-tit mt40 mb5">{{ $t('chubaoFS.login.password') }}</p>
          <el-input show-password v-model="password" placeholder="Please enter password" class="login-form-txt" @keyup.enter.native="login" ></el-input>
          <el-button type="primary" @click="login" class="login-form-submit mt60">{{ $t('chubaoFS.login.login') }}</el-button>
        </div>
      </div>
    </div>
    <div class="copy-right">
      &copy; &nbsp;2020 The ChubaoFS Authors
    </div>
  </div>
</template>

<script>
import baseGql from '../graphql/baseGql'
export default {
  name: 'login',
  data () {
    return {
      name: '',
      password: ''
    }
  },
  methods: {
    login () {
      const variables = {
        userID: this.name,
        password: this.sha1(this.password)
      }
      this.apollo.query(this.url.login, baseGql.login, variables).then((res) => {
        if (res.code || res.message) {
          this.$message.error(res.message)
          return
        }
        sessionStorage.setItem('access_token', res.data.login.token)
        sessionStorage.setItem('access_userID', res.data.login.userID)
        if (sessionStorage.getItem('returnUrl')) {
          window.location.href = sessionStorage.getItem('returnUrl')
        } else {
          this.$router.push({ path: '/' })
        }
      }).catch((error) => {
        console.log(error)
      })
    }
  }
}
</script>

<style scoped>
  .login{
    width:100%;
    height:100vh;
    min-height: 650px;
    overflow: auto;
  }
  .login-bg{
    background: url("../assets/images/login-bg.jpg") no-repeat;
    background-size: cover;
  }
  .login-logo{
    width:186px;
    height:48px;
    margin-top:48px;
    margin-left: 48px;
    /*display: inline-block;*/
    font-size: 0;
    background: url("../assets/images/logo-pic.png") no-repeat;
    background-size: cover;
  }
  .login-con{
    max-width:1020px;
    /*height: calc(100vh - 228px);*/
    height:600px;
    margin: 0 auto;
    margin-top: calc(50vh - 410px);
  }
  .login-show-text{
    width: 50%;
    margin-top: 150px;
    float: left;
    /*position: absolute;*/
    /*left:25%;*/
    /*top: 32%*/
  }
  .login-welcom{
    line-height: 108px;
    font-family: Helvetica-Bold;
    font-size: 90px;
    color: rgba(255,255,255,1);
    text-shadow: 0px 9px 17px rgba(1,61,98,0.61);
  }
  .underline{
    width: 64px;
    height: 6px;
    margin-left: 10px;
    background: rgba(255,255,255,1);
  }
  .login-welcom-subtit{
    width: 277px;
    height: 24px;
    line-height: 24px;
    font-family: Helvetica;
    font-size: 20px;
    padding-left: 10px;
    margin-top: 30px;
    color: rgba(255,255,255,1);
    text-shadow: 0px 1px 3px rgba(0,62,91,1);
  }
  .login-block{
    width: 40%;
    min-width: 300px;
    max-width: 490px;
    height: 100%;
    max-height: 600px;
    padding: 85px 50px;
    box-sizing: border-box;
    background: rgba(255,255,255,1);
    border-radius: 12px;
    box-shadow: 0px 13px 30px 0px rgba(0,31,76,0.19);
    float:right;
    /*position: absolute;*/
    /*top:calc(50% - 315px);*/
    /*right:18%;*/
    /*margin-top:calc(50% - 315px);*/
    /*margin-left: 60%;*/
    /*position: relative;*/
  }
  .login-block h3{
    height: 36px;
    line-height: 36px;
    font-family: Helvetica-Bold;
    font-size: 30px;
    color: rgba(40,181,247,1);
  }
  .login-block .h3-subtit{
    height: 19px;
    line-height: 19px;
    font-family: Helvetica;
    font-size: 16px;
    color: rgba(155,155,155,1);
  }
  .login-form-tit{
    height: 19px;
    line-height: 19px;
    font-family: Helvetica;
    font-size: 16px;
    color: rgba(200,200,200,1);
  }
  .login-form-txt{
  }
  .login-form-submit{
    width: 220px;
    height: 60px;
    background: rgba(14,172,244,1);
    border-radius: 30px;
    box-shadow: 0px 5px 25px 0px rgba(41,158,241,0.29);
    margin-left: calc(50% - 110px);
  }
  .login .copy-right{
    width: 100%;
    height: 17px;
    line-height: 17px;
    font-size: 14px;
    color: rgba(255,255,255,1);
    text-align: center;
    position: absolute;
    bottom:20px;
  }
  @media screen and (max-width: 1200px) {
    .login-con{
      max-width:1020px;
      width:90%;
      height:auto;
      margin: 0 auto;
      margin-top: 0;
      margin-bottom: 50px;
    }
    .login-logo{
      margin-top: 30px;
    }
    .login .copy-right{
      position: inherit;
      margin-bottom: 20px;
    }
    .login-show-text{
      width:100%;
      margin-top: 10px;
      float: none;
      text-align: center;
    }
    .login-welcom{
      font-size: 40px;
      line-height: 50px;
    }
    .underline{
      margin: 10px auto;
    }
    .login-welcom-subtit{
      width:100%;
      text-align: center;
      margin-top: 0px;
    }
    .login-block{
      float: none;
      margin: 0 auto;
      width: 40%;
      min-width: 300px;
      max-width: 490px;
      height: auto;
      max-height: 600px;
      padding: 25px 50px;
      margin-top: 20px;
    }
  }
</style>
<style>
  .login-form-txt .el-input__inner{
    height:50px;
    line-height: 50px;
    padding-left: 0;
    font-size: 16px;
    border:none;
    color:#057FE1;
    border-bottom: #E7E7E7 1px solid;
  }
</style>
