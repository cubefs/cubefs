<template>
  <div class="container">
    <Logo class="logo" />
    <div class="description">
      Infinite Storage, Limitless Value
    </div>
    <div v-if="!isSignup" class="login">
      <el-form
        ref="loginForm"
        key="login"
        :model="loginForm"
        :rules="loginRules"
        label-width="0px"
        style="width: 240px; margin-top: 45px;"
      >
        <div class="label">
          账号
          <el-tooltip class="item" effect="dark" content="只能包含数字、大写字母、小写字母、下划线" placement="right">
            <i class="el-icon-question"></i>
          </el-tooltip>
        </div>
        <el-form-item prop="user_name">
          <el-input v-model="loginForm.user_name" placeholder="请输入账号" />
        </el-form-item>
        <div class="label">
          密码
          <el-tooltip class="item" effect="dark" placement="right">
            <div slot="content">密码长度大于等于8小于等于16<br />密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上</div>
            <i class="el-icon-question"></i>
          </el-tooltip>
        </div>
        <el-form-item prop="password">
          <el-input v-model="loginForm.password" type="password" placeholder="请输入密码" />
        </el-form-item>
      </el-form>
      <div class="btn-group">
        <div class="signup-btn" @click="goToSignup">注册</div>
        <div class="signup-btn password-btn" @click="goToPassword">修改密码</div>
        <el-button v-loading="loading" class="login-btn" :class="{ active: canLogin }" @click="login">登录</el-button>
      </div>
    </div>
    <div v-if="isSignup" class="signup">
      <i class="el-icon-back back-icon" @click="goToLogin"></i>
      <el-form
        ref="signupForm"
        key="signup"
        :model="signupForm"
        label-width="0px"
        :rules="signupRules"
        style="width: 240px; margin-top: 45px;"
      >
        <div class="label">
          账号
          <el-tooltip class="item" effect="dark" content="只能包含数字、大写字母、小写字母、下划线" placement="right">
            <i class="el-icon-question"></i>
          </el-tooltip>
        </div>
        <el-form-item prop="user_name">
          <el-input v-model="signupForm.user_name" autocomplete="off" placeholder="请输入账号" />
        </el-form-item>
        <div v-if="!isPassword">
          <div class="label">
            密码
            <el-tooltip class="item" effect="dark" placement="right">
              <div slot="content">密码长度大于等于8小于等于16<br />密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上</div>
              <i class="el-icon-question"></i>
            </el-tooltip>
          </div>
          <el-form-item prop="password">
            <el-input v-model="signupForm.password" autocomplete="new-password" type="password" placeholder="请输入密码" />
          </el-form-item>
          <div class="label">手机</div>
          <el-form-item prop="phone">
            <el-input v-model="signupForm.phone" placeholder="请输入手机" />
          </el-form-item>
          <div class="label">邮箱</div>
          <el-form-item prop="email">
            <el-input v-model="signupForm.email" placeholder="请输入邮箱" />
          </el-form-item>
        </div>
        <div v-else>
          <div class="label">
            旧密码
            <el-tooltip class="item" effect="dark" placement="right">
              <div slot="content">密码长度大于等于8小于等于16<br />密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上</div>
              <i class="el-icon-question"></i>
            </el-tooltip>
          </div>
          <el-form-item prop="old_password">
            <el-input v-model="signupForm.old_password" type="password" placeholder="请输入旧密码" />
          </el-form-item>
          <div class="label">
            新密码
            <el-tooltip class="item" effect="dark" placement="right">
              <div slot="content">密码长度大于等于8小于等于16<br />密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上</div>
              <i class="el-icon-question"></i>
            </el-tooltip>
          </div>
          <el-form-item prop="new_password">
            <el-input v-model="signupForm.new_password" autocomplete="new-password" type="password" placeholder="请输入新密码" />
          </el-form-item>
        </div>
      </el-form>
      <el-button v-loading="signupLoading" class="signup-btn-big" :class="{ active: canSignup }" @click="signup">{{ isPassword ? '修改密码' : '注册' }}</el-button>
    </div>
    <div class="copyright">© 2023 The CubeFS Authors</div>
  </div>
</template>

<script>
import Logo from './logo.vue'
import {
  userLogin,
  userCreate,
  selfPasswordUpdate,
} from '@/api/auth'
export default {
  components: {
    Logo,
  },
  data() {
    const validateUser = (rule, value, cb) => {
      if (this.checkUser(value)) {
        cb()
      } else {
        cb(new Error('账号格式错误！请重新输入'))
      }
    }
    const validatePassword = (rule, value, cb) => {
      if (this.checkPassword(value)) {
        cb()
      } else {
        cb(new Error('密码格式错误！请重新输入'))
      }
    }
    return {
      loginForm: {
        user_name: '',
        password: '',
      },
      signupForm: {
        user_name: '',
        password: '',
        email: '',
        phone: '',
        old_password: '',
        new_password: '',
      },
      isSignup: false,
      isPassword: false,
      loginRules: {
        user_name: [{ validator: validateUser, trigger: 'blur' }],
        password: { validator: validatePassword, trigger: 'blur' },
      },
      signupRules: {
        user_name: [{ validator: validateUser, trigger: 'blur' }],
        password: [{ validator: validatePassword, trigger: 'blur' }],
        phone: [{ required: true, message: '请输入手机号', trigger: 'blur' }],
        email: [{ required: true, message: '请输入邮箱', trigger: 'blur' }],
        old_password: [{ validator: validatePassword, trigger: 'blur' }],
        new_password: [
          { validator: validatePassword, trigger: 'blur' },
        ],
      },
      loading: false,
      signupLoading: false,
    }
  },
  computed: {
    canLogin() {
      return this.checkUser(this.loginForm.user_name) && this.checkPassword(this.loginForm.password)
    },
    canSignup() {
      if (!this.isPassword) {
        return this.checkUser(this.signupForm.user_name) && this.checkPassword(this.signupForm.password) && !!this.signupForm.phone && !!this.signupForm.email
      } else {
        return this.checkUser(this.signupForm.user_name) && this.checkPassword(this.signupForm.old_password) && this.checkPassword(this.signupForm.new_password)
      }
    },
  },
  watch: {
    isSignup(newVal) {
      if (newVal) {
        this.loginForm = {
          user_name: '',
          password: '',
        }
      } else {
        this.signupForm = {
          user_name: '',
          password: '',
          email: '',
          phone: '',
          old_password: '',
          new_password: '',
        }
      }
    },
  },
  methods: {
    goToSignup() {
      this.isSignup = true
    },
    goToPassword() {
      this.isSignup = true
      this.isPassword = true
    },
    goToLogin() {
      this.isSignup = false
      this.isPassword = false
    },
    login() {
      if (!this.canLogin || this.loading) {
        return
      }
      this.loading = true
      userLogin(this.loginForm).then(async() => {
        this.$message.success('登录成功')
        localStorage.setItem('userInfo', JSON.stringify({ user_name: this.loginForm.user_name }))
        await this.$store.dispatch('moduleUser/setAuth')
        this.$router.push('/')
      }).finally(() => {
        this.loading = false
      })
    },
    async signup() {
      if (!this.canSignup || this.loading) {
        return
      }
      await this.$refs.signupForm.validate()
      this.signupLoading = true
      if (!this.isPassword) {
        // eslint-disable-next-line camelcase
        const { old_password, new_password, ...params } = this.signupForm
        userCreate(params).then(() => {
          this.$message.success('注册成功')
          this.isSignup = false
        }).finally(() => {
          this.signupLoading = false
        })
      } else {
        // eslint-disable-next-line camelcase
        const { old_password, new_password, user_name } = this.signupForm
        selfPasswordUpdate({ old_password, new_password, user_name }).then(() => {
          this.$message.success('修改密码成功')
          this.isSignup = false
        }).finally(() => {
          this.signupLoading = false
        })
      }
    },
    checkUser(str) {
      return /^[0-9A-Za-z_]+$/.test(str)
    },
    checkPassword(str) {
      // 密码只能包含数字、大写字母、小写字母、特殊字符（~!@#$%^&*_.?），且至少两种类型以上
      const regList = [/[0-9]/, /[A-Z]/, /[a-z]/, /[~!@#$%^&*_.?]/]
      let num = 0
      regList.forEach(item => {
        if (item.test(str)) {
          num++
        }
      })
      if (str.length > 7 && str.length < 17 && num > 1) {
        return true
      }
      return false
    },
  },
}

</script>
<style scoped lang='scss'>
.container {
  width: 100vw;
  height: 100vh;
  background: url('@/assets/images/login.png');
  display: flex;
  flex-direction: column;
  align-items: center;
}
.logo {
  margin-top: 8vh;
  margin-bottom: 4vh;
}
.description {
  width: 800px;
  font-size: 26px;
  line-height: 36px;
  text-align: center;
  color: #FFFFFF;
  margin-bottom: 4vh;
}
.login {
  width: 540px;
  height: 326px;
  background: rgba(0, 0, 0, 0.33);
  display: flex;
  align-items: center;
  border-radius: 8px;
  flex-direction: column;
}
.signup {
  width: 540px;
  background: rgba(0, 0, 0, 0.33);
  display: flex;
  align-items: center;
  border-radius: 8px;
  flex-direction: column;
  position: relative;
}
.label {
  font-family: 'PingFang SC';
  font-style: normal;
  font-weight: 400;
  font-size: 13px;
  line-height: 22px;
  color: #FFFFFF;
  margin-bottom: 5px;
}
.btn-group {
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 240px;
}
.signup-btn {
  margin-top: 10px;
  color: #c7c7c7;
  cursor: pointer;
}
.signup-btn:hover {
  color: #FFFFFF
}
.password-btn {
  margin-left: -20px;
}
.login-btn {
  border: none;
  margin-top: 12px;
  width: 120px;
  background: #CCE6DE;
  border-radius: 4px;
  height: 38px;
  font-weight: 550;
  font-size: 14px;
  line-height: 22px;
  color: #FFFFFF;
}
.signup-btn-big {
  border: none;
  margin-top: 12px;
  margin-bottom: 40px;
  width: 240px;
  background: #CCE6DE;
  border-radius: 4px;
  height: 38px;
  font-weight: 550;
  font-size: 14px;
  line-height: 22px;
  color: #FFFFFF;
}
.active {
  background: #008059;
}

.copyright {
  margin-top: auto;
  margin-bottom: 2vh;
  color: #FFFFFF;
  opacity: 39%;
}
.back-icon {
  color: #c7c7c7;
  cursor: pointer;
  position: absolute;
  top: 10px;
  left: 10px;
  font-size: 24px;
}
.back-icon:hover {
  color: #FFFFFF
}
</style>
