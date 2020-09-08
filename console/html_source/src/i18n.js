import Vue from 'vue'
import VueI18n from 'vue-i18n'
import Element from 'element-ui'
import messages from './locales'

Vue.use(VueI18n)

const matchArr = window.location.href.match(/#\/(zh|en)/)
const urlLang = matchArr && matchArr[1]

const userLang = urlLang || window.localStorage.getItem('ChubaoFS-language') || 'en' // || navigatorLang

const i18n = new VueI18n({
  locale: userLang,
  fallbackLocale: 'en',
  messages: messages
})
Vue.use(Element, {
  i18n: (key, value) => i18n.t(key, value)
})
export default i18n
