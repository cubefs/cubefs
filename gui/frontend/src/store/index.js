import Vue from 'vue'
import Vuex from 'vuex'
import actions from './actions'
import mutations from './mutations'
import getters from './getter'
import clusterInfoModule from './modules/cluster'
import { moduleUser } from './modules/user'
import { moduleEbs } from './modules/ebs'
Vue.use(Vuex)

export default new Vuex.Store({
  modules: {
    clusterInfoModule,
    moduleEbs,
    moduleUser,
  },
  state: {
    routes: []
  },
  getters,
  actions,
  mutations,
})
