import { userAuth } from '@/api/auth'

export const moduleUser = {
  namespaced: true,
  state: () => ({
    allAuth: null,
  }),
  mutations: {
    SETALLAUTH(state, data) {
      state.allAuth = data
    },
  },
  actions: {
    setAuth({ commit }) {
      return new Promise((resolve, reject) => {
        userAuth().then(({ data }) => {
          commit('SETALLAUTH', data)
          resolve()
        }).catch((error) => {
          reject(error)
        })
      })
    },
  },
}
