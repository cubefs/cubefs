const moduleCluster = {
  namespaced: true,
  state: () => ({
    clusterName: '',
    leaderAddr: '',
    masterAddr: [],
    cli: '',
    ebsClusterInfo: [],
    clusterInfo: {},
    domainName: ''
  }),
  mutations: {
    setClusterInfo(state, payload) {
      const { clusterName, leaderAddr, masterAddr, cli, domainName, clusterInfo, ebsClusterInfo } = payload
      state.clusterName = clusterName
      state.leaderAddr = leaderAddr
      state.masterAddr = masterAddr
      state.cli = cli
      state.domainName = domainName
      state.clusterInfo = clusterInfo
      state.ebsClusterInfo = ebsClusterInfo
      sessionStorage.setItem('clusterInfo', JSON.stringify({ ...payload }))
    },
    setEbsClusterInfo(state, payload) {
      state.clusterInfo = payload.clusterInfo
      sessionStorage.setItem('clusterInfo', JSON.stringify(payload.clusterInfo))
    },
  },
  actions: {},
  getters: {
    clusterInfog(state) {
      const { clusterName, leaderAddr, masterAddr, cli, domainName, clusterInfo, ebsClusterInfo } = state
      if (clusterName && leaderAddr) {
        return {
          clusterName, leaderAddr, masterAddr, cli, domainName, clusterInfo, ebsClusterInfo
        }
      } else {
        return JSON.parse(sessionStorage.getItem('clusterInfo'))
      }
    },
    ebsClusterInfog(state) {
      const { ebsClusterInfo, clusterName } = state
      if (clusterName) {
        return ebsClusterInfo;
      } else {
        return JSON.parse(sessionStorage.getItem('clusterInfo')).ebsClusterInfo
      }
    },
    ebsIdcList(state) {
      const clusterInfo = state.clusterInfo || JSON.parse(sessionStorage.getItem('clusterInfo'))
      return clusterInfo.idc.split(',').map(item => ({ label: item, value: item }))
    },
  },
}
export default moduleCluster
