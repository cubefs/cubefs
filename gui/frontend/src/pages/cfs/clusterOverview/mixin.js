import { mapGetters } from 'vuex'

export default {
  computed: {
    ...mapGetters('clusterInfoModule', {
      clusterInfo: 'clusterInfog',
    }),
    clusterName() {
      return this.clusterInfo?.clusterName
    },
    ebsClusterList() {
      return this.clusterInfo.ebsClusterInfo || []
    },
    idcList() {
      return this.clusterInfo.clusterInfo.idc.split(',').map(item => ({ label: item, value: item }))
    }
  },
}
