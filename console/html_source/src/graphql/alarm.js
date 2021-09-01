import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  queryAlarmList: gql`query AlarmList($size:int32) {
    alarmList(size:$size){
      detail
      hostname
      key
      time
      type
      value
    }
  }`,
  clusterView: gql`query clusterView{
    clusterView{
        name
      }
    }`
}

export default baseGql
