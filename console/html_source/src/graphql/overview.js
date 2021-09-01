import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  ClusterView: gql`query ClusterView {
    clusterView{
      leaderAddr
      volumeCount
      metaPartitionCount
      dataPartitionCount
      serverCount
      disableAutoAlloc
    }
  }`,
  topNUser: gql`query TopNUser($n:int32) {
    topNUser(n:$n){
      name
      size
      ratio
    }
  }`,
  dataNode: gql`query Query($queryTotal:String, $queryUsed:String,) {
    total:Query(query:$queryTotal)
    used:Query(query:$queryUsed)
  }`,
  rangeQuery: gql`query RangeQuery($queryTotal:String, $queryUsed:String, $start:uint32, $end:uint32,$step:uint32) {
    total:RangeQuery(query:$queryTotal, start:$start, end:$end,step:$step),
    used:RangeQuery(query:$queryUsed, start:$start, end:$end,step:$step)
  }`,
  rangeVolQuery: gql`query RangeQuery($query:String, $start:uint32, $end:uint32,$step:uint32) {
#    total:RangeQuery(query:$query, start:$start, end:$end,step:$step),
    RangeQuery(query:$query, start:$start, end:$end,step:$step)
  }`,
  queryUserList: gql`query ListUserInfo {
    listUserInfo{
      user_id
      user_type
      userStatistical{
        data
        dataPartitionCount
        metaPartitionCount
        volumeCount
      }
    }
  }`
}

export default baseGql
