import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  clusterView: gql`query clusterView {
    clusterView {
      name
    }
  }`,
  masterList: gql`query masterList {
    masterList {
      addr
    }
  }`,
  queryVolumeList: gql`query listVolume ($keyword:String, $userID:String) {
    listVolume (keyword:$keyword, userID:$userID) {
      name
    }
  }`,
  rangeQuery: gql`query RangeQuery($query:String, $start:uint32, $end:uint32,$step:uint32){
    RangeQuery: RangeQuery(query:$query, start:$start, end:$end,step:$step)
  }`,
  query: gql`query Query($query:String) {
    Query: Query(query:$query)
  }`
}

export default baseGql
