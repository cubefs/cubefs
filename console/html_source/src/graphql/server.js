import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  queryExpandAll: gql`query{
    clusterView{
      name
      volumeCount
    }
    dataNodeList{ addr }
    metaNodeList{ addr }
    masterList{ addr }
  }`,
  queryMasterList: gql`query{
    masterList{ 
      addr
      isLeader 
    }
  }`,
  queryMetaList: gql`query{
    clusterView {
      name #cluster
      metaNodes {
        addr # serverIP
        status # status
        iD
        storeType
        toMetaNode {
          zoneName # idc
          metaPartitionCount
          used
          total
          ratio
          reportTime
          isActive
          storeType
        }
      }
    }
  }`,
  queryDataList: gql`query{
    clusterView {
      name #cluster
      dataNodes {
        addr # serverIP
        status # status
        iD
        toDataNode {
          zoneName # idc
          dataPartitionCount
          used
          total
          usageRatio
          reportTime
          isActive
        }
      }
    }
  }`,
  dataNodeGet: gql`query DataNodeGet($addr:String) {
    dataNodeGet(addr:$addr){
      dataPartitionReports{
        diskPath
        extentCount
        isLeader
        needCompare
        partitionID
        partitionStatus
        total
        used
        volName
      }
    }
  }`,
  metaNodeGet: gql`query MetaNodeGet($addr:String) {
    metaNodeGet(addr:$addr){
      metaPartitionInfos{
        end
        isLeader
        maxInodeID
        partitionID
        start
        status
        volName
        storeType
      }
    }
  }`,
  queryServerList: gql`query{
    clusterView {
      name #cluster
      dataNodes {
        addr # serverIP
        status # status
        iD
        toDataNode {
          zoneName # idc
          dataPartitionCount
          used
          total
          usageRatio
          reportTime
        }
      }
    }
  }`
}

export default baseGql
