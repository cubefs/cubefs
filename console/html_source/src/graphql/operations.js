import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  queryServerList: gql`query dataNodeListTest($num:int64) {
    dataNodeListTest(num:$num){
      addr
      availableSpace
      #      badDisks
      carry
      dataPartitionCount
      #      dataPartitionReports
      iD
      nodeSetID
      #      persistenceDataPartitions
      reportTime
      selectedTimes
      total
      usageRatio
      used
      zoneName
    }
  }`,
  serverManagementList: gql`query clusterView {
    clusterView {
      name
      dataNodes {
        addr
        reportDisks
        status
        isWritable
        toDataNode {
          zoneName
          used
          total
          usageRatio
          badDisks
        }
      },
      metaNodes {
        addr
        status
        isWritable
        storeType
        toMetaNode {
          zoneName
          used
          total
          ratio
        }
      }
    }
  }`,
  queryVolumeList: gql`query listVolume ($keyword:String, $userID:String) {
      listVolume (keyword:$keyword, userID:$userID) {
        name
        capacity
        occupied
        dpReplicaNum
        owner
        createTime
        oSSAccessKey
        oSSSecretKey
        inodeCount
        toSimpleVolView {
          description
          dpCnt
          mpCnt
          rwDpCnt
        }
        status
      }
    }`,
  createVolume: gql`mutation createVolume($authenticate:Bool, $capacity:Uint64, $crossZone:Bool, $dataPartitionSize:Uint64, $dpReplicaNum:Uint64, $enableToken:Bool, $followerRead:Bool, $mpCount:Uint64, $name:String, $owner:String, $zoneName:String, $description:String) {
      createVolume(authenticate:$authenticate, capacity:$capacity, crossZone:$crossZone, dataPartitionSize:$dataPartitionSize, dpReplicaNum:$dpReplicaNum, enableToken:$enableToken, followerRead:$followerRead, mpCount:$mpCount, name:$name, owner:$owner, zoneName:$zoneName, description:$description){
        name
      }
    }`,
  updateVolume: gql`mutation updateVolume($authKey:String, $capacity:Uint64, $enableToken:Bool, $name:String, $replicaNum:Uint64, $zoneName:String, $description:String) {
      updateVolume(authKey:$authKey, capacity:$capacity, enableToken:$enableToken, name:$name, replicaNum:$replicaNum, zoneName:$zoneName, description:$description){
        name
        capacity
      }
    }`,
  deleteVolume: gql`mutation deleteVolume($authKey:String, $name:String) {
      deleteVolume(authKey:$authKey, name:$name){
        code
        message
      }
    }`,
  queryPermissionList: gql`query volPermission ($volName:String, $userID:String) {
      volPermission (volName:$volName, userID:$userID) {
        userID,
        access
      }
    }`,
  grantPermission: gql`mutation updateUserPolicy($policy:Array, $userID:String, $volume:String) {
      updateUserPolicy(policy:$policy, userID:$userID, volume:$volume){
        user_id
      }
    }`,
  deletePermission: gql`mutation removeUserPolicy($userID:String, $volume:String) {
      removeUserPolicy(userID:$userID, volume:$volume){
        user_id
      }
    }`,
  decommissionDataNode: gql`mutation decommissionDataNode($offLineAddr:String) {
      decommissionDataNode(offLineAddr:$offLineAddr){
        code
        message
      }
    }`,
  decommissionMetaNode: gql`mutation decommissionMetaNode($offLineAddr:String) {
      decommissionMetaNode(offLineAddr:$offLineAddr){
        code
        message
      }
    }`,
  decommissionDisk: gql`mutation decommissionDisk($offLineAddr:String, $diskPath:String) {
      decommissionDisk(offLineAddr:$offLineAddr, diskPath:$diskPath){
        code
        message
      }
    }`,
  queryMetaPartitionList: gql`query metaPartitionList {
    metaPartitionList{
      partitionID
      dentryCount
      start
      end
      hosts
      inodeCount
      isRecover
      maxInodeID
      partitionID
      peers{
        iD
        addr
      }
      replicaNum
      status
      missNodes
      storeType
    }
  }`,
  queryDataPartitionList: gql`query dataPartitionList {
    dataPartitionList{
      partitionID
      hosts
      peers{
        iD
        addr
      }
      replicaNum
      lastLoadedTime
      status
      missNodes
      volName
    }
  }`,
  queryMetaNodeAddrList: gql`query clusterView {
    clusterView{
      metaNodes{
        addr
        isWritable
        status
        toMetaNode{
          metaPartitionCount
          total
          used
        }
      }
    }
  }`,

  queryDataNodeAddrList: gql`query clusterView {
    clusterView{
      dataNodes{
        addr
        isWritable
        status
        toDataNode{
          dataPartitionCount
          total
          used
        }
      }
    }
  }`,
  addMetaReplica: gql`mutation addMetaReplica($addr:String, $partitionID:int64) {
    addMetaReplica(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
  deleteMetaReplica: gql`mutation deleteMetaReplica($addr:String, $partitionID:int64) {
    deleteMetaReplica(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
  decommissionMetaPartition: gql`mutation decommissionMetaPartition($addr:String, $partitionID:int64) {
    decommissionMetaPartition(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
  addDataReplica: gql`mutation addDataReplica($addr:String, $partitionID:int64) {
    addDataReplica(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
  deleteDataReplica: gql`mutation deleteDataReplica($addr:String, $partitionID:int64) {
    deleteDataReplica(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
  decommissionDataPartition: gql`mutation decommissionDataPartition($addr:String, $partitionID:int64) {
    decommissionDataPartition(addr:$addr, partitionID:$partitionID){
      code
      message
    }
  }`,
}

export default baseGql
