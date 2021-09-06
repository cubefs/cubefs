import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  getUserInfo: gql`query getUserInfo ($userID:String) {
      getUserInfo (userID:$userID) {
        access_key
        secret_key
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
  queryFileList: gql`query listFile ($request:Object, $volName:String) {
      listFile (request:$request, volName:$volName) {
        infos {
          FSFileInfo {
            key
            value
          }
          eTag
          inode
          mIMEType
          mode
          modifyTime
          createTime
          path
          size
        }
        isTruncated
        nextMarker
        prefixes
      }
    }`,
  createFolder: gql`mutation createDir($path:String, $volName:String) {
      createDir(path:$path, volName:$volName){
        eTag
      }
    }`,
  signURL: gql`mutation signURL($path:String, $volName:String, $expireMinutes:Uint64) {
      signURL(path:$path, volName:$volName, expireMinutes:$expireMinutes){
        code
        message
      }
    }`,
  deleteDir: gql`mutation deleteDir($path:String, $volName:String) {
      deleteDir(path:$path, volName:$volName){
        code
        message
      }
    }`,
  deleteFile: gql`mutation deleteFile($path:String, $volName:String) {
      deleteFile(path:$path, volName:$volName){
        code
        message
      }
    }`
}

export default baseGql
