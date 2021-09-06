import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  queryUserList: gql`query ListUserInfo {
      listUserInfo{
        user_id
        user_type
        description
      }
    }`,
  createUser: gql`mutation CreateUser($accessKey:String, $iD:String, $password:String,$secretKey:String,$type:Uint8, $description:String) {
      createUser(accessKey:$accessKey, iD:$iD, password:$password,secretKey:$secretKey,type:$type, description:$description){
        user_id
      }
    }`,
  updateUser: gql`mutation updateUser($accessKey:String, $userID:String, $secretKey:String, $type:Uint8, $description:String, $password:String) {
      updateUser(accessKey:$accessKey, userID:$userID, secretKey:$secretKey, type:$type, description:$description, password:$password){
        user_id
      }
    }`,
  deleteUser: gql`mutation deleteUser($userID:String) {
      deleteUser(userID:$userID){
        code
      }
    }`
}

export default baseGql
