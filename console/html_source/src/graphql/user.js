import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  createUser: gql`mutation CreateUser($accessKey:String, $iD:String, $password:String,$secretKey:String,$type:Uint8) {
      createUser(accessKey:$accessKey, iD:$iD, password:$password,secretKey:$secretKey,type:$type){
        user_id
      }
    }`
}

export default baseGql
