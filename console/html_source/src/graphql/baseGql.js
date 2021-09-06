import gql from 'graphql-tag' // 引入graphql
const baseGql = {
  login: gql`query Login($userID:String,$password:String) {
      login(userID:$userID,password:$password){
        token
        userID
      }
    }`,
  dashboard: gql`query Dashboard {
    Dashboard
  }`
}

export default baseGql
