import {ApolloClient} from 'apollo-client'
import {HttpLink} from 'apollo-link-http'
import {InMemoryCache} from 'apollo-cache-inmemory'
import {ApolloLink} from 'apollo-link'

export default {
  query (uri, gql, variables, config) {
    const apiLink = new HttpLink({
      uri: uri, // 请求路径
      credentials: 'include'
    })
    const middlewareLink = new ApolloLink((operation, forward) => {
      const token = sessionStorage.getItem('access_token')
      operation.setContext({
        headers: {
          authorization: `${token}` || null
        }
      })
      return forward(operation)
    })
    const apolloClient = new ApolloClient({
      link: middlewareLink.concat(apiLink), // 如果不添加请求头直接放路径
      cache: new InMemoryCache()
    })
    return apolloClient.query({
      query: gql,
      variables: variables
    }).then((reponse) => {
      return reponse
    }).catch((err) => {
      if (err.graphQLErrors[0]) {
        const errJson = JSON.parse(err.graphQLErrors[0])
        if (errJson.code === 407) {
          sessionStorage.setItem('returnUrl', window.location.href)
          window.location.href = window.location.origin + '/login'
          // window.location.href = window.location.origin + '/login?returnUrl=' + window.location.href.replace(window.location.origin, '')
        } else {
          return errJson
        }
      }
      return err
      // return err
    })
  },
  mutation (uri, gql, variables, config) {
    const apiLink = new HttpLink({
      uri: uri, // 请求路径
      credentials: 'same-origin'
    })
    const middlewareLink = new ApolloLink((operation, forward) => {
      const token = sessionStorage.getItem('access_token')
      operation.setContext({
        headers: {
          authorization: `${token}` || null
        }
      })
      return forward(operation)
    })
    const apolloClient = new ApolloClient({
      link: middlewareLink.concat(apiLink), // 如果不添加请求头直接放路径
      cache: new InMemoryCache()
    })
    return apolloClient.mutate({
      mutation: gql,
      variables: variables
    }).then((reponse) => {
      return reponse
    }).catch((err) => {
      if (err.graphQLErrors[0]) {
        const errJson = JSON.parse(err.graphQLErrors[0])
        if (errJson.code === 401 || errJson.code === 407) {
          sessionStorage.setItem('returnUrl', window.location.href)
          window.location.href = window.location.origin + '/login'
          // window.location.href = window.location.origin + '/login?returnUrl=' + window.location.href.replace(window.location.origin, '')
        } else {
          return errJson
        }
      }
      return err
    })
  }
}
