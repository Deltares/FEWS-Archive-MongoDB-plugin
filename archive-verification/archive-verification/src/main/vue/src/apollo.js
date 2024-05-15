import { ApolloClient, HttpLink, InMemoryCache } from '@apollo/client/core'

const apolloClient = new ApolloClient({
  link: new HttpLink({uri: '/verification/graphql'}),
  connectToDevTools: false,
  cache: new InMemoryCache()
})

export default apolloClient