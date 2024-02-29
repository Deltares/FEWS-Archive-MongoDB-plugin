import { createApp, provide, h } from 'vue'
import App from './App.vue'
import router from './router'
import apolloClient from "./apollo"
import vuetify from './vuetify'
import { DefaultApolloClient } from '@vue/apollo-composable'

const app = createApp({
  setup () {
    provide(DefaultApolloClient, apolloClient)
  },
  render: () => h(App),
})

app.use(router).use(vuetify).mount('#app')
