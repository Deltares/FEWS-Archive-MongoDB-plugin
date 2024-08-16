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

app.config.errorHandler = (err, instance, info) => {
  localStorage.setItem('stack', err.stack)
  localStorage.setItem('message', err.message)
  localStorage.setItem('info', info)
  router.push({ name: 'Error' })
}

app.use(router).use(vuetify).mount('#app')
