import {createApp} from 'vue'
import App from './App.vue'
import router from './router'
import vuetify from './vuetify'
import './assets/main.css'

const app = createApp(App)

app.config.errorHandler = (err, instance, info) => {
  localStorage.setItem('stack', err.stack)
  localStorage.setItem('message', err.message)
  localStorage.setItem('info', info)
  router.push({name: 'Error'})
}

app.use(router).use(vuetify).mount('#app')
