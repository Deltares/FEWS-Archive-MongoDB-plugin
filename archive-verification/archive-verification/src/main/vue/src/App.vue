<script setup>
import {ref, computed, onMounted, onUnmounted} from 'vue'
import {useRoute} from 'vue-router'
import {graphql} from '@/graphql'
import {links, groups} from '@/navigation'

const drawer = ref(true)
const rt = useRoute()
const route = computed(() => rt?.path)
const clock = ref([])
const user = ref(null)
const version = ref(null)
const computedUser = computed(() => user.value?.Email?.split('@')[0])
const computedVersion = computed(() => `Version: ${version.value?.Version}`)

const USER = `query {user {Name, Email}}`
const VERSION = `query {version {Version}}`

onMounted(async () => {
  user.value = (await graphql(USER)).user
  version.value = (await graphql(VERSION)).version
})

const timeZones = ['America/Chicago', 'America/New_York', 'GMT']
const dateFormat = {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: true,
  timeZoneName: 'short',
}

function updateClock() {
  const date = new Date()
  clock.value = timeZones.map((timeZone) => date.toLocaleString('en-US', {...dateFormat, timeZone}))
}

updateClock()
const clockTimer = setInterval(updateClock, 1000)
onUnmounted(() => clearInterval(clockTimer))
</script>

<template>
  <v-app>
    <v-app-bar color="#f8f8f8" density="compact" flat class="d-flex align-center border-b border-opacity-25">
      <v-app-bar-nav-icon @click="drawer = !drawer" />
      <v-app-bar-title><router-link class="link" to="/">TVA VERIFICATION</router-link></v-app-bar-title>
      <v-divider vertical />
      <div class="pa-4" :title="computedVersion"><v-icon>mdi-update</v-icon></div>
      <v-divider vertical />
      <div class="pa-4" title="Location"><v-icon>mdi-map-marker-outline</v-icon>{{ route }}</div>
      <v-divider vertical />
      <div class="pa-4" title="User"><v-icon>mdi-account-outline</v-icon>{{ computedUser }}</div>
      <v-divider vertical />
      <div class="pa-4" title="Clock"><v-icon>mdi-clock-outline</v-icon></div>
      <div class="pr-2" title="Clock" style="font-family: monospace; font-size: 10pt; line-height: 9pt">
        <div v-for="(time, i) in clock" :key="i">{{ time }}</div>
      </div>
    </v-app-bar>

    <v-navigation-drawer v-model="drawer" color="#f8f8f8" class="border-e border-opacity-50" rail>
      <v-list density="compact" nav>
        <v-list-item v-for="l in links" :key="l.to" :prepend-icon="l.icon" :to="l.to" :title.attr="l.label" />
        <v-divider></v-divider>
        <v-list-item v-for="g in groups" :key="g.label" :prepend-icon="g.icon" :title.attr="g.label">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item v-for="i in g.items" :key="i.to" :to="i.to">{{ i.label }}</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>

    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>
