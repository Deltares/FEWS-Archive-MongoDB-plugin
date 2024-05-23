<script setup>
import {ref, computed} from "vue"
import {useRoute} from "vue-router"
import {useQuery} from "@vue/apollo-composable";
import gql from "graphql-tag";

const drawer = ref(true)
const rt = useRoute()
const route = computed(() => rt?.path)
const clock = ref(null)
const { result: user } = useQuery(gql`query user {user {Name, Email}}`)
const { result: version } = useQuery(gql`query version {version {Version}}`)
const computedUser = computed(() => user?.value?.user?.Email?.split('@')[0])
const computedVersion = computed(() => `Version: ${version?.value?.version?.Version}`)

const dateFormat = { year: 'numeric', month: '2-digit',day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true, timeZoneName: 'short' }
let clockTimeout = setTimeout(updateClock, 100)
function updateClock() {
  let date = new Date()
  clock.value = [
    `${date.toLocaleDateString('en-US', {...dateFormat, timeZone: 'America/Chicago'})}`,
    `${date.toLocaleDateString('en-US', {...dateFormat, timeZone: 'America/New_York'})}`,
    `${date.toLocaleDateString('en-US', {...dateFormat, timeZone: 'GMT'})}`].join('<br/>')
  clearTimeout(clockTimeout)
  clockTimeout = setTimeout(updateClock, 100)
}
</script>

<template>
  <v-app>
    <v-app-bar color="#f8f8f8" density="compact" flat class="d-flex align-center border-b border-opacity-25">
      <v-app-bar-nav-icon @click="drawer = !drawer"/>
      <v-app-bar-title><router-link class="text-h6 link" to="/">TVA VERIFICATION</router-link></v-app-bar-title>
      <v-divider vertical/>
      <div class="pa-4" :title="computedVersion"><v-icon>mdi-update</v-icon></div><v-divider vertical/>
      <div class="pa-4" title="Location"><v-icon>mdi-map-marker-outline</v-icon>{{route}}</div><v-divider vertical/>
      <div class="pa-4" title="User"><v-icon>mdi-account-outline</v-icon>{{computedUser}}</div><v-divider vertical/>
      <div class="pa-4" title="Clock"><v-icon>mdi-clock-outline</v-icon></div>
      <div class="pr-2" title="Clock" style="font-family: monospace; font-size: 10pt; line-height: 9pt" v-html="clock"></div>
    </v-app-bar>

    <v-navigation-drawer color="#f8f8f8" class="border-r border-opacity-50" rail v-model="drawer">
      <v-list density="compact" nav>
        <v-list-item prepend-icon="mdi-home-outline" to="/" :title.attr="'Home'"/>
        <v-list-item prepend-icon="mdi-cog-outline" to="/configurationSettings" :title.attr="'Settings'"/>
        <v-list-item prepend-icon="mdi-cogs" to="/configurationDescription" :title.attr="'Description'"/>
        <v-divider></v-divider>
        <v-list-item prepend-icon="mdi-check-all" :title.attr="'Verification'">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item to="/class">Class</v-list-item>
              <v-list-item to="/forecast">Forecast</v-list-item>
              <v-list-item to="/locationAttributes">LocationAttributes</v-list-item>
              <v-list-item to="/normal">Normal</v-list-item>
              <v-list-item to="/observed">Observed</v-list-item>
              <v-list-item to="/seasonality">Seasonality</v-list-item>
              <v-list-item to="/study">Study</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
        <v-list-item prepend-icon="mdi-waves" :title.attr="'Fews'">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item to="/fewsLocations">Locations</v-list-item>
              <v-list-item to="/fewsParameters">Parameters</v-list-item>
              <v-list-item to="/fewsQualifiers">Qualifiers</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
        <v-list-item prepend-icon="mdi-export" :title.attr="'Output'">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item to="/outputPowerQuery">PowerQuery</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
        <v-list-item prepend-icon="mdi-artboard" :title.attr="'Template'">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item to="/templateCube">Cube</v-list-item>
              <v-list-item to="/templateDrdlYaml">DrdlYaml</v-list-item>
              <v-list-item to="/templatePowerQuery">PowerQuery</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
        <v-list-item prepend-icon="mdi-axis-arrow" :title.attr="'Dimension'">
          <v-menu activator="parent" location="left" open-on-hover>
            <v-list density="compact">
              <v-list-item to="/dimensionIsOriginalForecast">IsOriginalForecast</v-list-item>
              <v-list-item to="/dimensionIsOriginalObserved">IsOriginalObserved</v-list-item>
              <v-list-item to="/dimensionMeasure">Measure</v-list-item>
            </v-list>
          </v-menu>
        </v-list-item>
      </v-list>
    </v-navigation-drawer>

    <v-main>
      <router-view/>
    </v-main>
  </v-app>
</template>

<style scoped>

</style>

<style>

.link{
  color: #000000;
  text-decoration: none;
}

.link:hover{
  color: #444444;
}

.v-table tbody tr:nth-child(odd){
  background-color: #eeeeee;
}

.v-table th{
  background-color: #888888 !important;
  color: white !important;
}

.v-table td{
  white-space: nowrap;
}

.input div:nth-child(odd) .input-label{
  background-color: #eeeeee;
}

.input div:nth-child(even) .input-label{
  background-color: #dddddd;
}

.input div:hover{
  .input-label{
    background-color: #666666;
    color: #ffffff;
  }
  .input-data{
    background-color: #fafafa;
  }
}

.input-label{
  text-overflow: ellipsis;
  overflow: hidden;
  flex: 0 0 250px;
  text-align: right;
}

.input-data{
  background-color: #afc8e1;
  max-height: 400px;
  overflow: auto;
}
</style>