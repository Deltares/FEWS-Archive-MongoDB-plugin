<script setup>
import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

// One document selects the study list plus every dropdown's options, so the page
// loads (and reloads after a mutation) in a single round trip.
const LIST = `query {
  studyN {_id, Name, Observed, Forecasts, Seasonalities, Class, LocationAttributes, ForecastStartMonth, ForecastEndMonth, Time, Value, Normal, Cube, Active, ReprocessDays, MaxLeadTimeMinutes}
  observedN {_id, Name}
  forecastN {_id, Name}
  normalN {_id, Name}
  seasonalityN {_id, Name}
  classN {_id, Name}
  locationAttributesN {_id, Name}
  templateCubeN {_id, Name}
}`
const ARGS = `$name: String!, $observed: String!, $forecasts: [String!]!, $seasonalities: [String!]!, $_class: String!, $locationAttributes: String!, $forecastStartMonth: String!, $forecastEndMonth: String!, $time: String!, $value: String!, $normal: String!, $cube: String!, $active: Boolean!, $reprocessDays: Int!, $maxLeadTimeMinutes: Int!`
const CALL = `name: $name, observed: $observed, forecasts: $forecasts, seasonalities: $seasonalities, _class: $_class, locationAttributes: $locationAttributes, forecastStartMonth: $forecastStartMonth, forecastEndMonth: $forecastEndMonth, time: $time, value: $value, normal: $normal, cube: $cube, active: $active, reprocessDays: $reprocessDays, maxLeadTimeMinutes: $maxLeadTimeMinutes`
const CREATE = `mutation (${ARGS}) {createStudy(${CALL})}`
const UPDATE = `mutation ($_id: ID!, ${ARGS}) {updateStudy(_id: $_id, ${CALL})}`
const DELETE = `mutation ($_id: ID!) {deleteStudy(_id: $_id)}`

const data = ref({})
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...data.value.studyN ?? []].sort((a, b) => a.Name.localeCompare(b.Name)))

const variables = () => {
  const s = selected.value
  return {name: s.Name, observed: s.Observed, forecasts: s.Forecasts, seasonalities: s.Seasonalities, _class: s.Class, locationAttributes: s.LocationAttributes, forecastStartMonth: s.ForecastStartMonth, forecastEndMonth: s.ForecastEndMonth, time: s.Time, value: s.Value, normal: s.Normal, cube: s.Cube, active: s.Active, reprocessDays: s.ReprocessDays, maxLeadTimeMinutes: s.MaxLeadTimeMinutes}
}

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    data.value = await graphql(LIST)
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
  const result = await graphql(CREATE, variables())
  selected.value._id = result.createStudy
  return result
})

const update = () => run(() => graphql(UPDATE, {_id: selected.value._id, ...variables()}))

const remove = () => {
  const {_id, Name} = selected.value
  if (!confirm(`Remove ${Name} [${_id}]?`)) return
  return run(async () => {
    const result = await graphql(DELETE, {_id})
    selected.value = {}
    return result
  })
}
</script>

<template>
<v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error?.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">Study Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Study (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.Name}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-observed" class="border rounded-lg pa-2 input-label">Observed</label><select id="i-observed" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Observed"><option v-for="x in data.observedN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecasts" class="border rounded-lg pa-2 input-label">Forecasts</label><select id="i-forecasts" multiple size="10" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Forecasts"><option v-for="x in data.forecastN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-seasonalities" class="border rounded-lg pa-2 input-label">Seasonalities</label><select id="i-seasonalities" multiple class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Seasonalities"><option v-for="x in data.seasonalityN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-_class" class="border rounded-lg pa-2 input-label">Class</label><select id="i-_class" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Class"><option v-for="x in data.classN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-locationAttributes" class="border rounded-lg pa-2 input-label">LocationAttributes</label><select id="i-locationAttributes" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.LocationAttributes"><option v-for="x in data.locationAttributesN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecastStartMonth" class="border rounded-lg pa-2 input-label">ForecastStartMonth</label><input id="i-forecastStartMonth" type="month" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ForecastStartMonth"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecastEndMonth" class="border rounded-lg pa-2 input-label">ForecastEndMonth</label><input id="i-forecastEndMonth" type="month" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ForecastEndMonth"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-time" class="border rounded-lg pa-2 input-label">Time</label><select id="i-time" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Time"><option value="local" label="local"/><option value="UTC" label="UTC"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-value" class="border rounded-lg pa-2 input-label">Value</label><select id="i-value" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Value"><option value="display" label="display"/><option value="native" label="native"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-normal" class="border rounded-lg pa-2 input-label">Normal</label><select id="i-normal" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Normal"><option v-for="x in data.normalN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-cube" class="border rounded-lg pa-2 input-label">Cube</label><select id="i-cube" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Cube"><option v-for="x in data.templateCubeN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-active" class="border rounded-lg pa-2 input-label">Active</label><div id="i-active" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data"><input type="checkbox" class="font-weight-bold" v-model="selected.Active"/></div></div>
    <div class="d-flex w-100 mt-2"><label for="i-reprocessDays" class="border rounded-lg pa-2 input-label">ReprocessDays</label><input id="i-reprocessDays" type="number" min="1" max="999" step="1" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ReprocessDays"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-maxLeadTimeMinutes" class="border rounded-lg pa-2 input-label">MaxLeadTimeMinutes</label><input id="i-maxLeadTimeMinutes" type="number" min="0" max="527040" step="1" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.MaxLeadTimeMinutes"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
