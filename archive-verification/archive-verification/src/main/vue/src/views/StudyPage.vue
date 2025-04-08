<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";

const study = useQuery(gql`query studyN {studyN {_id, Name, Observed, Forecasts, Seasonalities, Class, LocationAttributes, ForecastStartMonth, ForecastEndMonth, Time, Value, Normal, Cube, Active, ReprocessDays, MaxLeadTimeMinutes}}`)
const observed = useQuery(gql`query observedN {observedN {_id, Name}}`)
const forecast = useQuery(gql`query forecastN {forecastN {_id, Name}}`)
const normal = useQuery(gql`query normalN {normalN {_id, Name}}`)
const seasonality = useQuery(gql`query seasonalityN {seasonalityN {_id, Name}}`)
const _class = useQuery(gql`query classN {classN {_id, Name}}`)
const locationAttributes = useQuery(gql`query locationAttributesN {locationAttributesN {_id, Name}}`)
const templateCube = useQuery(gql`query templateCubeN {templateCubeN {_id, Name}}`)

const selected = ref({})
const success = ref(null)
const mutateError = ref(null)
const mutateLoading = ref(null)
const loading = computed(() => study.loading.value || observed.loading.value || forecast.loading.value || normal.loading.value || seasonality.loading.value || _class.loading.value || locationAttributes.loading.value || templateCube.loading.value || mutateLoading.value)
const error = computed(() => study.error.value || observed.error.value || forecast.error.value || normal.error.value || seasonality.error.value || _class.error.value || locationAttributes.error.value || templateCube.error.value || mutateError.value)
const studySorted = computed(() => study.result.value && study.result.value.studyN ? study.result.value.studyN.slice().sort((a, b) => a.Name.localeCompare(b.Name)) : [])

const createMutation = useMutation(gql`mutation createStudy($name: String!, $observed: String!, $forecasts: [String!]!, $seasonalities: [String!]!, $_class: String!, $locationAttributes: String!, $forecastStartMonth: String!, $forecastEndMonth: String!, $time: String!, $value: String!, $normal: String!, $cube: String!, $active: Boolean!, $reprocessDays: Int!, $maxLeadTimeMinutes: Int!) {createStudy(name: $name, observed: $observed, forecasts: $forecasts, seasonalities: $seasonalities, _class: $_class, locationAttributes: $locationAttributes, forecastStartMonth: $forecastStartMonth, forecastEndMonth: $forecastEndMonth, time: $time, value: $value, normal: $normal, cube: $cube, active: $active, reprocessDays: $reprocessDays, maxLeadTimeMinutes: $maxLeadTimeMinutes)}`)
const updateMutation = useMutation(gql`mutation updateStudy($_id: ID!, $name: String!, $observed: String!, $forecasts: [String!]!, $seasonalities: [String!]!, $_class: String!, $locationAttributes: String!, $forecastStartMonth: String!, $forecastEndMonth: String!, $time: String!, $value: String!, $normal: String!, $cube: String!, $active: Boolean!, $reprocessDays: Int!, $maxLeadTimeMinutes: Int!) {updateStudy(_id: $_id, name: $name, observed: $observed, forecasts: $forecasts, seasonalities: $seasonalities, _class: $_class, locationAttributes: $locationAttributes, forecastStartMonth: $forecastStartMonth, forecastEndMonth: $forecastEndMonth, time: $time, value: $value, normal: $normal, cube: $cube, active: $active, reprocessDays: $reprocessDays, maxLeadTimeMinutes: $maxLeadTimeMinutes)}`)
const deleteMutation = useMutation(gql`mutation deleteStudy($_id: ID!) {deleteStudy(_id: $_id)}`)

async function create() {
  const {Name, Observed, Forecasts, Seasonalities, Class, LocationAttributes, ForecastStartMonth, ForecastEndMonth, Time, Value, Normal, Cube, Active, ReprocessDays, MaxLeadTimeMinutes} = selected.value
  const result = await mutate(() => createMutation.mutate({ name: Name, observed: Observed, forecasts: Forecasts, seasonalities: Seasonalities, _class: Class, locationAttributes: LocationAttributes, forecastStartMonth: ForecastStartMonth, forecastEndMonth: ForecastEndMonth, time: Time, value: Value, normal: Normal, cube: Cube, active: Active, reprocessDays: ReprocessDays, maxLeadTimeMinutes: MaxLeadTimeMinutes }))
  selected.value._id = result?.data ? result.data.createStudy : selected.value._id
}

async function update() {
  const {_id, Name, Observed, Forecasts, Seasonalities, Class, LocationAttributes, ForecastStartMonth, ForecastEndMonth, Time, Value, Normal, Cube, Active, ReprocessDays,  MaxLeadTimeMinutes} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, name: Name, observed: Observed, forecasts: Forecasts, seasonalities: Seasonalities, _class: Class, locationAttributes: LocationAttributes, forecastStartMonth: ForecastStartMonth, forecastEndMonth: ForecastEndMonth, time: Time, value: Value, normal: Normal, cube: Cube, active: Active, reprocessDays: ReprocessDays, maxLeadTimeMinutes: MaxLeadTimeMinutes }))
}

async function remove() {
  const {_id, Name} = selected.value
  if (confirm(`Remove ${Name} [${_id}]?`)) {
    await mutate(() => deleteMutation.mutate({_id: _id}))
    selected.value = {}
  }
}

async function refetchAll(){
  await Promise.all([
    study.refetch(),
    observed.refetch(),
    forecast.refetch(),
    normal.refetch(),
    seasonality.refetch(),
    _class.refetch(),
    locationAttributes.refetch(),
    templateCube.refetch()
  ])
}

async function mutate(mutation){
  try {
    mutateLoading.value = true
    mutateError.value = null
    success.value = null
    const result = await mutation()
    await refetchAll()
    success.value = {'message': JSON.stringify(result.data)}
    return result
  }
  catch (e){
    mutateError.value = e
  }
  finally {
    mutateLoading.value = false
  }
}
</script>

<template>
<v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>Study Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Study (JSON)</th></tr></thead>
    <tbody><tr v-for="s in studySorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.Name}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div v-if="observed.result.value" class="d-flex w-100 mt-2"><label for="i-observed" class="border rounded-lg pa-2 input-label">Observed</label><select id="i-observed" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Observed"><option v-for="x in observed.result.value.observedN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div v-if="forecast.result.value" class="d-flex w-100 mt-2"><label for="i-forecasts" class="border rounded-lg pa-2 input-label">Forecasts</label><select id="i-forecasts" multiple size="10" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Forecasts"><option v-for="x in forecast.result.value.forecastN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div v-if="seasonality.result.value" class="d-flex w-100 mt-2"><label for="i-seasonalities" class="border rounded-lg pa-2 input-label">Seasonalities</label><select id="i-seasonalities" multiple class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Seasonalities"><option v-for="x in seasonality.result.value.seasonalityN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div v-if="_class.result.value" class="d-flex w-100 mt-2"><label for="i-_class" class="border rounded-lg pa-2 input-label">Class</label><select id="i-_class" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Class"><option v-for="x in _class.result.value.classN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div v-if="locationAttributes.result.value" class="d-flex w-100 mt-2"><label for="i-locationAttributes" class="border rounded-lg pa-2 input-label">LocationAttributes</label><select id="i-locationAttributes" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.LocationAttributes"><option v-for="x in locationAttributes.result.value.locationAttributesN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecastStartMonth" class="border rounded-lg pa-2 input-label">ForecastStartMonth</label><input id="i-forecastStartMonth" type="month" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ForecastStartMonth"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecastEndMonth" class="border rounded-lg pa-2 input-label">ForecastEndMonth</label><input id="i-forecastEndMonth" type="month" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ForecastEndMonth"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-time" class="border rounded-lg pa-2 input-label">Time</label><select id="i-time" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Time"><option value="local" label="local"/><option value="UTC" label="UTC"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-value" class="border rounded-lg pa-2 input-label">Value</label><select id="i-value" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Value"><option value="display" label="display"/><option value="native" label="native"/></select></div>
    <div v-if="normal.result.value" class="d-flex w-100 mt-2"><label for="i-normal" class="border rounded-lg pa-2 input-label">Normal</label><select id="i-normal" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Normal"><option v-for="x in normal.result.value.normalN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div v-if="templateCube.result.value" class="d-flex w-100 mt-2"><label for="i-cube" class="border rounded-lg pa-2 input-label">Cube</label><select id="i-cube" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Cube"><option v-for="x in templateCube.result.value.templateCubeN" :key="x.Name" :value="x.Name" :label="x.Name"/></select></div>
    <div class="d-flex w-100 mt-2"><label for="i-active" class="border rounded-lg pa-2 input-label">Active</label><div id="i-active" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data"><input type="checkbox" class="font-weight-bold" v-model="selected.Active"/></div></div>
    <div class="d-flex w-100 mt-2"><label for="i-reprocessDays" class="border rounded-lg pa-2 input-label">ReprocessDays</label><input id="i-reprocessDays" type="number" min="1" max="999" step="1" pattern="\d+" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ReprocessDays"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-maxLeadTimeMinutes" class="border rounded-lg pa-2 input-label">MaxLeadTimeMinutes</label><input id="i-maxLeadTimeMinutes" type="number" min="0" max="527040" step="1" pattern="\d+" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.MaxLeadTimeMinutes"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>

<style scoped>

</style>