<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";
import JsonEditorVue from 'json-editor-vue'

const { result, loading, error, refetch } = useQuery(gql`query forecastN {forecastN {_id, Name, ForecastName, Collection, Filters}}`)
const selected = ref({})
const success = ref(null)
const warning = ref(null)
const sorted = computed(() => result?.value?.forecastN ? result.value.forecastN.slice().sort((a, b) => a.Name.localeCompare(b.Name)) : [])

const testQuery = useQuery(gql`query forecastTest($collection: String!, $filters: JSON!) {forecastTest(collection: $collection, filters: $filters){FilterName, Success}}`, {skip: true})
const createMutation = useMutation(gql`mutation createForecast($name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {createForecast(name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`)
const updateMutation = useMutation(gql`mutation updateForecast($_id: ID!, $name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {updateForecast(_id: $_id, name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`)
const deleteMutation = useMutation(gql`mutation deleteForecast($_id: ID!) {deleteForecast(_id: $_id)}`)

async function create() {
  const {Name, ForecastName, Collection, Filters} = selected.value
  const result = await mutate(() => createMutation.mutate({ name: Name, forecastName: ForecastName, collection: Collection, filters: JSON.parse(Filters) }))
  selected.value._id = result?.data ? result.data.createForecast : selected.value._id
}

async function update() {
  const {_id, Name, ForecastName, Collection, Filters} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, name: Name, forecastName: ForecastName, collection: Collection, filters: JSON.parse(Filters) }))
}

async function remove() {
  const {_id, Name} = selected.value
  if (confirm(`Remove ${Name} [${_id}]?`)) {
    await mutate(() => deleteMutation.mutate({_id: _id}))
    selected.value = {}
  }
}

async function test() {
  try {
    const {Collection, Filters} = selected.value
    loading.value = true
    error.value = warning.value = success.value = null
    const result = await testQuery.refetch({collection: Collection, filters: JSON.parse(Filters)})
    const message = result.data["forecastTest"].map(d => JSON.stringify(d)).join("<br>")
    const allSucceed = result.data["forecastTest"].every(d => d["Success"] === "true")
    if(allSucceed) {success.value = {'message': message}} else {warning.value = {'message': message}}
  }
  catch (e){
    error.value = e
  }
  finally {
    loading.value = false
  }
}

async function mutate(mutation){
  try {
    loading.value = true
    error.value = success.value = null
    const result = await mutation()
    await refetch()
    success.value = {'message': JSON.stringify(result.data)}
    return result
  }
  catch (e){
    error.value = e
  }
  finally {
    loading.value = false
  }
}
</script>

<template>
<v-overlay :model-value="!!loading" class="align-center justify-center"><v-progress-circular color="white" v-if="loading" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error.message }}</v-alert>
<v-alert type="warning" closable :model-value="!!warning">{{ warning.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>Forecast Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr>
      <th><v-icon>mdi-pencil-outline</v-icon></th>
      <th>Name</th>
      <th>ForecastName</th>
      <th>Collection</th>
      <th class="w-100">Filters (JSON)</th>
    </tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id">
      <td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s, Filters: JSON.stringify(s.Filters, null, 2)}" v-model="selected._id" /></td>
      <td><label :for="'r_'+s._id">{{s.Name}}</label></td>
      <td>{{s.ForecastName}}</td>
      <td>{{s.Collection}}</td>
      <td><input type="text" class="w-100" readonly :value="JSON.stringify(s.Filters)"/></td>
    </tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-forecastName" class="border rounded-lg pa-2 input-label">ForecastName</label><input id="i-forecastName" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.ForecastName"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-collection" class="border rounded-lg pa-2 input-label">Collection</label><input id="i-collection" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Collection"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-filters" class="border rounded-lg pa-2 input-label">Filters</label><JsonEditorVue id="i-filters" mode="text" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" v-model="selected.Filters"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
    <v-btn class="ml-2" @click="test">Test</v-btn>
  </div>
</div>
</template>

<style scoped>

</style>