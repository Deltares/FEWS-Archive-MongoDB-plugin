<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";
import JsonEditorVue from 'json-editor-vue'

const { result, loading, error, refetch } = useQuery(gql`query forecastN {forecastN {_id, Name, ForecastName, Collection, Filters}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result.value && result.value.forecastN ? result.value.forecastN.slice().sort((a, b) => a.Name.localeCompare(b.Name)) : [])

const createMutation = useMutation(gql`mutation createForecast($name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {createForecast(name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`)
const updateMutation = useMutation(gql`mutation updateForecast($_id: ID!, $name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {updateForecast(_id: $_id, name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`)
const deleteMutation = useMutation(gql`mutation deleteForecast($_id: ID!) {deleteForecast(_id: $_id)}`)

async function create() {
  const {Name, ForecastName, Collection, Filters} = selected.value
  const result = await mutate(() => createMutation.mutate({ name: Name, forecastName: ForecastName, collection: Collection, filters: Filters }))
  selected.value._id = result && result.data ? result.data.createForecast : selected.value._id
}

async function update() {
  const {_id, Name, ForecastName, Collection, Filters} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, name: Name, forecastName: ForecastName, collection: Collection, filters: Filters }))
}

async function remove() {
  const {_id, Name} = selected.value
  if (confirm(`Remove ${Name} [${_id}]?`)) {
    await mutate(() => deleteMutation.mutate({_id: _id}))
    selected.value = {}
  }
}

async function mutate(mutation){
  try {
    loading.value = true
    error.value = null
    success.value = null
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
<v-alert type="success" closable :model-value="!!success">{{ success.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h2>Forecast Editor</h2></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="200px">
    <thead><tr>
      <th><v-icon>mdi-pencil-outline</v-icon></th>
      <th>Name</th>
      <th>ForecastName</th>
      <th>Collection</th>
      <th class="w-100">Filters (JSON)</th>
    </tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id">
      <td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td>
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
    <div class="d-flex w-100 mt-2"><label for="i-filters" class="border rounded-lg pa-2 input-label">Filters</label><json-editor-vue id="i-filters" mode="text" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" @change="v => selected.Filters = JSON.parse(v.text)" v-model="selected.Filters"/></div>
  </div>
  <div class="mt-4">
    <v-btn variant="outlined" @click="create">Create</v-btn>
    <v-btn variant="outlined" class="ml-2" @click="update">Update</v-btn>
    <v-btn variant="outlined" class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>

<style scoped>

</style>