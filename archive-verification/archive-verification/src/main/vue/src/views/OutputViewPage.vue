<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";
import JsonEditorVue from "json-editor-vue";

const { result, loading, error, refetch } = useQuery(gql`query outputViewN {outputViewN {_id, Database, State, View, Collection, Name, Environment, Study, Value}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result?.value?.outputViewN ? result.value.outputViewN.slice().sort((a, b) => `${a.Database}_${a.State}_${a.View}`.localeCompare(`${b.Database}_${b.State}_${b.View}`)) : [])

const createMutation = useMutation(gql`mutation createOutputView($database: String!, $state: String!, $view: String!, $collection: String!, $name: String!, $environment: String!, $study: String!, $value: JSON!) {createOutputView(database: $database, state: $state, view: $view, collection: $collection, name: $name, environment: $environment, study: $study, value: $value)}`)
const updateMutation = useMutation(gql`mutation updateOutputView($_id: ID!, $database: String!, $state: String!, $view: String!, $collection: String!, $name: String!, $environment: String!, $study: String!, $value: JSON!) {updateOutputView(_id: $_id, database: $database, state: $state, view: $view, collection: $collection, name: $name, environment: $environment, study: $study, value: $value)}`)
const deleteMutation = useMutation(gql`mutation deleteOutputView($_id: ID!) {deleteOutputView(_id: $_id)}`)

async function create() {
  const {Database, State, View, Collection, Name, Environment, Study, Value} = selected.value
  const result = await mutate(() => createMutation.mutate({ database: Database, state: State, view: View, collection: Collection, name: Name, environment: Environment, study: Study, value: JSON.parse(Value) }))
  selected.value._id = result?.data ? result.data.createOutputView : selected.value._id
}

async function update() {
  const {_id, Database, State, View, Collection, Name, Environment, Study, Value} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, database: Database, state: State, view: View, collection: Collection, name: Name, environment: Environment, study: Study, value: JSON.parse(Value) }))
}

async function remove() {
  const {_id, Database, State, View} = selected.value
  if (confirm(`Remove ${Database}_${State}_${View} [${_id}]?`)) {
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>OutputView Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr>
      <th><v-icon>mdi-pencil-outline</v-icon></th>
      <th>Database</th>
      <th>State</th>
      <th>View</th>
      <th>Collection</th>
      <th>Name</th>
      <th>Environment</th>
      <th>Study</th>
      <th class="w-100">Value (JSON)</th>
    </tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id">
      <td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s, Value: JSON.stringify(s.Value, null, 2)}" v-model="selected._id" /></td>
      <td><label :for="'r_'+s._id">{{s.Database}}</label></td>
      <td><label :for="'r_'+s._id">{{s.State}}</label></td>
      <td><label :for="'r_'+s._id">{{s.View}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Collection}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Name}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Environment}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Study}}</label></td>
      <td><input type="text" class="w-100" readonly /></td>
<!--      <td><input type="text" class="w-100" readonly :value="JSON.stringify(s.Bim)"/></td>-->
    </tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-database" class="border rounded-lg pa-2 input-label">Database</label><input id="i-database" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Database"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-state" class="border rounded-lg pa-2 input-label">State</label><input id="i-state" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.State"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-view" class="border rounded-lg pa-2 input-label">View</label><input id="i-view" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.View"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-collection" class="border rounded-lg pa-2 input-label">Collection</label><input id="i-collection" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Collection"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-environment" class="border rounded-lg pa-2 input-label">Environment</label><input id="i-environment" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Environment"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-study" class="border rounded-lg pa-2 input-label">Study</label><input id="i-study" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Study"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-value" class="border rounded-lg pa-2 input-label">Value</label><JsonEditorVue id="i-value" mode="text" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" v-model="selected.Value"/></div>
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