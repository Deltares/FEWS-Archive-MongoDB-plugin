<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";

const { result, loading, error, refetch } = useQuery(gql`query templateDrdlYamlN {templateDrdlYamlN {_id, Database, Type, Name, Template}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result.value && result.value.templateDrdlYamlN ? result.value.templateDrdlYamlN.slice().sort((a, b) => `${a.Database}_${a.Type}_${a.Name}`.localeCompare(`${b.Database}_${b.Type}_${b.Name}`)) : [])

const createMutation = useMutation(gql`mutation createTemplateDrdlYaml($database: String!, $type: String!, $name: String!, $template: String!) {createTemplateDrdlYaml(database: $database, type: $type, name: $name, template: $template)}`)
const updateMutation = useMutation(gql`mutation updateTemplateDrdlYaml($_id: ID!, $database: String!, $type: String!, $name: String!, $template: String!) {updateTemplateDrdlYaml(_id: $_id, database: $database, type: $type, name: $name, template: $template)}`)
const deleteMutation = useMutation(gql`mutation deleteTemplateDrdlYaml($_id: ID!) {deleteTemplateDrdlYaml(_id: $_id)}`)

async function create() {
  const {Database, Type, Name, Template} = selected.value
  let name = Name || ""
  const result = await mutate(() => createMutation.mutate({ database: Database, type: Type, name: name, template: Template }))
  selected.value._id = result && result.data ? result.data.createTemplateDrdlYaml : selected.value._id
}

async function update() {
  const {_id, Database, Type, Name, Template} = selected.value
  let name = Name || ""
  await mutate(() => updateMutation.mutate({ _id: _id, database: Database, type: Type, name: name, template: Template }))
}

async function remove() {
  const {_id, Database, Type, Name} = selected.value
  if (confirm(`Remove ${Database}_${Type}_${Name} [${_id}]?`)) {
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h2>TemplateDrdlYaml Editor</h2></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="200px">
    <thead><tr>
      <th><v-icon>mdi-pencil-outline</v-icon></th>
      <th>Database</th>
      <th>Type</th>
      <th>Name</th>
      <th class="w-100">Template</th>
    </tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id">
      <td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td>
      <td><label :for="'r_'+s._id">{{s.Database}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Type}}</label></td>
      <td><label :for="'r_'+s._id">{{s.Name}}</label></td>
      <td><input type="text" class="w-100" readonly :value="s.Template.replace('\n', '\\n')"/></td>
    </tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Type}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-database" class="border rounded-lg pa-2 input-label">Database</label><input id="i-database" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Database"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-type" class="border rounded-lg pa-2 input-label">Type</label><input id="i-type" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Type"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-template" class="border rounded-lg pa-2 input-label">Template</label><textarea id="i-template" spellcheck="false" style="height: 400px; white-space: nowrap;" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" v-model="selected.Template"/></div>
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