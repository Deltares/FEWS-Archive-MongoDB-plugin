<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";
import JsonEditorVue from 'json-editor-vue'

const { result, loading, error, refetch } = useQuery(gql`query outputCubeN {outputCubeN {_id, Name, Bim}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result?.value?.outputCubeN ? result.value.outputCubeN.slice().sort((a, b) => a.Name.localeCompare(b.Name)) : [])

const createMutation = useMutation(gql`mutation createOutputCube($name: String!, $bim: JSON!) {createOutputCube(name: $name, bim: $bim)}`)
const updateMutation = useMutation(gql`mutation updateOutputCube($_id: ID!, $name: String!, $bim: JSON!) {updateOutputCube(_id: $_id, name: $name, bim: $bim)}`)
const deleteMutation = useMutation(gql`mutation deleteOutputCube($_id: ID!) {deleteOutputCube(_id: $_id)}`)

async function create() {
  const {Name, Bim} = selected.value
  const result = await mutate(() => createMutation.mutate({ name: Name, bim: JSON.parse(Bim) }))
  selected.value._id = result?.data ? result.data.createOutputCube : selected.value._id
}

async function update() {
  const {_id, Name, Bim} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, name: Name, bim: JSON.parse(Bim) }))
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>OutputCube Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Bim (JSON)</th></tr></thead>
    <tbody>
      <tr v-for="s in sorted" :key="s._id" :title="s._id">
        <td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s, Bim: JSON.stringify(s.Bim, null, 2)}" v-model="selected._id" /></td>
        <td><label :for="'r_'+s._id">{{s.Name}}</label></td>
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(s.Bim)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-bim" class="border rounded-lg pa-2 input-label">Bim</label><JsonEditorVue id="i-bim" mode="text" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" v-model="selected.Bim"/></div>
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