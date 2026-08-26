<script setup>
import {ref, computed, onMounted} from 'vue'
import JsonEditorVue from 'json-editor-vue'
import {graphql} from '@/graphql'

const LIST = `query {seasonalityN {_id, Name, Breakpoint}}`
const CREATE = `mutation ($name: String!, $breakpoint: JSON!) {createSeasonality(name: $name, breakpoint: $breakpoint)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $breakpoint: JSON!) {updateSeasonality(_id: $_id, name: $name, breakpoint: $breakpoint)}`
const DELETE = `mutation ($_id: ID!) {deleteSeasonality(_id: $_id)}`

const items = ref([])
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...items.value].sort((a, b) => a.Name.localeCompare(b.Name)))

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    items.value = (await graphql(LIST)).seasonalityN
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
  const {Name, Breakpoint} = selected.value
  const data = await graphql(CREATE, {name: Name, breakpoint: JSON.parse(Breakpoint)})
  selected.value._id = data.createSeasonality
  return data
})

const update = () => run(() => {
  const {_id, Name, Breakpoint} = selected.value
  return graphql(UPDATE, {_id, name: Name, breakpoint: JSON.parse(Breakpoint)})
})

const remove = () => {
  const {_id, Name} = selected.value
  if (!confirm(`Remove ${Name} [${_id}]?`)) return
  return run(async () => {
    const data = await graphql(DELETE, {_id})
    selected.value = {}
    return data
  })
}
</script>

<template>
<v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error?.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">Seasonality Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Breakpoint (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s, Breakpoint: JSON.stringify(s.Breakpoint, null, 2)}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.Name}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s.Breakpoint)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-breakpoint" class="border rounded-lg pa-2 input-label">Breakpoint</label><JsonEditorVue id="i-breakpoint" mode="text" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" v-model="selected.Breakpoint"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
