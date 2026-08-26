<script setup>
import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

const LIST = `query {dimensionIsOriginalObservedN {_id, isOriginalObserved}}`
const CREATE = `mutation ($isOriginalObserved: Boolean!) {createDimensionIsOriginalObserved(isOriginalObserved: $isOriginalObserved)}`
const UPDATE = `mutation ($_id: ID!, $isOriginalObserved: Boolean!) {updateDimensionIsOriginalObserved(_id: $_id, isOriginalObserved: $isOriginalObserved)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionIsOriginalObserved(_id: $_id)}`

const items = ref([])
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...items.value].sort((a, b) => `${a.isOriginalObserved}`.localeCompare(`${b.isOriginalObserved}`)))

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    items.value = (await graphql(LIST)).dimensionIsOriginalObservedN
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
  const {isOriginalObserved} = selected.value
  const data = await graphql(CREATE, {isOriginalObserved})
  selected.value._id = data.createDimensionIsOriginalObserved
  return data
})

const update = () => run(() => {
  const {_id, isOriginalObserved} = selected.value
  return graphql(UPDATE, {_id, isOriginalObserved})
})

const remove = () => {
  const {_id, isOriginalObserved} = selected.value
  if (!confirm(`Remove ${isOriginalObserved} [${_id}]?`)) return
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">DimensionIsOriginalObserved Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>isOriginalObserved</th><th class="w-100">IsOriginalObserved (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.isOriginalObserved}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.isOriginalObserved}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-isOriginalObserved" class="border rounded-lg pa-2 input-label">isOriginalObserved</label><div id="i-isOriginalObserved" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data"><input type="checkbox" class="font-weight-bold" v-model="selected.isOriginalObserved"/></div></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
