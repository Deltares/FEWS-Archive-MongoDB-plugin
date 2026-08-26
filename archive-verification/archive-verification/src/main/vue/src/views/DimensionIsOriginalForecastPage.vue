<script setup>
import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

const LIST = `query {dimensionIsOriginalForecastN {_id, isOriginalForecast}}`
const CREATE = `mutation ($isOriginalForecast: Boolean!) {createDimensionIsOriginalForecast(isOriginalForecast: $isOriginalForecast)}`
const UPDATE = `mutation ($_id: ID!, $isOriginalForecast: Boolean!) {updateDimensionIsOriginalForecast(_id: $_id, isOriginalForecast: $isOriginalForecast)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionIsOriginalForecast(_id: $_id)}`

const items = ref([])
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...items.value].sort((a, b) => `${a.isOriginalForecast}`.localeCompare(`${b.isOriginalForecast}`)))

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    items.value = (await graphql(LIST)).dimensionIsOriginalForecastN
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
  const {isOriginalForecast} = selected.value
  const data = await graphql(CREATE, {isOriginalForecast})
  selected.value._id = data.createDimensionIsOriginalForecast
  return data
})

const update = () => run(() => {
  const {_id, isOriginalForecast} = selected.value
  return graphql(UPDATE, {_id, isOriginalForecast})
})

const remove = () => {
  const {_id, isOriginalForecast} = selected.value
  if (!confirm(`Remove ${isOriginalForecast} [${_id}]?`)) return
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">DimensionIsOriginalForecast Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>isOriginalForecast</th><th class="w-100">IsOriginalForecast (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.isOriginalForecast}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.isOriginalForecast}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-isOriginalForecast" class="border rounded-lg pa-2 input-label">isOriginalForecast</label><div id="i-isOriginalForecast" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data"><input type="checkbox" class="font-weight-bold" v-model="selected.isOriginalForecast"/></div></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
