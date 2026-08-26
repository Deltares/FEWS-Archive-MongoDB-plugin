<script setup>
import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

const LIST = `query {dimensionMeasureN {_id, measureId, measure, perfectScore}}`
const CREATE = `mutation ($measureId: String!, $measure: String!, $perfectScore: Int!) {createDimensionMeasure(measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`
const UPDATE = `mutation ($_id: ID!, $measureId: String!, $measure: String!, $perfectScore: Int!) {updateDimensionMeasure(_id: $_id, measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionMeasure(_id: $_id)}`

const items = ref([])
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...items.value].sort((a, b) => a.measureId.localeCompare(b.measureId)))

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    items.value = (await graphql(LIST)).dimensionMeasureN
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
  const {measureId, measure, perfectScore} = selected.value
  const data = await graphql(CREATE, {measureId, measure, perfectScore})
  selected.value._id = data.createDimensionMeasure
  return data
})

const update = () => run(() => {
  const {_id, measureId, measure, perfectScore} = selected.value
  return graphql(UPDATE, {_id, measureId, measure, perfectScore})
})

const remove = () => {
  const {_id, measureId} = selected.value
  if (!confirm(`Remove ${measureId} [${_id}]?`)) return
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">DimensionMeasure Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>measureId</th><th class="w-100">Measure (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.measureId}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.measureId}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-measureId" class="border rounded-lg pa-2 input-label">measureId</label><input id="i-measureId" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.measureId"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-measure" class="border rounded-lg pa-2 input-label">measure</label><input id="i-measure" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.measure"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-perfectScore" class="border rounded-lg pa-2 input-label">perfectScore</label><input id="i-perfectScore" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.perfectScore"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
