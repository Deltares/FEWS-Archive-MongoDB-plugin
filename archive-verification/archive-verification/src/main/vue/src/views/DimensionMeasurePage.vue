<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";

const { result, loading, error, refetch } = useQuery(gql`query dimensionMeasureN {dimensionMeasureN {_id, measureId, measure, perfectScore}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result?.value?.dimensionMeasureN ? result.value.dimensionMeasureN.slice().sort((a, b) => a.measureId.localeCompare(b.measureId)) : [])

const createMutation = useMutation(gql`mutation createDimensionMeasure($measureId: String!, $measure: String!, $perfectScore: Int!) {createDimensionMeasure(measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`)
const updateMutation = useMutation(gql`mutation updateDimensionMeasure($_id: ID!, $measureId: String!, $measure: String!, $perfectScore: Int!) {updateDimensionMeasure(_id: $_id, measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`)
const deleteMutation = useMutation(gql`mutation deleteDimensionMeasure($_id: ID!) {deleteDimensionMeasure(_id: $_id)}`)

async function create() {
  const {measureId, measure, perfectScore} = selected.value
  const result = await mutate(() => createMutation.mutate({ measureId: measureId, measure: measure, perfectScore: perfectScore }))
  selected.value._id = result?.data ? result.data.createDimensionMeasure : selected.value._id
}

async function update() {
  const {_id, measureId, measure, perfectScore} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, measureId: measureId, measure: measure, perfectScore: perfectScore }))
}

async function remove() {
  const {_id, measureId} = selected.value
  if (confirm(`Remove ${measureId} [${_id}]?`)) {
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>DimensionMeasure Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>measureId</th><th class="w-100">Measure (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.measureId}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.environment}}</h4></div>
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

<style scoped>

</style>