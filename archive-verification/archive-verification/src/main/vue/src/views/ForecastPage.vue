<script setup>
import JsonEditorVue from 'json-editor-vue'
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {forecastN {_id, Name, ForecastName, Collection, Filters}}`
const TEST = `query ($collection: String!, $filters: JSON!) {forecastTest(collection: $collection, filters: $filters){FilterName, Success}}`
const CREATE = `mutation ($name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {createForecast(name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $forecastName: String!, $collection: String!, $filters: JSON!) {updateForecast(_id: $_id, name: $name, forecastName: $forecastName, collection: $collection, filters: $filters)}`
const DELETE = `mutation ($_id: ID!) {deleteForecast(_id: $_id)}`

const {sorted, selected, loading, error, success, warning, run} = useEditor(LIST, 'forecastN')

const create = () =>
  run(async () => {
    const {Name, ForecastName, Collection, Filters} = selected.value
    const data = await graphql(CREATE, {
      name: Name,
      forecastName: ForecastName,
      collection: Collection,
      filters: JSON.parse(Filters),
    })
    selected.value._id = data.createForecast
    return data
  })

const update = () =>
  run(() => {
    const {_id, Name, ForecastName, Collection, Filters} = selected.value
    return graphql(UPDATE, {
      _id,
      name: Name,
      forecastName: ForecastName,
      collection: Collection,
      filters: JSON.parse(Filters),
    })
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

async function test() {
  loading.value = true
  error.value = null
  success.value = null
  warning.value = null
  try {
    const {Collection, Filters} = selected.value
    const results = (await graphql(TEST, {collection: Collection, filters: JSON.parse(Filters)})).forecastTest
    const message = results.map((d) => JSON.stringify(d)).join('\n')
    if (results.every((d) => d.Success === 'true')) success.value = message
    else warning.value = message
  } catch (e) {
    error.value = e
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <StatusBar :loading="loading" :error="error" :warning="warning" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="Forecast Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name" :on-select="(item) => ({...item, Filters: JSON.stringify(item.Filters, null, 2)})">
      <template #headers>
        <th>ForecastName</th>
        <th>Collection</th>
        <th class="w-100">Filters (JSON)</th>
      </template>
      <template #cells="{item}">
        <td>{{ item.ForecastName }}</td>
        <td>{{ item.Collection }}</td>
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item.Filters)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-model="selected.ForecastName" label="ForecastName" />
      <InputRow v-model="selected.Collection" label="Collection" />
      <InputRow label="Filters">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" v-model="selected.Filters" mode="text" :class="fieldClass" />
        </template>
      </InputRow>
    </div>
    <EditorActions @create="create" @update="update" @remove="remove">
      <v-btn class="ml-2" @click="test">Test</v-btn>
    </EditorActions>
  </div>
</template>
