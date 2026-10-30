<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {dimensionIsOriginalForecastN {_id, isOriginalForecast}}`
const CREATE = `mutation ($isOriginalForecast: Boolean!) {createDimensionIsOriginalForecast(isOriginalForecast: $isOriginalForecast)}`
const UPDATE = `mutation ($_id: ID!, $isOriginalForecast: Boolean!) {updateDimensionIsOriginalForecast(_id: $_id, isOriginalForecast: $isOriginalForecast)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionIsOriginalForecast(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'dimensionIsOriginalForecastN', (a) => `${a.isOriginalForecast}`)

const create = () =>
  run(async () => {
    const {isOriginalForecast} = selected.value
    const data = await graphql(CREATE, {isOriginalForecast})
    selected.value._id = data.createDimensionIsOriginalForecast
    return data
  })

const update = () =>
  run(() => {
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
  <StatusBar :loading="loading" :error="error" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="DimensionIsOriginalForecast Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="isOriginalForecast" :label="(item) => item.isOriginalForecast">
      <template #headers>
        <th class="w-100">IsOriginalForecast (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.isOriginalForecast" />
    <div class="input">
      <InputRow v-model="selected.isOriginalForecast" label="isOriginalForecast" type="checkbox" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
