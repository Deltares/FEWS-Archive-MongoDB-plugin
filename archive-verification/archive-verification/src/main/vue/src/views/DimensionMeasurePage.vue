<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {dimensionMeasureN {_id, measureId, measure, perfectScore}}`
const CREATE = `mutation ($measureId: String!, $measure: String!, $perfectScore: Int!) {createDimensionMeasure(measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`
const UPDATE = `mutation ($_id: ID!, $measureId: String!, $measure: String!, $perfectScore: Int!) {updateDimensionMeasure(_id: $_id, measureId: $measureId, measure: $measure, perfectScore: $perfectScore)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionMeasure(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'dimensionMeasureN', (a) => a.measureId)

const create = () =>
  run(async () => {
    const {measureId, measure, perfectScore} = selected.value
    const data = await graphql(CREATE, {measureId, measure, perfectScore})
    selected.value._id = data.createDimensionMeasure
    return data
  })

const update = () =>
  run(() => {
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
  <StatusBar :loading="loading" :error="error" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="DimensionMeasure Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="measureId" :label="(item) => item.measureId">
      <template #headers>
        <th class="w-100">Measure (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.measureId" />
    <div class="input">
      <InputRow v-model="selected.measureId" label="measureId" />
      <InputRow v-model="selected.measure" label="measure" />
      <InputRow v-model="selected.perfectScore" label="perfectScore" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
