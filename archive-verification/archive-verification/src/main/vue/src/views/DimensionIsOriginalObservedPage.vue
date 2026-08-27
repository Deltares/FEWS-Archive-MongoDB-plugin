<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {dimensionIsOriginalObservedN {_id, isOriginalObserved}}`
const CREATE = `mutation ($isOriginalObserved: Boolean!) {createDimensionIsOriginalObserved(isOriginalObserved: $isOriginalObserved)}`
const UPDATE = `mutation ($_id: ID!, $isOriginalObserved: Boolean!) {updateDimensionIsOriginalObserved(_id: $_id, isOriginalObserved: $isOriginalObserved)}`
const DELETE = `mutation ($_id: ID!) {deleteDimensionIsOriginalObserved(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'dimensionIsOriginalObservedN', (a) => `${a.isOriginalObserved}`)

const create = () =>
  run(async () => {
    const {isOriginalObserved} = selected.value
    const data = await graphql(CREATE, {isOriginalObserved})
    selected.value._id = data.createDimensionIsOriginalObserved
    return data
  })

const update = () =>
  run(() => {
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
  <StatusBar :loading="loading" :error="error" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="DimensionIsOriginalObserved Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="isOriginalObserved" :label="(item) => item.isOriginalObserved">
      <template #headers>
        <th class="w-100">IsOriginalObserved (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.isOriginalObserved" />
    <div class="input">
      <InputRow v-model="selected.isOriginalObserved" label="isOriginalObserved" type="checkbox" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
