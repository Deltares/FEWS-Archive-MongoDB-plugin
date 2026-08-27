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

const LIST = `query {seasonalityN {_id, Name, Breakpoint}}`
const CREATE = `mutation ($name: String!, $breakpoint: JSON!) {createSeasonality(name: $name, breakpoint: $breakpoint)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $breakpoint: JSON!) {updateSeasonality(_id: $_id, name: $name, breakpoint: $breakpoint)}`
const DELETE = `mutation ($_id: ID!) {deleteSeasonality(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'seasonalityN')

const create = () =>
  run(async () => {
    const {Name, Breakpoint} = selected.value
    const data = await graphql(CREATE, {name: Name, breakpoint: JSON.parse(Breakpoint)})
    selected.value._id = data.createSeasonality
    return data
  })

const update = () =>
  run(() => {
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
  <StatusBar :loading="loading" :error="error" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="Seasonality Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name" :on-select="(item) => ({...item, Breakpoint: JSON.stringify(item.Breakpoint, null, 2)})">
      <template #headers>
        <th class="w-100">Breakpoint (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item.Breakpoint)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow label="Breakpoint">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" v-model="selected.Breakpoint" mode="text" :class="fieldClass" />
        </template>
      </InputRow>
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
