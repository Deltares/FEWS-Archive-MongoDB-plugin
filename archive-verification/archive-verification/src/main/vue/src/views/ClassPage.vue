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

const LIST = `query {classN {_id, Name, Locations}}`
const CREATE = `mutation ($name: String!, $locations: JSON!) {createClass(name: $name, locations: $locations)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $locations: JSON!) {updateClass(_id: $_id, name: $name, locations: $locations)}`
const DELETE = `mutation ($_id: ID!) {deleteClass(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'classN')

const create = () =>
  run(async () => {
    const {Name, Locations} = selected.value
    const data = await graphql(CREATE, {name: Name, locations: JSON.parse(Locations)})
    selected.value._id = data.createClass
    return data
  })

const update = () =>
  run(() => {
    const {_id, Name, Locations} = selected.value
    return graphql(UPDATE, {_id, name: Name, locations: JSON.parse(Locations)})
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
    <PageHeader title="Class Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name" :on-select="(item) => ({...item, Locations: JSON.stringify(item.Locations, null, 2)})">
      <template #headers>
        <th class="w-100">Locations (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item.Locations)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow label="Locations">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" v-model="selected.Locations" mode="text" :class="fieldClass" />
        </template>
      </InputRow>
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
