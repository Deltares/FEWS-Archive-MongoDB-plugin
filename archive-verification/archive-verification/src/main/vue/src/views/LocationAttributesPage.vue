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

const LIST = `query {locationAttributesN {_id, Name, Attributes}}`
const CREATE = `mutation ($name: String!, $attributes: JSON!) {createLocationAttributes(name: $name, attributes: $attributes)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $attributes: JSON!) {updateLocationAttributes(_id: $_id, name: $name, attributes: $attributes)}`
const DELETE = `mutation ($_id: ID!) {deleteLocationAttributes(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'locationAttributesN')

const create = () =>
  run(async () => {
    const {Name, Attributes} = selected.value
    const data = await graphql(CREATE, {name: Name, attributes: JSON.parse(Attributes)})
    selected.value._id = data.createLocationAttributes
    return data
  })

const update = () =>
  run(() => {
    const {_id, Name, Attributes} = selected.value
    return graphql(UPDATE, {_id, name: Name, attributes: JSON.parse(Attributes)})
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
    <PageHeader title="LocationAttributes Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name" :on-select="(item) => ({...item, Attributes: JSON.stringify(item.Attributes, null, 2)})">
      <template #headers>
        <th class="w-100">Attributes (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item.Attributes)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow label="Attributes">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" v-model="selected.Attributes" mode="text" :class="fieldClass" />
        </template>
      </InputRow>
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
