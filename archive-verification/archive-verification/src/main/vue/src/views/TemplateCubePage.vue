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

const LIST = `query {templateCubeN {_id, Name, Template}}`
const CREATE = `mutation ($name: String!, $template: JSON!) {createTemplateCube(name: $name, template: $template)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $template: JSON!) {updateTemplateCube(_id: $_id, name: $name, template: $template)}`
const DELETE = `mutation ($_id: ID!) {deleteTemplateCube(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'templateCubeN')

const create = () =>
  run(async () => {
    const {Name, Template} = selected.value
    const data = await graphql(CREATE, {name: Name, template: JSON.parse(Template)})
    selected.value._id = data.createTemplateCube
    return data
  })

const update = () =>
  run(() => {
    const {_id, Name, Template} = selected.value
    return graphql(UPDATE, {_id, name: Name, template: JSON.parse(Template)})
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
    <PageHeader title="TemplateCube Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name" :on-select="(item) => ({...item, Template: JSON.stringify(item.Template, null, 2)})">
      <template #headers>
        <th class="w-100">Template (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item.Template)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow label="Template">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" v-model="selected.Template" mode="text" :class="fieldClass" />
        </template>
      </InputRow>
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
