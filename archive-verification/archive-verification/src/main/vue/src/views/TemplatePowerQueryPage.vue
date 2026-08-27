<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {templatePowerQueryN {_id, Name, Template}}`
const CREATE = `mutation ($name: String!, $template: String!) {createTemplatePowerQuery(name: $name, template: $template)}`
const UPDATE = `mutation ($_id: ID!, $name: String!, $template: String!) {updateTemplatePowerQuery(_id: $_id, name: $name, template: $template)}`
const DELETE = `mutation ($_id: ID!) {deleteTemplatePowerQuery(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'templatePowerQueryN')

const create = () =>
  run(async () => {
    const {Name, Template} = selected.value
    const data = await graphql(CREATE, {name: Name, template: Template})
    selected.value._id = data.createTemplatePowerQuery
    return data
  })

const update = () =>
  run(() => {
    const {_id, Name, Template} = selected.value
    return graphql(UPDATE, {_id, name: Name, template: Template})
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
    <PageHeader title="TemplatePowerQuery Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name">
      <template #headers>
        <th class="w-100">Template</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="item.Template.replaceAll('\n', '\\n')" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-model="selected.Template" label="Template" type="textarea" style="height: 400px; white-space: nowrap" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
