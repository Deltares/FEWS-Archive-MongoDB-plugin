<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {templateDrdlYamlN {_id, Type, Name, Template}}`
const CREATE = `mutation ($type: String!, $name: String!, $template: String!) {createTemplateDrdlYaml(type: $type, name: $name, template: $template)}`
const UPDATE = `mutation ($_id: ID!, $type: String!, $name: String!, $template: String!) {updateTemplateDrdlYaml(_id: $_id, type: $type, name: $name, template: $template)}`
const DELETE = `mutation ($_id: ID!) {deleteTemplateDrdlYaml(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'templateDrdlYamlN', (a) => `${a.Type}_${a.Name}`)

const create = () =>
  run(async () => {
    const {Type, Name, Template} = selected.value
    const data = await graphql(CREATE, {type: Type, name: Name || '', template: Template})
    selected.value._id = data.createTemplateDrdlYaml
    return data
  })

const update = () =>
  run(() => {
    const {_id, Type, Name, Template} = selected.value
    return graphql(UPDATE, {_id, type: Type, name: Name || '', template: Template})
  })

const remove = () => {
  const {_id, Type, Name} = selected.value
  if (!confirm(`Remove ${Type}_${Name} [${_id}]?`)) return
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
    <PageHeader title="TemplateDrdlYaml Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Type" :label="(item) => item.Type">
      <template #headers>
        <th>Name</th>
        <th class="w-100">Template</th>
      </template>
      <template #cells="{item, id}">
        <td>
          <label :for="id">{{ item.Name }}</label>
        </td>
        <td><input type="text" class="w-100" readonly :value="item.Template.replaceAll('\n', '\\n')" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Type" />
    <div class="input">
      <InputRow v-model="selected.Type" label="Type" />
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-model="selected.Template" label="Template" type="textarea" style="height: 400px; white-space: nowrap" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
