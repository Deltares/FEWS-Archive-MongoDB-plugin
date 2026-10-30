<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const LIST = `query {outputPowerQueryN {_id, Study, Name, Month, Expression}}`
const CREATE = `mutation ($study: String!, $name: String!, $month: String!, $expression: String!) {createOutputPowerQuery(study: $study, name: $name, month: $month, expression: $expression)}`
const UPDATE = `mutation ($_id: ID!, $study: String!, $name: String!, $month: String!, $expression: String!) {updateOutputPowerQuery(_id: $_id, study: $study, name: $name, month: $month, expression: $expression)}`
const DELETE = `mutation ($_id: ID!) {deleteOutputPowerQuery(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'outputPowerQueryN', (a) => `${a.Study}_${a.Name}_${a.Month}`)

const create = () =>
  run(async () => {
    const {Study, Name, Month, Expression} = selected.value
    const data = await graphql(CREATE, {study: Study, name: Name, month: Month || '', expression: Expression})
    selected.value._id = data.createOutputPowerQuery
    return data
  })

const update = () =>
  run(() => {
    const {_id, Study, Name, Month, Expression} = selected.value
    return graphql(UPDATE, {_id, study: Study, name: Name, month: Month || '', expression: Expression})
  })

const remove = () => {
  const {_id, Study, Name, Month} = selected.value
  if (!confirm(`Remove ${Study}_${Name}_${Month} [${_id}]?`)) return
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
    <PageHeader title="OutputPowerQuery Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Study" :label="(item) => item.Study">
      <template #headers>
        <th>Name</th>
        <th>Month</th>
        <th class="w-100">Expression</th>
      </template>
      <template #cells="{item, id}">
        <td>
          <label :for="id">{{ item.Name }}</label>
        </td>
        <td>
          <label :for="id">{{ item.Month }}</label>
        </td>
        <td><input type="text" class="w-100" readonly :value="item.Expression.replaceAll('\n', '\\n')" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Study" label="Study" />
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-model="selected.Month" label="Month" />
      <InputRow v-model="selected.Expression" label="Expression" type="textarea" style="height: 400px; white-space: nowrap" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
