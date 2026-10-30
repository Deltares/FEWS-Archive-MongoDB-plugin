<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const FIELDS = [
  'toEmailAddresses',
  'fromEmailAddress',
  'drdlYamlPath',
  'fewsRestApiUri',
  'environment',
  'cubeAdmins',
  'cubeUsers',
  'bimPath',
  'databaseConnectionString',
  'smtpServer',
  'smtpPort',
  'smtpUser',
  'smtpPass',
  'tabularConnectionString',
  'execute',
  'reprocessCubes',
  'taskInterval',
  'fewsArchiveDbAesPassword',
  'fewsArchiveDbUsername',
  'fewsArchiveDbConnection',
  'dataStaleAfterSeconds',
  'cubeParallelPartitions',
  'cubeThreads',
  'dataParallelPartitions',
  'dataThreads',
  'fewsVerificationDbAesPassword',
  'fewsVerificationDbConnection',
  'fewsVerificationDbUsername',
  'parallel',
  'processData',
]
const args = ['name', ...FIELDS]
const sig = args.map((a) => `$${a}: String!`).join(', ')
const call = args.map((a) => `${a}: $${a}`).join(', ')

const LIST = `query {configurationDescriptionN {_id, Name, ${FIELDS.join(', ')}}}`
const CREATE = `mutation (${sig}) {createConfigurationDescription(${call})}`
const UPDATE = `mutation ($_id: ID!, ${sig}) {updateConfigurationDescription(_id: $_id, ${call})}`
const DELETE = `mutation ($_id: ID!) {deleteConfigurationDescription(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'configurationDescriptionN')

const variables = () => ({name: selected.value.Name, ...Object.fromEntries(FIELDS.map((f) => [f, selected.value[f]]))})

const create = () =>
  run(async () => {
    const data = await graphql(CREATE, variables())
    selected.value._id = data.createConfigurationDescription
    return data
  })

const update = () => run(() => graphql(UPDATE, {_id: selected.value._id, ...variables()}))

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
    <PageHeader title="ConfigurationDescription Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name">
      <template #headers>
        <th class="w-100">Description (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-for="f in FIELDS" :key="f" v-model="selected[f]" :label="f" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
