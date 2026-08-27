<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

const FIELDS = {
  toEmailAddresses: 'String!',
  fromEmailAddress: 'String!',
  drdlYamlPath: 'String!',
  fewsRestApiUri: 'String!',
  environment: 'String!',
  cubeAdmins: 'String!',
  cubeUsers: 'String!',
  bimPath: 'String!',
  databaseConnectionString: 'String!',
  smtpServer: 'String!',
  smtpPort: 'Int!',
  smtpUser: 'String!',
  smtpPass: 'String!',
  tabularConnectionString: 'String!',
  execute: 'Boolean!',
  reprocessCubes: 'String!',
  taskInterval: 'String!',
  fewsArchiveDbAesPassword: 'String!',
  fewsArchiveDbUsername: 'String!',
  fewsArchiveDbConnection: 'String!',
  dataStaleAfterSeconds: 'Int!',
  cubeParallelPartitions: 'Int!',
  cubeThreads: 'Int!',
  dataParallelPartitions: 'Int!',
  dataThreads: 'Int!',
  fewsVerificationDbAesPassword: 'String!',
  fewsVerificationDbConnection: 'String!',
  fewsVerificationDbUsername: 'String!',
  parallel: 'Boolean!',
  processData: 'Boolean!',
}
const names = Object.keys(FIELDS)
const sig = names.map((n) => `$${n}: ${FIELDS[n]}`).join(', ')
const call = names.map((n) => `${n}: $${n}`).join(', ')

const LIST = `query {configurationSettingsN {_id, ${names.join(', ')}}}`
const CREATE = `mutation (${sig}) {createConfigurationSettings(${call})}`
const UPDATE = `mutation ($_id: ID!, ${sig}) {updateConfigurationSettings(_id: $_id, ${call})}`
const DELETE = `mutation ($_id: ID!) {deleteConfigurationSettings(_id: $_id)}`

const {sorted, selected, loading, error, success, run} = useEditor(LIST, 'configurationSettingsN', (a) => a.environment)

const variables = () => Object.fromEntries(names.map((n) => [n, selected.value[n]]))

const create = () =>
  run(async () => {
    const data = await graphql(CREATE, variables())
    selected.value._id = data.createConfigurationSettings
    return data
  })

const update = () => run(() => graphql(UPDATE, {_id: selected.value._id, ...variables()}))

const remove = () => {
  const {_id, environment} = selected.value
  if (!confirm(`Remove ${environment} [${_id}]?`)) return
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
    <PageHeader title="ConfigurationSettings Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="environment" :label="(item) => item.environment">
      <template #headers>
        <th class="w-100">Settings (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.environment" />
    <div class="input">
      <InputRow v-model="selected.toEmailAddresses" label="toEmailAddresses" />
      <InputRow v-model="selected.fromEmailAddress" label="fromEmailAddress" />
      <InputRow v-model="selected.drdlYamlPath" label="drdlYamlPath" />
      <InputRow v-model="selected.fewsRestApiUri" label="fewsRestApiUri" />
      <InputRow v-model="selected.environment" label="environment" />
      <InputRow v-model="selected.cubeAdmins" label="cubeAdmins" />
      <InputRow v-model="selected.cubeUsers" label="cubeUsers" />
      <InputRow v-model="selected.bimPath" label="bimPath" />
      <InputRow v-model="selected.databaseConnectionString" label="databaseConnectionString" />
      <InputRow v-model="selected.smtpServer" label="smtpServer" />
      <InputRow v-model="selected.smtpPort" label="smtpPort" type="number" min="1024" max="65535" step="1" />
      <InputRow v-model="selected.smtpUser" label="smtpUser" />
      <InputRow v-model="selected.smtpPass" label="smtpPass" />
      <InputRow v-model="selected.tabularConnectionString" label="tabularConnectionString" />
      <InputRow v-model="selected.execute" label="execute" type="checkbox" />
      <InputRow v-model="selected.reprocessCubes" label="reprocessCubes" />
      <InputRow v-model="selected.taskInterval" label="taskInterval" />
      <InputRow v-model="selected.fewsArchiveDbUsername" label="fewsArchiveDbUsername" />
      <InputRow v-model="selected.fewsArchiveDbAesPassword" label="fewsArchiveDbAesPassword" />
      <InputRow v-model="selected.fewsArchiveDbConnection" label="fewsArchiveDbConnection" />
      <InputRow v-model="selected.dataStaleAfterSeconds" label="dataStaleAfterSeconds" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.cubeParallelPartitions" label="cubeParallelPartitions" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.cubeThreads" label="cubeThreads" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.dataParallelPartitions" label="dataParallelPartitions" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.dataThreads" label="dataThreads" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.fewsVerificationDbUsername" label="fewsVerificationDbUsername" />
      <InputRow v-model="selected.fewsVerificationDbAesPassword" label="fewsVerificationDbAesPassword" />
      <InputRow v-model="selected.fewsVerificationDbConnection" label="fewsVerificationDbConnection" />
      <InputRow v-model="selected.parallel" label="parallel" type="checkbox" />
      <InputRow v-model="selected.processData" label="processData" type="checkbox" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
