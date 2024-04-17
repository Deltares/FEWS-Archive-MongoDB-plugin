<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";

const { result, loading, error, refetch } = useQuery(gql`query configurationSettingsN {configurationSettingsN {_id, toEmailAddresses, fromEmailAddress, drdlYamlPath, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, archiveDb, taskInterval, databaseConnectionAesPassword, databaseConnectionUsername, parallelPartitions, drdlYamlConfigPath, drdlYamlMongoDbUri}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result?.value?.configurationSettingsN ? result.value.configurationSettingsN.slice().sort((a, b) => a.environment.localeCompare(b.environment)) : [])

const createMutation = useMutation(gql`mutation createConfigurationSettings($toEmailAddresses: String!, $fromEmailAddress: String!, $drdlYamlPath: String!, $environment: String!, $threads: Int!, $cubeAdmins: String!, $cubeUsers: String!, $bimPath: String!, $databaseConnectionString: String!, $smtpServer: String!, $tabularConnectionString: String!,$drdlYamlServiceRestart: String!, $execute: Boolean!, $archiveDb: String!, $taskInterval: String!, $databaseConnectionAesPassword: String!, $databaseConnectionUsername: String!, $parallelPartitions: Int!, $drdlYamlConfigPath: String!, $drdlYamlMongoDbUri: String!) {createConfigurationSettings(toEmailAddresses: $toEmailAddresses, fromEmailAddress: $fromEmailAddress, drdlYamlPath: $drdlYamlPath, environment: $environment, threads: $threads, cubeAdmins: $cubeAdmins, cubeUsers: $cubeUsers, bimPath: $bimPath, databaseConnectionString: $databaseConnectionString, smtpServer: $smtpServer, tabularConnectionString: $tabularConnectionString, drdlYamlServiceRestart: $drdlYamlServiceRestart, execute: $execute, archiveDb: $archiveDb, taskInterval: $taskInterval, databaseConnectionAesPassword: $databaseConnectionAesPassword, databaseConnectionUsername: $databaseConnectionUsername, parallelPartitions: $parallelPartitions, drdlYamlConfigPath: $drdlYamlConfigPath, drdlYamlMongoDbUri: $drdlYamlMongoDbUri)}`)
const updateMutation = useMutation(gql`mutation updateConfigurationSettings($_id: ID!, $toEmailAddresses: String!, $fromEmailAddress: String!, $drdlYamlPath: String!, $environment: String!, $threads: Int!, $cubeAdmins: String!, $cubeUsers: String!, $bimPath: String!, $databaseConnectionString: String!, $smtpServer: String!, $tabularConnectionString: String!,$drdlYamlServiceRestart: String!, $execute: Boolean!, $archiveDb: String!, $taskInterval: String!, $databaseConnectionAesPassword: String!, $databaseConnectionUsername: String!, $parallelPartitions: Int!, $drdlYamlConfigPath: String!, $drdlYamlMongoDbUri: String!) {updateConfigurationSettings(_id: $_id, toEmailAddresses: $toEmailAddresses, fromEmailAddress: $fromEmailAddress, drdlYamlPath: $drdlYamlPath, environment: $environment, threads: $threads, cubeAdmins: $cubeAdmins, cubeUsers: $cubeUsers, bimPath: $bimPath, databaseConnectionString: $databaseConnectionString, smtpServer: $smtpServer, tabularConnectionString: $tabularConnectionString, drdlYamlServiceRestart: $drdlYamlServiceRestart, execute: $execute, archiveDb: $archiveDb, taskInterval: $taskInterval, databaseConnectionAesPassword: $databaseConnectionAesPassword, databaseConnectionUsername: $databaseConnectionUsername, parallelPartitions: $parallelPartitions, drdlYamlConfigPath: $drdlYamlConfigPath, drdlYamlMongoDbUri: $drdlYamlMongoDbUri)}`)
const deleteMutation = useMutation(gql`mutation deleteConfigurationSettings($_id: ID!) {deleteConfigurationSettings(_id: $_id)}`)

async function create() {
  const {toEmailAddresses, fromEmailAddress, drdlYamlPath, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, archiveDb, taskInterval, databaseConnectionAesPassword, databaseConnectionUsername, parallelPartitions, drdlYamlConfigPath, drdlYamlMongoDbUri} = selected.value
  const result = await mutate(() => createMutation.mutate({ toEmailAddresses: toEmailAddresses, fromEmailAddress: fromEmailAddress, drdlYamlPath: drdlYamlPath, environment: environment, threads: threads, cubeAdmins: cubeAdmins, cubeUsers: cubeUsers, bimPath: bimPath, databaseConnectionString: databaseConnectionString, smtpServer: smtpServer, tabularConnectionString: tabularConnectionString, drdlYamlServiceRestart: drdlYamlServiceRestart, execute: execute, archiveDb: archiveDb, taskInterval: taskInterval, databaseConnectionAesPassword: databaseConnectionAesPassword, databaseConnectionUsername: databaseConnectionUsername, parallelPartitions: parallelPartitions, drdlYamlConfigPath: drdlYamlConfigPath, drdlYamlMongoDbUri: drdlYamlMongoDbUri }))
  selected.value._id = result?.data ? result.data.createConfigurationSettings : selected.value._id
}

async function update() {
  const {_id, toEmailAddresses, fromEmailAddress, drdlYamlPath, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, archiveDb, taskInterval, databaseConnectionAesPassword, databaseConnectionUsername, parallelPartitions, drdlYamlConfigPath, drdlYamlMongoDbUri} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, toEmailAddresses: toEmailAddresses, fromEmailAddress: fromEmailAddress, drdlYamlPath: drdlYamlPath, environment: environment, threads: threads, cubeAdmins: cubeAdmins, cubeUsers: cubeUsers, bimPath: bimPath, databaseConnectionString: databaseConnectionString, smtpServer: smtpServer, tabularConnectionString: tabularConnectionString, drdlYamlServiceRestart: drdlYamlServiceRestart, execute: execute, archiveDb: archiveDb, taskInterval: taskInterval, databaseConnectionAesPassword: databaseConnectionAesPassword, databaseConnectionUsername: databaseConnectionUsername, parallelPartitions: parallelPartitions, drdlYamlConfigPath: drdlYamlConfigPath, drdlYamlMongoDbUri: drdlYamlMongoDbUri }))
}

async function remove() {
  const {_id, Name} = selected.value
  if (confirm(`Remove ${Name} [${_id}]?`)) {
    await mutate(() => deleteMutation.mutate({_id: _id}))
    selected.value = {}
  }
}

async function mutate(mutation){
  try {
    loading.value = true
    error.value = null
    success.value = null
    const result = await mutation()
    await refetch()
    success.value = {'message': JSON.stringify(result.data)}
    return result
  }
  catch (e){
    error.value = e
  }
  finally {
    loading.value = false
  }
}
</script>

<template>
<v-overlay :model-value="!!loading" class="align-center justify-center"><v-progress-circular color="white" v-if="loading" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>ConfigurationSettings Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>environment</th><th class="w-100">Settings (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.environment}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.environment}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-toEmailAddresses" class="border rounded-lg pa-2 input-label">toEmailAddresses</label><input id="i-toEmailAddresses" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.toEmailAddresses"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fromEmailAddress" class="border rounded-lg pa-2 input-label">fromEmailAddress</label><input id="i-fromEmailAddress" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fromEmailAddress"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlPath" class="border rounded-lg pa-2 input-label">drdlYamlPath</label><input id="i-drdlYamlPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-environment" class="border rounded-lg pa-2 input-label">environment</label><input id="i-environment" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.environment"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-threads" class="border rounded-lg pa-2 input-label">threads</label><input id="i-threads" type="number" min="1" max="999" step="1" pattern="\d+" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.threads"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-cubeAdmins" class="border rounded-lg pa-2 input-label">cubeAdmins</label><input id="i-cubeAdmins" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.cubeAdmins"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-cubeUsers" class="border rounded-lg pa-2 input-label">cubeUsers</label><input id="i-cubeUsers" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.cubeUsers"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-bimPath" class="border rounded-lg pa-2 input-label">bimPath</label><input id="i-bimPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.bimPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-databaseConnectionString" class="border rounded-lg pa-2 input-label">databaseConnectionString</label><input id="i-databaseConnectionString" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.databaseConnectionString"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-smtpServer" class="border rounded-lg pa-2 input-label">smtpServer</label><input id="i-smtpServer" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.smtpServer"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-tabularConnectionString" class="border rounded-lg pa-2 input-label">tabularConnectionString</label><input id="i-tabularConnectionString" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.tabularConnectionString"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlServiceRestart" class="border rounded-lg pa-2 input-label">drdlYamlServiceRestart</label><input id="i-drdlYamlServiceRestart" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlServiceRestart"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-execute" class="border rounded-lg pa-2 input-label">execute</label><div id="i-execute" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data"><input type="checkbox" class="font-weight-bold" v-model="selected.execute"/></div></div>
    <div class="d-flex w-100 mt-2"><label for="i-archiveDb" class="border rounded-lg pa-2 input-label">archiveDb</label><input id="i-archiveDb" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.archiveDb"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-taskInterval" class="border rounded-lg pa-2 input-label">taskInterval</label><input id="i-taskInterval" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.taskInterval"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-databaseConnectionUsername" class="border rounded-lg pa-2 input-label">databaseConnectionUsername</label><input id="i-databaseConnectionUsername" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.databaseConnectionUsername"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-databaseConnectionAesPassword" title="databaseConnectionAesPassword" class="border rounded-lg pa-2 input-label">databaseConnectionAesPassword</label><input id="i-databaseConnectionAesPassword" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.databaseConnectionAesPassword"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-parallelPartitions" class="border rounded-lg pa-2 input-label">parallelPartitions</label><input id="i-parallelPartitions" type="number" min="1" max="999" step="1" pattern="\d+" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.parallelPartitions"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlConfigPath" class="border rounded-lg pa-2 input-label">drdlYamlConfigPath</label><input id="i-drdlYamlConfigPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlConfigPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlMongoDbUri" class="border rounded-lg pa-2 input-label">drdlYamlMongoDbUri</label><input id="i-drdlYamlMongoDbUri" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlMongoDbUri"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>

<style scoped>

</style>