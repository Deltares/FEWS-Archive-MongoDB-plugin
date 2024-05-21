<script setup>
import { useQuery, useMutation } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { ref, computed } from "vue";

const { result, loading, error, refetch } = useQuery(gql`query configurationDescriptionN {configurationDescriptionN {_id, Name, toEmailAddresses, fromEmailAddress, drdlYamlPath, fewsRestApiUri, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, reprocessCubes, taskInterval, fewsArchiveDbAesPassword, fewsArchiveDbUsername, parallelPartitions, drdlYamlConfigPath, fewsArchiveDbConnection, drdlYamlServiceRestartSuccess}}`)
const selected = ref({})
const success = ref(null)
const sorted = computed(() => result?.value?.configurationDescriptionN ? result.value.configurationDescriptionN.slice().sort((a, b) => a.environment.localeCompare(b.environment)) : [])

const createMutation = useMutation(gql`mutation createConfigurationDescription($name: String!, $toEmailAddresses: String!, $fromEmailAddress: String!, $drdlYamlPath: String!, $fewsRestApiUri: String!, $environment: String!, $threads: String!, $cubeAdmins: String!, $cubeUsers: String!, $bimPath: String!, $databaseConnectionString: String!, $smtpServer: String!, $tabularConnectionString: String!,$drdlYamlServiceRestart: String!, $execute: String!, $reprocessCubes: String!, $taskInterval: String!, $fewsArchiveDbAesPassword: String!, $fewsArchiveDbUsername: String!, $parallelPartitions: String!, $drdlYamlConfigPath: String!, $fewsArchiveDbConnection: String!, $drdlYamlServiceRestartSuccess: String!) {createConfigurationDescription(name: $name, toEmailAddresses: $toEmailAddresses, fromEmailAddress: $fromEmailAddress, drdlYamlPath: $drdlYamlPath, fewsRestApiUri: $fewsRestApiUri, environment: $environment, threads: $threads, cubeAdmins: $cubeAdmins, cubeUsers: $cubeUsers, bimPath: $bimPath, databaseConnectionString: $databaseConnectionString, smtpServer: $smtpServer, tabularConnectionString: $tabularConnectionString, drdlYamlServiceRestart: $drdlYamlServiceRestart, execute: $execute, reprocessCubes: $reprocessCubes, taskInterval: $taskInterval, fewsArchiveDbAesPassword: $fewsArchiveDbAesPassword, fewsArchiveDbUsername: $fewsArchiveDbUsername, parallelPartitions: $parallelPartitions, drdlYamlConfigPath: $drdlYamlConfigPath, fewsArchiveDbConnection: $fewsArchiveDbConnection, drdlYamlServiceRestartSuccess: $drdlYamlServiceRestartSuccess)}`)
const updateMutation = useMutation(gql`mutation updateConfigurationDescription($_id: ID!, $name: String!, $toEmailAddresses: String!, $fromEmailAddress: String!, $drdlYamlPath: String!, $fewsRestApiUri: String!, $environment: String!, $threads: String!, $cubeAdmins: String!, $cubeUsers: String!, $bimPath: String!, $databaseConnectionString: String!, $smtpServer: String!, $tabularConnectionString: String!,$drdlYamlServiceRestart: String!, $execute: String!, $reprocessCubes: String!, $taskInterval: String!, $fewsArchiveDbAesPassword: String!, $fewsArchiveDbUsername: String!, $parallelPartitions: String!, $drdlYamlConfigPath: String!, $fewsArchiveDbConnection: String!, $drdlYamlServiceRestartSuccess: String!) {updateConfigurationDescription(_id: $_id, name: $name, toEmailAddresses: $toEmailAddresses, fromEmailAddress: $fromEmailAddress, drdlYamlPath: $drdlYamlPath, fewsRestApiUri: $fewsRestApiUri, environment: $environment, threads: $threads, cubeAdmins: $cubeAdmins, cubeUsers: $cubeUsers, bimPath: $bimPath, databaseConnectionString: $databaseConnectionString, smtpServer: $smtpServer, tabularConnectionString: $tabularConnectionString, drdlYamlServiceRestart: $drdlYamlServiceRestart, execute: $execute, reprocessCubes: $reprocessCubes, taskInterval: $taskInterval, fewsArchiveDbAesPassword: $fewsArchiveDbAesPassword, fewsArchiveDbUsername: $fewsArchiveDbUsername, parallelPartitions: $parallelPartitions, drdlYamlConfigPath: $drdlYamlConfigPath, fewsArchiveDbConnection: $fewsArchiveDbConnection, drdlYamlServiceRestartSuccess: $drdlYamlServiceRestartSuccess)}`)
const deleteMutation = useMutation(gql`mutation deleteConfigurationDescription($_id: ID!) {deleteConfigurationDescription(_id: $_id)}`)

async function create() {
  const {Name, toEmailAddresses, fromEmailAddress, drdlYamlPath, fewsRestApiUri, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, reprocessCubes, taskInterval, fewsArchiveDbAesPassword, fewsArchiveDbUsername, parallelPartitions, drdlYamlConfigPath, fewsArchiveDbConnection, drdlYamlServiceRestartSuccess} = selected.value
  const result = await mutate(() => createMutation.mutate({ name: Name, toEmailAddresses: toEmailAddresses, fromEmailAddress: fromEmailAddress, drdlYamlPath: drdlYamlPath, fewsRestApiUri: fewsRestApiUri, environment: environment, threads: threads, cubeAdmins: cubeAdmins, cubeUsers: cubeUsers, bimPath: bimPath, databaseConnectionString: databaseConnectionString, smtpServer: smtpServer, tabularConnectionString: tabularConnectionString, drdlYamlServiceRestart: drdlYamlServiceRestart, execute: execute, reprocessCubes: reprocessCubes, taskInterval: taskInterval, fewsArchiveDbAesPassword: fewsArchiveDbAesPassword, fewsArchiveDbUsername: fewsArchiveDbUsername, parallelPartitions: parallelPartitions, drdlYamlConfigPath: drdlYamlConfigPath, fewsArchiveDbConnection: fewsArchiveDbConnection, drdlYamlServiceRestartSuccess: drdlYamlServiceRestartSuccess }))
  selected.value._id = result?.data ? result.data.createConfigurationDescription : selected.value._id
}

async function update() {
  const {_id, Name, toEmailAddresses, fromEmailAddress, drdlYamlPath, fewsRestApiUri, environment, threads, cubeAdmins, cubeUsers, bimPath, databaseConnectionString, smtpServer, tabularConnectionString, drdlYamlServiceRestart, execute, reprocessCubes, taskInterval, fewsArchiveDbAesPassword, fewsArchiveDbUsername, parallelPartitions, drdlYamlConfigPath, fewsArchiveDbConnection, drdlYamlServiceRestartSuccess} = selected.value
  await mutate(() => updateMutation.mutate({ _id: _id, name: Name, toEmailAddresses: toEmailAddresses, fromEmailAddress: fromEmailAddress, drdlYamlPath: drdlYamlPath, fewsRestApiUri: fewsRestApiUri, environment: environment, threads: threads, cubeAdmins: cubeAdmins, cubeUsers: cubeUsers, bimPath: bimPath, databaseConnectionString: databaseConnectionString, smtpServer: smtpServer, tabularConnectionString: tabularConnectionString, drdlYamlServiceRestart: drdlYamlServiceRestart, execute: execute, reprocessCubes: reprocessCubes, taskInterval: taskInterval, fewsArchiveDbAesPassword: fewsArchiveDbAesPassword, fewsArchiveDbUsername: fewsArchiveDbUsername, parallelPartitions: parallelPartitions, drdlYamlConfigPath: drdlYamlConfigPath, fewsArchiveDbConnection: fewsArchiveDbConnection, drdlYamlServiceRestartSuccess: drdlYamlServiceRestartSuccess }))
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
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>ConfigurationDescription Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Description (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.Name}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-Name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-Name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-toEmailAddresses" class="border rounded-lg pa-2 input-label">toEmailAddresses</label><input id="i-toEmailAddresses" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.toEmailAddresses"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fromEmailAddress" class="border rounded-lg pa-2 input-label">fromEmailAddress</label><input id="i-fromEmailAddress" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fromEmailAddress"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlPath" class="border rounded-lg pa-2 input-label">drdlYamlPath</label><input id="i-drdlYamlPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fewsRestApiUri" class="border rounded-lg pa-2 input-label">fewsRestApiUri</label><input id="i-fewsRestApiUri" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fewsRestApiUri"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-environment" class="border rounded-lg pa-2 input-label">environment</label><input id="i-environment" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.environment"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-threads" class="border rounded-lg pa-2 input-label">threads</label><input id="i-threads" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.threads"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-cubeAdmins" class="border rounded-lg pa-2 input-label">cubeAdmins</label><input id="i-cubeAdmins" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.cubeAdmins"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-cubeUsers" class="border rounded-lg pa-2 input-label">cubeUsers</label><input id="i-cubeUsers" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.cubeUsers"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-bimPath" class="border rounded-lg pa-2 input-label">bimPath</label><input id="i-bimPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.bimPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-databaseConnectionString" class="border rounded-lg pa-2 input-label">databaseConnectionString</label><input id="i-databaseConnectionString" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.databaseConnectionString"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-smtpServer" class="border rounded-lg pa-2 input-label">smtpServer</label><input id="i-smtpServer" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.smtpServer"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-tabularConnectionString" class="border rounded-lg pa-2 input-label">tabularConnectionString</label><input id="i-tabularConnectionString" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.tabularConnectionString"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlServiceRestart" class="border rounded-lg pa-2 input-label">drdlYamlServiceRestart</label><input id="i-drdlYamlServiceRestart" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlServiceRestart"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-execute" class="border rounded-lg pa-2 input-label">execute</label><input id="i-execute" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.execute"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-reprocessCubes" class="border rounded-lg pa-2 input-label">reprocessCubes</label><input id="i-reprocessCubes" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.reprocessCubes"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-taskInterval" class="border rounded-lg pa-2 input-label">taskInterval</label><input id="i-taskInterval" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.taskInterval"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fewsArchiveDbUsername" class="border rounded-lg pa-2 input-label">fewsArchiveDbUsername</label><input id="i-fewsArchiveDbUsername" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fewsArchiveDbUsername"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fewsArchiveDbAesPassword" title="fewsArchiveDbAesPassword" class="border rounded-lg pa-2 input-label">fewsArchiveDbAesPassword</label><input id="i-fewsArchiveDbAesPassword" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fewsArchiveDbAesPassword"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-parallelPartitions" class="border rounded-lg pa-2 input-label">parallelPartitions</label><input id="i-parallelPartitions" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.parallelPartitions"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlConfigPath" class="border rounded-lg pa-2 input-label">drdlYamlConfigPath</label><input id="i-drdlYamlConfigPath" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlConfigPath"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-fewsArchiveDbConnection" class="border rounded-lg pa-2 input-label">fewsArchiveDbConnection</label><input id="i-fewsArchiveDbConnection" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.fewsArchiveDbConnection"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-drdlYamlServiceRestartSuccess" class="border rounded-lg pa-2 input-label">drdlYamlServiceRestartSuccess</label><input id="i-drdlYamlServiceRestartSuccess" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.drdlYamlServiceRestartSuccess"/></div>
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