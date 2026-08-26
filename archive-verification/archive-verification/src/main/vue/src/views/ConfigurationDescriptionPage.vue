<script setup>
import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

const FIELDS = ['toEmailAddresses', 'fromEmailAddress', 'drdlYamlPath', 'fewsRestApiUri', 'environment', 'cubeAdmins', 'cubeUsers', 'bimPath', 'databaseConnectionString', 'smtpServer', 'smtpPort', 'smtpUser', 'smtpPass', 'tabularConnectionString', 'execute', 'reprocessCubes', 'taskInterval', 'fewsArchiveDbAesPassword', 'fewsArchiveDbUsername', 'fewsArchiveDbConnection', 'dataStaleAfterSeconds', 'cubeParallelPartitions', 'cubeThreads', 'dataParallelPartitions', 'dataThreads', 'fewsVerificationDbAesPassword', 'fewsVerificationDbConnection', 'fewsVerificationDbUsername', 'parallel', 'processData']
const args = ['name', ...FIELDS]
const sig = args.map(a => `$${a}: String!`).join(', ')
const call = args.map(a => `${a}: $${a}`).join(', ')

const LIST = `query {configurationDescriptionN {_id, Name, ${FIELDS.join(', ')}}}`
const CREATE = `mutation (${sig}) {createConfigurationDescription(${call})}`
const UPDATE = `mutation ($_id: ID!, ${sig}) {updateConfigurationDescription(_id: $_id, ${call})}`
const DELETE = `mutation ($_id: ID!) {deleteConfigurationDescription(_id: $_id)}`

const items = ref([])
const selected = ref({})
const loading = ref(false)
const error = ref(null)
const success = ref(null)
const sorted = computed(() => [...items.value].sort((a, b) => a.environment.localeCompare(b.environment)))
const variables = () => ({name: selected.value.Name, ...Object.fromEntries(FIELDS.map(f => [f, selected.value[f]]))})

async function run(mutation) {
  loading.value = true
  error.value = null
  success.value = null
  try {
    if (mutation) success.value = JSON.stringify(await mutation())
    items.value = (await graphql(LIST)).configurationDescriptionN
  }
  catch (e) { error.value = e }
  finally { loading.value = false }
}

onMounted(() => run())

const create = () => run(async () => {
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
<v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error?.message }}</v-alert>
<v-alert type="success" closable :model-value="!!success">{{ success }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">ConfigurationDescription Editor</h3></div>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead><tr><th><v-icon>mdi-pencil-outline</v-icon></th><th>Name</th><th class="w-100">Description (JSON)</th></tr></thead>
    <tbody><tr v-for="s in sorted" :key="s._id" :title="s._id"><td><input :id="'r_'+s._id" type="radio" :value="s._id" @change="selected = {...s}" v-model="selected._id" /></td><td><label :for="'r_'+s._id">{{s.Name}}</label></td><td><input type="text" class="w-100" readonly :value="JSON.stringify(s)"/></td></tr></tbody>
  </v-table>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Editing: {{selected.Name}}</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-Name" class="border rounded-lg pa-2 input-label">Name</label><input id="i-Name" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected.Name"/></div>
    <div v-for="f in FIELDS" :key="f" class="d-flex w-100 mt-2"><label :for="'i-'+f" class="border rounded-lg pa-2 input-label">{{f}}</label><input :id="'i-'+f" type="text" class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" v-model="selected[f]"/></div>
  </div>
  <div class="mt-4">
    <v-btn @click="create">Create</v-btn>
    <v-btn class="ml-2" @click="update">Update</v-btn>
    <v-btn class="ml-2" @click="remove">Delete</v-btn>
  </div>
</div>
</template>
