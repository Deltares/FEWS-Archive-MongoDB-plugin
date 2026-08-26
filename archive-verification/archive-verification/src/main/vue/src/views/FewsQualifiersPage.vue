<script setup>
import {ref, onMounted} from 'vue'
import JsonEditorVue from 'json-editor-vue'
import {graphql} from '@/graphql'

const item = ref({})
const loading = ref(false)
const error = ref(null)

onMounted(async () => {
  loading.value = true
  try { item.value = (await graphql(`query {fewsQualifiersN {_id, qualifiers, lastUpdated}}`)).fewsQualifiersN[0] ?? {} }
  catch (e) { error.value = e }
  finally { loading.value = false }
})
</script>

<template>
<v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error?.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1"><h3 class="ma-1">FewsQualifiers Viewer</h3></div>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4 class="ma-1">Viewing: FewsQualifiers</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-lastUpdated" :title="item?._id" class="border rounded-lg pa-2 input-label">lastUpdated</label><input id="i-lastUpdated" type="text" readonly class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" :value="item?.lastUpdated"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-qualifiers" class="border rounded-lg pa-2 input-label">qualifiers</label><JsonEditorVue id="i-qualifiers" read-only :main-menu-bar="false" :status-bar="false" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" :model-value="item?.qualifiers"/></div>
  </div>
</div>
</template>
