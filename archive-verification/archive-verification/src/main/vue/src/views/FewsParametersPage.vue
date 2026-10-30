<script setup>
import {ref, onMounted} from 'vue'
import JsonEditorVue from 'json-editor-vue'
import {graphql} from '@/graphql'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'

const LIST = `query {fewsParametersN {_id, parameters, lastUpdated}}`

const item = ref({})
const loading = ref(false)
const error = ref(null)

onMounted(async () => {
  loading.value = true
  try {
    item.value = (await graphql(LIST)).fewsParametersN[0] ?? {}
  } catch (e) {
    error.value = e
  } finally {
    loading.value = false
  }
})
</script>

<template>
  <StatusBar :loading="loading" :error="error" />
  <div class="pa-4 pt-2">
    <PageHeader title="FewsParameters Viewer" />
    <SubHeader verb="Viewing" value="FewsParameters" />
    <div class="input">
      <InputRow :model-value="item?.lastUpdated" label="lastUpdated" readonly :title="item?._id" />
      <InputRow label="parameters">
        <template #default="{id, fieldClass}">
          <JsonEditorVue :id="id" read-only :main-menu-bar="false" :status-bar="false" :class="fieldClass" :model-value="item?.parameters" />
        </template>
      </InputRow>
    </div>
  </div>
</template>
