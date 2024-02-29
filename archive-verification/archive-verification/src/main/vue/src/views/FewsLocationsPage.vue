<script setup>
import { useQuery } from '@vue/apollo-composable'
import gql from 'graphql-tag'
import { computed } from "vue";
import JsonViewer from 'vue-json-viewer'

const { result, loading, error } = useQuery(gql`query fewsLocationsN {fewsLocationsN {_id, locations, lastUpdated}}`)
const item = computed(() => result?.value?.fewsLocationsN ? result.value.fewsLocationsN[0] : {})

</script>

<template>
<v-overlay :model-value="!!loading" class="align-center justify-center"><v-progress-circular color="white" v-if="loading" indeterminate/></v-overlay>
<v-alert type="error" closable :model-value="!!error">{{ error.message }}</v-alert>
<div class="pa-4 pt-2">
  <div class="bg-blue-darken-2 rounded-lg text-center pa-2"><h3>Class Editor</h3></div>
  <div class="bg-grey-darken-2 text-center mt-6 border rounded-lg"><h4>Viewing: FewsLocations</h4></div>
  <div class="input">
    <div class="d-flex w-100 mt-2"><label for="i-lastUpdated" :title="item?._id" class="border rounded-lg pa-2 input-label">lastUpdated</label><input id="i-lastUpdated" type="text" readonly class="border rounded-lg pa-2 flex-grow-1 ml-2 input-data" :value="item?.lastUpdated"/></div>
    <div class="d-flex w-100 mt-2"><label for="i-locations" class="border rounded-lg pa-2 input-label">locations</label><json-viewer id="i-locations" class="border rounded-lg pa-2 flex-grow-1 ml-3 input-data" :value="item?.locations" /></div>
  </div>
</div>
</template>

<style scoped>

</style>