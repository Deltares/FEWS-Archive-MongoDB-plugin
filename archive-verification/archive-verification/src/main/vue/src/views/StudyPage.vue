<script setup>
import {graphql} from '@/graphql'
import {useEditor} from '@/composables/useEditor'
import StatusBar from '@/components/StatusBar.vue'
import InputRow from '@/components/InputRow.vue'
import SelectTable from '@/components/SelectTable.vue'
import PageHeader from '@/components/PageHeader.vue'
import SubHeader from '@/components/SubHeader.vue'
import EditorActions from '@/components/EditorActions.vue'

// One document selects the study list plus every dropdown's options, so the page
// loads (and reloads after a mutation) in a single round trip.
const LIST = `query {
  studyN {_id, Name, Observed, Forecasts, Seasonalities, Class, LocationAttributes, ForecastStartMonth, ForecastEndMonth, Time, Value, Normal, Cube, Active, ReprocessDays, MaxLeadTimeMinutes}
  observedN {_id, Name}
  forecastN {_id, Name}
  normalN {_id, Name}
  seasonalityN {_id, Name}
  classN {_id, Name}
  locationAttributesN {_id, Name}
  templateCubeN {_id, Name}
}`
const ARGS = `$name: String!, $observed: String!, $forecasts: [String!]!, $seasonalities: [String!]!, $_class: String!, $locationAttributes: String!, $forecastStartMonth: String!, $forecastEndMonth: String!, $time: String!, $value: String!, $normal: String!, $cube: String!, $active: Boolean!, $reprocessDays: Int!, $maxLeadTimeMinutes: Int!`
const CALL = `name: $name, observed: $observed, forecasts: $forecasts, seasonalities: $seasonalities, _class: $_class, locationAttributes: $locationAttributes, forecastStartMonth: $forecastStartMonth, forecastEndMonth: $forecastEndMonth, time: $time, value: $value, normal: $normal, cube: $cube, active: $active, reprocessDays: $reprocessDays, maxLeadTimeMinutes: $maxLeadTimeMinutes`
const CREATE = `mutation (${ARGS}) {createStudy(${CALL})}`
const UPDATE = `mutation ($_id: ID!, ${ARGS}) {updateStudy(_id: $_id, ${CALL})}`
const DELETE = `mutation ($_id: ID!) {deleteStudy(_id: $_id)}`

// `data` carries the whole response, so the dropdowns below read the other root
// fields the one query selected.
const {data, sorted, selected, loading, error, success, run} = useEditor(LIST, 'studyN')

const variables = () => {
  const s = selected.value
  return {
    name: s.Name,
    observed: s.Observed,
    forecasts: s.Forecasts,
    seasonalities: s.Seasonalities,
    _class: s.Class,
    locationAttributes: s.LocationAttributes,
    forecastStartMonth: s.ForecastStartMonth,
    forecastEndMonth: s.ForecastEndMonth,
    time: s.Time,
    value: s.Value,
    normal: s.Normal,
    cube: s.Cube,
    active: s.Active,
    reprocessDays: s.ReprocessDays,
    maxLeadTimeMinutes: s.MaxLeadTimeMinutes,
  }
}

const create = () =>
  run(async () => {
    const result = await graphql(CREATE, variables())
    selected.value._id = result.createStudy
    return result
  })

const update = () => run(() => graphql(UPDATE, {_id: selected.value._id, ...variables()}))

const remove = () => {
  const {_id, Name} = selected.value
  if (!confirm(`Remove ${Name} [${_id}]?`)) return
  return run(async () => {
    const result = await graphql(DELETE, {_id})
    selected.value = {}
    return result
  })
}
</script>

<template>
  <StatusBar :loading="loading" :error="error" :success="success" />
  <div class="pa-4 pt-2">
    <PageHeader title="Study Editor" />
    <SelectTable v-model="selected" :items="sorted" label-header="Name">
      <template #headers>
        <th class="w-100">Study (JSON)</th>
      </template>
      <template #cells="{item}">
        <td><input type="text" class="w-100" readonly :value="JSON.stringify(item)" /></td>
      </template>
    </SelectTable>
    <SubHeader :value="selected.Name" />
    <div class="input">
      <InputRow v-model="selected.Name" label="Name" />
      <InputRow v-model="selected.Observed" label="Observed" type="select" :options="data.observedN" option-key="Name" />
      <InputRow v-model="selected.Forecasts" label="Forecasts" type="select" :options="data.forecastN" option-key="Name" multiple size="10" />
      <InputRow v-model="selected.Seasonalities" label="Seasonalities" type="select" :options="data.seasonalityN" option-key="Name" multiple size="10" />
      <InputRow v-model="selected.Class" label="Class" type="select" :options="data.classN" option-key="Name" />
      <InputRow v-model="selected.LocationAttributes" label="LocationAttributes" type="select" :options="data.locationAttributesN" option-key="Name" />
      <InputRow v-model="selected.ForecastStartMonth" label="ForecastStartMonth" type="month" />
      <InputRow v-model="selected.ForecastEndMonth" label="ForecastEndMonth" type="month" />
      <InputRow v-model="selected.Time" label="Time" type="select" :options="['local', 'UTC']" />
      <InputRow v-model="selected.Value" label="Value" type="select" :options="['display', 'native']" />
      <InputRow v-model="selected.Normal" label="Normal" type="select" :options="data.normalN" option-key="Name" />
      <InputRow v-model="selected.Cube" label="Cube" type="select" :options="data.templateCubeN" option-key="Name" />
      <InputRow v-model="selected.Active" label="Active" type="checkbox" />
      <InputRow v-model="selected.ReprocessDays" label="ReprocessDays" type="number" min="1" max="999" step="1" />
      <InputRow v-model="selected.MaxLeadTimeMinutes" label="MaxLeadTimeMinutes" type="number" min="0" max="527040" step="1" />
    </div>
    <EditorActions @create="create" @update="update" @remove="remove" />
  </div>
</template>
