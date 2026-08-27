<script setup>
// The scrolling list at the top of every editor page: one radio per row, and
// picking a row loads it into the form below.
//
//   <SelectTable v-model="selected" :items="sorted" :label="s => s.Name"
//                :on-select="s => ({...s, Filters: JSON.stringify(s.Filters, null, 2)})">
//     <template #headers><th class="w-100">Filters (JSON)</th></template>
//     <template #cells="{item, id}">…</template>
//   </SelectTable>
//
// The radio and its label live here, so their for/id stay paired; every other
// column comes from the two slots. `onSelect` exists because some pages need to
// reshape the row on the way into the form (JSON columns are edited as text).
defineProps({
  items: {type: Array, required: true},
  labelHeader: {type: String, required: true},
  label: {type: Function, default: (item) => item.Name},
  onSelect: {type: Function, default: (item) => ({...item})},
})

const selected = defineModel({type: Object, required: true})

const rowId = (item) => `r_${item._id}`
</script>

<template>
  <v-table hover class="border rounded-lg mt-2" density="compact" fixed-header height="300px">
    <thead>
      <tr>
        <th><v-icon>mdi-pencil-outline</v-icon></th>
        <th>{{ labelHeader }}</th>
        <slot name="headers" />
      </tr>
    </thead>
    <tbody>
      <tr v-for="item in items" :key="item._id" :title="item._id">
        <td>
          <input :id="rowId(item)" type="radio" :value="item._id" :checked="selected._id === item._id" @change="selected = onSelect(item)" />
        </td>
        <td>
          <label :for="rowId(item)">{{ label(item) }}</label>
        </td>
        <slot :id="rowId(item)" name="cells" :item="item" />
      </tr>
    </tbody>
  </v-table>
</template>
