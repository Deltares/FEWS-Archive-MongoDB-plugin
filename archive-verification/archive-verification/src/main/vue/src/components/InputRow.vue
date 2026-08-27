<script setup>
import {useAttrs, useId} from 'vue'

// One label + field row on an editor page.
//
//   <InputRow v-model="x" label="Name" type="text" />
//   <InputRow v-model="x" label="Port" type="number" min="1" max="999" />
//   <InputRow v-model="x" label="Active" type="checkbox" />
//   <InputRow v-model="x" label="Template" type="textarea" />
//   <InputRow v-model="x" label="Observed" type="select" :options="data.observedN" option-key="Name" />
//   <InputRow v-model="x" label="Time" type="select" :options="['local', 'UTC']" />
//
// Anything else - a JSON editor, or a field needing custom markup - goes in the
// default slot, which receives the generated id and the field's classes so the
// <label for> and the striping in main.css keep working.
defineOptions({inheritAttrs: false})

const props = defineProps({
  label: {type: String, required: true},
  type: {type: String, default: 'text'},
  options: {type: Array, default: () => []},
  optionKey: {type: String, default: null},
  title: {type: String, default: undefined},
})

const model = defineModel({type: [String, Number, Boolean, Array, Object], default: null})
const id = useId()
const attrs = useAttrs()
const fieldClass = 'border rounded-lg pa-2 flex-grow-1 ml-2 input-data'
const optionValue = (option) => (props.optionKey ? option[props.optionKey] : option)

function onInput(event) {
  const {value} = event.target
  const numeric = props.type === 'number' || attrs.type === 'number'
  model.value = numeric ? (value === '' ? null : Number(value)) : value
}
</script>

<template>
  <div class="d-flex w-100 mt-2">
    <label :for="id" :title="title ?? label" class="border rounded-lg pa-2 input-label">{{ label }}</label>
    <slot :id="id" :field-class="fieldClass">
      <div v-if="type === 'checkbox'" :id="id" :class="fieldClass"><input v-model="model" type="checkbox" class="font-weight-bold" /></div>
      <textarea v-else-if="type === 'textarea'" :id="id" v-model="model" v-bind="$attrs" spellcheck="false" :class="fieldClass" />
      <select v-else-if="type === 'select'" :id="id" v-model="model" v-bind="$attrs" :class="fieldClass">
        <option v-for="o in options" :key="optionValue(o)" :value="optionValue(o)" :label="optionValue(o)" />
      </select>
      <input v-else :id="id" v-bind="$attrs" :type="type" :class="fieldClass" :value="model" @input="onInput" />
    </slot>
  </div>
</template>
