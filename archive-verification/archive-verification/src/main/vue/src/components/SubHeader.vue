<script setup>
defineProps({
  value: {type: [String, Number, Boolean], default: null},
  verb: {type: String, default: 'Editing'},
})
</script>

<template>
  <div class="bg-grey-darken-2 text-center mt-2 border rounded-lg">
    <h4 class="ma-1">{{ verb }}: {{ value }}</h4>
  </div>
</template>
