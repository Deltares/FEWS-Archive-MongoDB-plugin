<script setup>
defineProps({title: {type: String, required: true}})
</script>

<template>
  <div class="bg-blue-darken-2 rounded-lg text-center pa-1">
    <h3 class="ma-1">{{ title }}</h3>
  </div>
</template>
