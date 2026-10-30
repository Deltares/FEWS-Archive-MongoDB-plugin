<script setup>
defineEmits(['create', 'update', 'remove'])
</script>

<template>
  <div class="mt-4">
    <v-btn @click="$emit('create')">Create</v-btn>
    <v-btn class="ml-2" @click="$emit('update')">Update</v-btn>
    <v-btn class="ml-2" @click="$emit('remove')">Delete</v-btn>
    <slot />
  </div>
</template>
