<script setup>
defineProps({
  loading: {type: Boolean, default: false},
  error: {type: Error, default: null},
  success: {type: String, default: null},
  warning: {type: String, default: null},
})
</script>

<template>
  <v-overlay :model-value="loading" class="align-center justify-center"><v-progress-circular color="white" indeterminate /></v-overlay>
  <v-alert type="error" closable :model-value="!!error">{{ error?.message }}</v-alert>
  <v-alert type="warning" closable :model-value="!!warning" style="white-space: pre-line">{{ warning }}</v-alert>
  <v-alert type="success" closable :model-value="!!success" style="white-space: pre-line">{{ success }}</v-alert>
</template>
