<script setup>
import { ref, onMounted } from 'vue'

const message = ref(null)
const stack = ref(null)

onMounted(() => {
  message.value = localStorage.getItem('message')
  stack.value = localStorage.getItem('stack')
})
</script>

<template>
  <div class="error-bg">
    <b>{{ message }}</b>
    <pre>{{ stack }}</pre>
  </div>
</template>

<style scoped>
.error-bg{
  background: url('/images/Bee-doo.jpg') top left / cover no-repeat fixed;
  height: 100%;
}
pre{
  white-space: pre;
}
</style>