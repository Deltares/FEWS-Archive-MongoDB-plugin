import {ref, computed, onMounted} from 'vue'
import {graphql} from '@/graphql'

export function useEditor(listQuery, collection, sortKey = (item) => item.Name) {
  const data = ref({})
  const selected = ref({})
  const loading = ref(false)
  const error = ref(null)
  const success = ref(null)
  const warning = ref(null)

  const items = computed(() => data.value[collection] ?? [])
  const sorted = computed(() => [...items.value].sort((a, b) => `${sortKey(a)}`.localeCompare(`${sortKey(b)}`)))

  async function run(mutation) {
    loading.value = true
    error.value = null
    success.value = null
    warning.value = null
    try {
      if (mutation) success.value = JSON.stringify(await mutation())
      data.value = await graphql(listQuery)
    } catch (e) {
      error.value = e
    } finally {
      loading.value = false
    }
  }

  onMounted(() => run())

  return {data, items, sorted, selected, loading, error, success, warning, run}
}
