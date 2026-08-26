export async function graphql(query, variables) {
  const response = await fetch('./graphql', {
    method: 'POST',
    headers: {'content-type': 'application/json'},
    body: JSON.stringify({query, variables})
  })
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
  const {data, errors} = await response.json()
  if (errors?.length) throw new Error(errors.map(e => e.message).join('\n'))
  return data
}
