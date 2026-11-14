export async function collect(client) {
  const response = await client('/entities');
  return response.items.map(entity => ({id:entity.id, value:entity.status.value,
    datasetId:entity.status.datasetId ?? null, instanceId:entity.status.instanceId}));
}
