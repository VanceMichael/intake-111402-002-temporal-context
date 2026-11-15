function rowsFromListing(data) {
  if (!data || !Array.isArray(data.items)) throw new Error('entities payload requires items');
  return data.items.map(entity => {
    if (!entity || typeof entity.id !== 'string' || !entity.id) throw new Error('entity id required');
    if (!entity.status || typeof entity.status !== 'object') throw new Error(`entity ${entity.id} lacks status`);
    return {id:entity.id, value:entity.status.value,
      datasetId:entity.status.datasetId ?? null, instanceId:entity.status.instanceId ?? null};
  });
}

/** Legacy single-instance projection: current value only, original row shape preserved. */
export async function collect(client) {
  return rowsFromListing(await client('/entities'));
}

export function listEntities(client) {
  return client('/entities').then(rowsFromListing);
}
