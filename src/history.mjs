import {normalizeRanges,numberOrNull} from './intervals.mjs';

/**
 * Read the restricted history projection for one entity. Nothing here decides a
 * business value: it only turns the source response into instance observations
 * plus the ranges the source claims to have covered.
 */
export async function readHistory(client,entity,attribute = null) {
  const data = await client(`/temporal/${encodeURIComponent(entity)}`);
  if (!data || !Array.isArray(data.attributes) || !Array.isArray(data.complete)) {
    throw new Error(`temporal payload for ${entity} lacks attributes/complete`);
  }
  const instances = [];
  for (const raw of data.attributes) {
    if (!raw || typeof raw !== 'object') throw new Error(`temporal payload for ${entity} has a malformed attribute`);
    if (attribute !== null && raw.attribute !== attribute) continue;
    const validFrom = numberOrNull(raw.validFrom);
    const validTo = numberOrNull(raw.validTo);
    if (validFrom === null || validTo === null || validFrom >= validTo) throw new Error(`temporal payload for ${entity} has a malformed interval`);
    if (!('value' in raw)) throw new Error(`temporal payload for ${entity} has an instance without value`);
    instances.push({
      entity,
      attribute: typeof raw.attribute === 'string' ? raw.attribute : null,
      datasetId: raw.datasetId ?? null,
      instanceId: raw.instanceId ?? null,
      value: raw.value,
      validFrom,
      validTo,
      source: typeof raw.source === 'string' && raw.source.length ? raw.source : null,
      raw
    });
  }
  const complete = normalizeRanges(data.complete);
  if (complete.length !== data.complete.length) throw new Error(`temporal payload for ${entity} has a malformed complete range`);
  return {entity,complete,instances};
}
