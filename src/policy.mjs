import {readFile} from 'node:fs/promises';

const defaultPath = new URL('../policy.json',import.meta.url);

export async function loadPolicy(path = defaultPath) {
  const policy = JSON.parse(await readFile(path,'utf8'));
  return normalizePolicy(policy);
}

export function normalizePolicy(policy = {}) {
  const sourceOrder = Array.isArray(policy.sourceOrder)
    ? policy.sourceOrder.filter(source => typeof source === 'string' && source.length > 0)
    : [];
  return {
    sourceOrder,
    interval: policy.interval === 'half-open' ? 'half-open' : 'half-open'
  };
}

/**
 * Lower rank means higher current-value priority. Sources outside sourceOrder are
 * deliberately ranked after every listed source: an unknown source must never win
 * merely because its observation is newer.
 */
export function sourceRank(source,sourceOrder) {
  if (typeof source === 'string') {
    const index = sourceOrder.indexOf(source);
    if (index >= 0) return index;
  }
  return sourceOrder.length;
}

export function compareSources(left,right,sourceOrder) {
  const leftRank = sourceRank(left,sourceOrder);
  const rightRank = sourceRank(right,sourceOrder);
  if (leftRank !== rightRank) return leftRank - rightRank;
  if (left === right) return 0;
  if (left === null || left === undefined) return 1;
  if (right === null || right === undefined) return -1;
  return String(left) < String(right) ? -1 : 1;
}

export function describeRank(source,sourceOrder) {
  const rank = sourceRank(source,sourceOrder);
  if (rank >= sourceOrder.length) return {rank:null,listed:false,sourceOrder};
  return {rank,listed:true,sourceOrder};
}
