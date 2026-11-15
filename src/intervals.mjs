function finiteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function normalizeRange(range) {
  if (!Array.isArray(range) || range.length !== 2) return null;
  const from = finiteNumber(range[0]);
  const to = finiteNumber(range[1]);
  if (from === null || to === null || from >= to) return null;
  return [from,to];
}

export function normalizeRanges(ranges) {
  if (!Array.isArray(ranges)) return [];
  return ranges.map(normalizeRange).filter(Boolean).sort((a,b) => a[0] - b[0] || a[1] - b[1]);
}

export function unionRanges(left,right) {
  const ranges = [...normalizeRanges(left),...normalizeRanges(right)].sort((a,b) => a[0] - b[0] || a[1] - b[1]);
  const merged = [];
  for (const range of ranges) {
    const previous = merged.at(-1);
    if (previous && range[0] <= previous[1]) previous[1] = Math.max(previous[1],range[1]);
    else merged.push([...range]);
  }
  return merged;
}

/** Complement of the complete ranges inside [0,horizon]. Half-open on both sides. */
export function gapsWithin(complete,horizon) {
  const limit = finiteNumber(horizon);
  if (limit === null || limit <= 0) return [];
  const gaps = [];
  let cursor = 0;
  for (const [from,to] of unionRanges([],complete)) {
    if (to <= 0) continue;
    if (from >= limit) break;
    if (from > cursor) gaps.push([cursor,Math.min(from,limit)]);
    cursor = Math.max(cursor,to);
    if (cursor >= limit) break;
  }
  if (cursor < limit) gaps.push([cursor,limit]);
  return gaps.filter(([from,to]) => from < to);
}

export function numberOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(number) ? number : null;
}
