import {readHistory} from './history.mjs';
import {collect} from './reader.mjs';

function historyRow(observation) {
  return {
    id:`${observation.entity}:${observation.attribute}:${observation.datasetId ?? ''}:${observation.instanceId ?? ''}@${observation.validFrom}`,
    entity:observation.entity,
    attribute:observation.attribute,
    datasetId:observation.datasetId ?? null,
    instanceId:observation.instanceId ?? null,
    value:observation.value,
    validFrom:observation.validFrom,
    validTo:observation.validTo,
    source:observation.source ?? null
  };
}

function changeRow(item) {
  return {
    id:`${item.entity}:${item.attribute}:${item.datasetId ?? ''}:${item.instanceId ?? ''}@${item.op === 'deleteInstance' ? item.at : item.validFrom}`,
    entity:item.entity,
    attribute:item.attribute,
    datasetId:item.datasetId ?? null,
    instanceId:item.instanceId ?? null,
    op:item.op,
    value:item.value,
    validFrom:item.validFrom ?? null,
    validTo:item.validTo ?? null,
    at:item.at ?? null,
    source:item.source ?? null
  };
}

function archivableChange(item) {
  return item && typeof item === 'object' && typeof item.entity === 'string' && item.entity.length > 0 &&
    typeof item.attribute === 'string' && item.attribute.length > 0 &&
    (item.op === 'upsert' || item.op === 'deleteInstance');
}

/**
 * Pull the history projection for every listed entity.
 *
 * Additive on purpose: a backfill snapshot never removes instances learned from
 * another channel, one entity failing does not abort the others, and a failure is
 * recorded rather than silently treated as an empty result.
 */
export async function backfill(client,store,archive,options = {}) {
  const entities = options.entities ?? null;
  const report = {ok:true,fetched:[],failed:[]};
  for (const entityId of entities) {
    try {
      const history = await readHistory(client,entityId,options.attribute ?? null);
      if (archive) {
        await archive.put(history.instances.map(historyRow),
          {source:options.origin ?? null,channel:'history',entity:entityId});
      }
      const merged = await store.mergeHistory(history,{origin:options.origin ?? null});
      report.fetched.push(merged);
    } catch (error) {
      report.ok = false;
      report.failed.push({entity:entityId,error:error.message,status:error.status ?? null});
      await store.recordBackfillFailure(entityId,error,{origin:options.origin ?? null});
    }
  }
  return report;
}

/**
 * Drain pages of /changes starting after the given cursor. Every page is merged
 * atomically with its cursor: a failed page leaves the cursor untouched so the
 * messages are retried, and replayed messages merge idempotently.
 */
export async function pollChanges(client,store,archive,options = {}) {
  const maxPages = options.maxPages ?? 50;
  const report = {ok:true,pages:[],applied:0,duplicate:0,rejected:0,startedAfter:options.after ?? null,next:null};
  let after = options.after ?? null;
  for (let page = 0; page < maxPages; page++) {
    const path = after === null ? '/changes?after=0' : `/changes?after=${encodeURIComponent(after)}`;
    let payload;
    try {
      payload = await client(path);
    } catch (error) {
      report.ok = false;
      report.error = {message:error.message,status:error.status ?? null,after};
      return report;
    }
    if (archive && Array.isArray(payload.items)) {
      const originals = payload.items.filter(archivableChange).map(changeRow);
      if (originals.length) {
        try {await archive.put(originals,{source:options.origin ?? null,channel:'changes'});}
        catch(error) {report.archiveError = error.message;}
      }
    }
    const result = await store.applyChanges(payload,{origin:options.origin ?? null});
    report.pages.push({after:result.previous,next:result.next,items:payload.items.length,
      applied:result.applied.length,duplicate:result.duplicate.length,rejected:result.rejected.length});
    report.applied += result.applied.length;
    report.duplicate += result.duplicate.length;
    report.rejected += result.rejected.length;
    for (const item of result.rejected) report.ok = false;
    after = result.next;
    report.next = after;
    if (payload.items.length === 0 || result.next === result.previous) break;
  }
  return report;
}

/**
 * One collection cycle. The cursor is read before the backfill so changes that
 * land while history is being fetched are still drained afterwards; the two
 * channels are merged, never allowed to clear each other.
 */
export async function sync(client,store,archive,options = {}) {
  const startedAt = options.clock ? options.clock() : new Date().toISOString();
  const origin = options.origin ?? null;
  const report = {startedAt,ok:true,entities:[],listing:null,backfill:null,changes:null,errors:[]};
  let listed;
  try {
    listed = await collect(client);
    if (archive) await archive.put(listed,{source:origin,adapter:'current'});
    await store.recordListing(listed.map(row => row.id),{origin});
    report.entities = listed.map(row => row.id);
  } catch (error) {
    report.ok = false;
    report.errors.push({stage:'listing',message:error.message,status:error.status ?? null});
    return store.recordRun({...report,finishedAt:options.clock ? options.clock() : new Date().toISOString()});
  }
  const snapshot = await store.snapshot();
  const before = snapshot.changeCursor;
  report.backfill = await backfill(client,store,archive,{entities:report.entities,origin});
  if (!report.backfill.ok) {report.ok = false;report.errors.push(...report.backfill.failed.map(failure => ({stage:'history',...failure})));}
  report.changes = await pollChanges(client,store,archive,{origin,after:before});
  if (!report.changes.ok) {report.ok = false;report.errors.push({stage:'changes',...report.changes.error,rejected:report.changes.rejected});}
  return store.recordRun({...report,finishedAt:options.clock ? options.clock() : new Date().toISOString()});
}
