import {randomUUID} from 'node:crypto';
import {mkdir,readFile,writeFile,rename,open} from 'node:fs/promises';
import {join} from 'node:path';
import {normalizeRanges,unionRanges,numberOrNull} from './intervals.mjs';

const FILE = 'temporal.json';
const RUN_LIMIT = 50;
const REJECT_LIMIT = 100;

function emptyState() {
  return {
    version: 1,
    entities: {},
    changeCursor: null,
    pendingDeletes: [],
    quarantine: [],
    ingest: {runs: [], rejected: []},
    receiveLog: []
  };
}

export function instanceKey(attribute,datasetId,instanceId) {
  return `${attribute ?? ''}\u0000${datasetId ?? ''}\u0000${instanceId ?? ''}`;
}

function observationKey(source,validFrom) {
  return JSON.stringify([source ?? null,validFrom]);
}

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requiredString(value,name) {
  if (typeof value !== 'string' || value.length === 0) throw new Error(`${name} required`);
  return value;
}

function finite(value,name) {
  const number = numberOrNull(value);
  if (number === null) throw new Error(`${name} must be a finite number`);
  return number;
}

function entityRecord(state,id) {
  let entity = state.entities[id];
  if (!entity) {
    entity = {entity:id, firstSeenAt:null, lastSeenAt:null, lastListedAt:null,
      attributes:{}, instances:{}, complete:[], historyFetches:[], backfill:{attempts:0,lastAttemptAt:null,lastSuccessAt:null,lastError:null,ok:null}};
    state.entities[id] = entity;
  }
  return entity;
}

function attrRecord(entity,attribute) {
  let record = entity.attributes[attribute];
  if (!record) {
    record = {attribute, complete:[], updatedAt:null};
    entity.attributes[attribute] = record;
  }
  return record;
}

/**
 * Merge one accepted history projection into the store. This is deliberately
 * additive: instances learned through the change channel are never dropped just
 * because a backfill snapshot does not mention them yet.
 */
function mergeHistoryInto(state,history,meta) {
  const fetchedAt = meta.at ?? new Date().toISOString();
  const entity = entityRecord(state,history.entity);
  entity.lastSeenAt = fetchedAt;
  entity.complete = unionRanges(entity.complete,history.complete);
  entity.historyFetches.push({at:fetchedAt,origin:meta.origin ?? null,complete:normalizeRanges(history.complete),instances:history.instances.length});
  if (entity.historyFetches.length > 20) entity.historyFetches.shift();
  entity.backfill.attempts += 1;
  entity.backfill.lastAttemptAt = fetchedAt;
  entity.backfill.lastSuccessAt = fetchedAt;
  entity.backfill.lastError = null;
  entity.backfill.ok = true;

  const attributesInPayload = new Set();
  for (const observation of history.instances) {
    const attribute = requiredString(observation.attribute,'history attribute');
    attributesInPayload.add(attribute);
    const record = attrRecord(entity,attribute);
    const key = instanceKey(attribute,observation.datasetId,observation.instanceId);
    let instance = entity.instances[key];
    if (!instance) {
      instance = {attribute,datasetId:observation.datasetId ?? null,instanceId:observation.instanceId ?? null,
        firstSeenAt:fetchedAt,lastSeenAt:fetchedAt,observations:{}};
      entity.instances[key] = instance;
    }
    instance.lastSeenAt = fetchedAt;
    const obsKey = observationKey(observation.source,observation.validFrom);
    const existing = instance.observations[obsKey];
    if (!existing) {
      instance.observations[obsKey] = {
        source:observation.source ?? null,
        value:observation.value,
        validFrom:observation.validFrom,
        validTo:observation.validTo,
        firstSeenAt:fetchedAt,
        lastSeenAt:fetchedAt,
        channels:['history'],
        origins:[meta.origin ?? null],
        revisions:[]
      };
    } else {
      existing.lastSeenAt = fetchedAt;
      if (!existing.channels.includes('history')) existing.channels.push('history');
      if (meta.origin && !existing.origins.includes(meta.origin)) existing.origins.push(meta.origin);
      if (existing.validTo !== observation.validTo || JSON.stringify(existing.value) !== JSON.stringify(observation.value)) {
        // A repeated history read that merely restates the projection is not a
        // revision; a concrete source bound replaces an unresolved null.
        existing.revisions.push({validTo:existing.validTo,value:existing.value,at:fetchedAt,source:existing.source,channel:'history'});
        if (existing.revisions.length > 10) existing.revisions.shift();
        existing.validTo = observation.validTo;
        existing.value = observation.value;
      }
    }
    attrRecord(entity,attribute).updatedAt = fetchedAt;
  }
  for (const attribute of attributesInPayload) {
    // complete applies to the attributes actually present in this projection;
    // an attribute absent from the payload has no new coverage claim.
    const record = attrRecord(entity,attribute);
    record.complete = unionRanges(record.complete,history.complete);
  }
  applyPendingDeletes(state,fetchedAt);
  state.receiveLog.push({at:fetchedAt,channel:'history',entity:history.entity,instances:history.instances.length,complete:normalizeRanges(history.complete)});
  trimLogs(state);
  return {entity:history.entity,instances:history.instances.length,complete:entity.complete};
}

function upsertObservation(state,item,meta) {
  const at = meta.at ?? new Date().toISOString();
  const entityId = requiredString(item.entity,'change entity');
  const attribute = requiredString(item.attribute,'change attribute');
  const validFrom = finite(item.validFrom,'change validFrom');
  const validTo = item.validTo === undefined || item.validTo === null ? null : finite(item.validTo,'change validTo');
  if (validTo !== null && validTo <= validFrom) throw new Error('change validTo must be after validFrom');
  if (!('value' in item)) throw new Error('change value required');
  const entity = entityRecord(state,entityId);
  entity.lastSeenAt = at;
  const record = attrRecord(entity,attribute);
  record.updatedAt = at;
  const key = instanceKey(attribute,item.datasetId,item.instanceId);
  let instance = entity.instances[key];
  if (!instance) {
    instance = {attribute,datasetId:item.datasetId ?? null,instanceId:item.instanceId ?? null,
      firstSeenAt:at,lastSeenAt:at,observations:{}};
    entity.instances[key] = instance;
  }
  instance.lastSeenAt = at;
  const obsKey = observationKey(item.source,validFrom);
  const existing = instance.observations[obsKey];
  // A change message without validTo does not assert that an instance is open
  // ended: keep a concrete bound already learned from the history projection.
  const resolvedValidTo = validTo === null && existing ? existing.validTo : validTo;
  if (!existing) {
    instance.observations[obsKey] = {
      source:item.source ?? null,
      value:item.value,
      validFrom,
      validTo,
      firstSeenAt:at,
      lastSeenAt:at,
      channels:['changes'],
      origins:[meta.origin ?? null],
      revisions:[]
    };
  } else {
    existing.lastSeenAt = at;
    if (!existing.channels.includes('changes')) existing.channels.push('changes');
    if (meta.origin && !existing.origins.includes(meta.origin)) existing.origins.push(meta.origin);
    if (existing.validTo !== resolvedValidTo || JSON.stringify(existing.value) !== JSON.stringify(item.value)) {
      existing.revisions.push({validTo:existing.validTo,value:existing.value,at,source:existing.source,channel:'changes'});
      if (existing.revisions.length > 10) existing.revisions.shift();
      existing.validTo = resolvedValidTo;
      existing.value = item.value;
    }
  }
  return {entity:entityId,datasetId:item.datasetId ?? null,instanceId:item.instanceId ?? null,attribute};
}

function deleteInstance(state,item,meta) {
  const at = meta.at ?? new Date().toISOString();
  const entityId = requiredString(item.entity,'delete entity');
  const attribute = requiredString(item.attribute,'delete attribute');
  const deletedAt = finite(item.at,'delete at');
  const key = instanceKey(attribute,item.datasetId,item.instanceId);
  // Record the evidence even when the instance has not been collected yet: the
  // delete is a fact about the source, but it is not a fact about an instance we
  // can display until the instance itself is seen.
  const entity = entityRecord(state,entityId);
  entity.lastSeenAt = at;
  const known = Boolean(entity.instances[key]);
  const existing = state.pendingDeletes.find(entry => entry.entity === entityId && entry.key === key && entry.at === deletedAt);
  if (existing) {
    existing.lastSeenAt = at;
    if (!existing.channels.includes('changes')) existing.channels.push('changes');
    if (meta.origin && !existing.origins.includes(meta.origin)) existing.origins.push(meta.origin);
    existing.applied = existing.applied || known;
    return {entity:entityId,datasetId:item.datasetId ?? null,instanceId:item.instanceId ?? null,attribute,at:deletedAt,duplicate:true,applied:existing.applied};
  }
  const entry = {entity:entityId,attribute,key,datasetId:item.datasetId ?? null,instanceId:item.instanceId ?? null,
    at:deletedAt,firstSeenAt:at,lastSeenAt:at,channels:['changes'],origins:[meta.origin ?? null],
    applied:known,appliedAt:known ? at : null,
    unobserved:known ? null : 'delete received before this instance was collected'};
  state.pendingDeletes.push(entry);
  return {entity:entityId,datasetId:item.datasetId ?? null,instanceId:item.instanceId ?? null,attribute,at:deletedAt,duplicate:false,applied:known};
}

function applyPendingDeletes(state,at) {
  for (const deletion of state.pendingDeletes) {
    if (deletion.applied) continue;
    const entity = state.entities[deletion.entity];
    const instance = entity?.instances?.[deletion.key];
    if (!instance) continue;
    deletion.applied = true;
    deletion.appliedAt = at;
    deletion.unobserved = null;
    deletion.observedInstances = Object.entries(instance.observations).map(([,observation]) => ({
      source:observation.source,
      validFrom:observation.validFrom,
      validTo:observation.validTo
    }));
  }
}

function trimLogs(state) {
  if (state.ingest.runs.length > RUN_LIMIT) state.ingest.runs.splice(0,state.ingest.runs.length - RUN_LIMIT);
  if (state.ingest.rejected.length > REJECT_LIMIT) state.ingest.rejected.splice(0,state.ingest.rejected.length - REJECT_LIMIT);
  if (state.receiveLog.length > 100) state.receiveLog.splice(0,state.receiveLog.length - 100);
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function assertState(state) {
  if (!isPlainObject(state) || !isPlainObject(state.entities)) return emptyState();
  const base = emptyState();
  return {...base,...state,entities:state.entities,
    pendingDeletes:Array.isArray(state.pendingDeletes) ? state.pendingDeletes : [],
    quarantine:Array.isArray(state.quarantine) ? state.quarantine : [],
    ingest:isPlainObject(state.ingest) ? {...base.ingest,...state.ingest} : base.ingest,
    receiveLog:Array.isArray(state.receiveLog) ? state.receiveLog : []};
}

/**
 * Durable store for instance observations, change cursor, deletion evidence and
 * per-range coverage. Writes are serialized and atomic; messages may be replayed
 * and are merged idempotently.
 */
export class TemporalStore {
  constructor(directory,options = {}) {
    this.directory = directory;
    this.clock = options.clock ?? (() => new Date().toISOString());
    this.serial = Promise.resolve();
    this.loaded = null;
  }

  async _load() {
    if (!this.loaded) {
      this.loaded = (async () => {
        try {return assertState(JSON.parse(await readFile(join(this.directory,FILE),'utf8')));}
        catch(error) {
          if(error.code === 'ENOENT') return emptyState();
          throw new Error(`temporal store is unreadable: ${error.message}`);
        }
      })();
    }
    return this.loaded;
  }

  async _write(state) {
    await mkdir(this.directory,{recursive:true});
    const temporary = join(this.directory,`${randomUUID()}.tmp`);
    await writeFile(temporary,JSON.stringify(state,null,2)+'\n');
    const handle = await open(temporary,'r');
    try {await handle.sync();} finally {await handle.close();}
    await rename(temporary,join(this.directory,FILE));
  }

  _mutate(operation) {
    const running = this.serial.then(async () => {
      const state = await this._load();
      const result = await operation(state);
      await this._write(state);
      return result;
    });
    this.serial = running.catch(() => {});
    return running;
  }

  async snapshot() {
    await this.serial;
    return clone(await this._load());
  }

  mergeHistory(history,meta = {}) {
    return this._mutate(state => mergeHistoryInto(state,history,{...meta,at:meta.at ?? this.clock()}));
  }

  recordBackfillFailure(entityId,error,meta = {}) {
    return this._mutate(state => {
      const at = meta.at ?? this.clock();
      const entity = entityRecord(state,requiredString(entityId,'entity id'));
      entity.backfill.attempts += 1;
      entity.backfill.lastAttemptAt = at;
      entity.backfill.lastError = {at,message:error.message,status:error.status ?? null};
      entity.backfill.ok = false;
      state.ingest.rejected.push({at,channel:'history',entity:entityId,error:error.message,status:error.status ?? null});
      trimLogs(state);
      return {entity:entityId,error:error.message,status:error.status ?? null};
    });
  }

  /**
   * Apply one /changes page. A malformed item is quarantined and reported; it is
   * never turned into a business observation. The cursor only advances with the
   * page that was actually processed.
   */
  applyChanges(payload,meta = {}) {
    return this._mutate(state => {
      if (!isPlainObject(payload) || !Array.isArray(payload.items)) throw new Error('changes payload requires items');
      const next = numberOrNull(payload.next);
      if (next === null) throw new Error('changes payload requires numeric next');
      const at = meta.at ?? this.clock();
      const applied = [];
      const duplicate = [];
      const rejected = [];
      for (const item of payload.items) {
        try {
          if (!isPlainObject(item)) throw new Error('change item must be an object');
          if (item.op === 'upsert') {
            const result = upsertObservation(state,item,{...meta,at});
            applied.push({op:'upsert',...result});
          } else if (item.op === 'deleteInstance') {
            const result = deleteInstance(state,item,{...meta,at});
            (result.duplicate ? duplicate : applied).push({op:'deleteInstance',...result});
          } else {
            throw new Error(`unknown change op ${JSON.stringify(item.op)}`);
          }
        } catch (error) {
          const entry = {at,item,error:error.message};
          state.quarantine.push(entry);
          state.ingest.rejected.push({at,channel:'changes',error:error.message,item});
          rejected.push(entry);
        }
      }
      const previous = state.changeCursor;
      state.changeCursor = next;
      applyPendingDeletes(state,at);
      const entities = [...new Set([...applied,...duplicate].map(item => item.entity))];
      state.receiveLog.push({at,channel:'changes',after:previous,next,items:payload.items.length,applied:applied.length,duplicate:duplicate.length,rejected:rejected.length});
      trimLogs(state);
      return {previous,next,applied,duplicate,rejected,entities};
    });
  }

  recordListing(ids,meta = {}) {
    return this._mutate(state => {
      const at = meta.at ?? this.clock();
      for (const id of ids) {
        const entity = entityRecord(state,requiredString(id,'listed entity id'));
        entity.lastSeenAt = at;
        entity.lastListedAt = at;
      }
      return {at,entities:[...ids]};
    });
  }

  recordRun(report) {
    return this._mutate(state => {
      const entry = {...report,at:report.finishedAt ?? this.clock()};
      state.ingest.runs.push(entry);
      trimLogs(state);
      return entry;
    });
  }
}
