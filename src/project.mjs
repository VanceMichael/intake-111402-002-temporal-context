import {gapsWithin,numberOrNull,unionRanges} from './intervals.mjs';
import {compareSources,sourceRank} from './policy.mjs';

function finiteOrNull(value) {
  return numberOrNull(value);
}

function observationsOf(entity,attribute,sourceOrder) {
  const result = [];
  for (const instance of Object.values(entity.instances)) {
    if (attribute !== undefined && instance.attribute !== attribute) continue;
    const key = `${instance.attribute}\u0000${instance.datasetId ?? ''}\u0000${instance.instanceId ?? ''}`;
    for (const observation of Object.values(instance.observations)) {
      result.push({...observation,key,datasetId:instance.datasetId,instanceId:instance.instanceId,
        attribute:instance.attribute,sourceOrder,sourceRank:sourceRank(observation.source,sourceOrder)});
    }
  }
  return result;
}

function tombstonesFor(deletions,key) {
  return deletions.filter(entry => entry.key === key).sort((a,b) => a.at - b.at);
}

/**
 * End of an observation's validity: the declared validTo, capped by the earliest
 * deleteInstance that actually postdates the observation. A deletion at or before
 * validFrom belongs to an earlier lifecycle and does not truncate a re-created instance.
 */
export function effectiveEnd(observation,deletions) {
  let end = finiteOrNull(observation.validTo);
  for (const deletion of tombstonesFor(deletions,observation.key)) {
    const at = finiteOrNull(deletion.at);
    if (at === null || at <= observation.validFrom) continue;
    end = end === null ? at : Math.min(end,at);
  }
  return end;
}

/** Latest instant for which there is any coverage claim or evidence of activity. */
function evidenceHorizon(entity,deletions,at) {
  const values = [at,...normalizeNumbers(entity.complete.map(range => range[1])),
    ...normalizeNumbers(deletions.map(deletion => deletion.at))];
  for (const instance of Object.values(entity.instances)) {
    for (const observation of Object.values(instance.observations)) {
      const end = effectiveEnd(observation,deletions);
      if (end !== null) values.push(end);
    }
  }
  return Math.max(0,...values);
}

function coverageFor(entity,attribute,deletions,at) {
  const record = entity.attributes[attribute] ?? {complete:[]};
  const complete = unionRanges([],record.complete);
  const horizon = evidenceHorizon(entity,deletions,at);
  return {complete,gaps:gapsWithin(complete,horizon),horizon,
    note:'gaps are ranges not yet asserted complete by the source; absence of an instance there is not evidence of deletion'};
}

function normalizeNumbers(values) {
  return values.map(finiteOrNull).filter(value => value !== null);
}

/**
 * A deletion only ends an observation when it falls inside that observation's
 * interval. Coincidence with a declared validTo is reported as validTo; a delete
 * that lands after the declared end is irrelevant to it.
 */
function truncatingDeletion(observation,deletions) {
  return tombstonesFor(deletions,observation.key)
    .filter(entry => finiteOrNull(entry.at) !== null && entry.at > observation.validFrom &&
      (observation.validTo === null || entry.at < observation.validTo))
    .sort((a,b) => a.at - b.at)[0] ?? null;
}

function describeObservation(observation,deletions) {
  const end = effectiveEnd(observation,deletions);
  const deletion = truncatingDeletion(observation,deletions);
  return {
    source:observation.source,
    value:observation.value,
    validFrom:observation.validFrom,
    validTo:observation.validTo,
    effectiveValidTo:end,
    sourceRank:observation.sourceRank,
    channels:[...observation.channels],
    origins:[...observation.origins],
    firstSeenAt:observation.firstSeenAt,
    lastSeenAt:observation.lastSeenAt,
    revisions:observation.revisions.map(revision => ({...revision})),
    endReason:deletion ? {kind:'deleteInstance',at:deletion.at,applied:deletion.applied}
      : observation.validTo === null ? null : {kind:'validTo',at:observation.validTo},
    deletions:tombstonesFor(deletions,observation.key).map(entry => ({
      at:entry.at,
      applied:entry.applied,
      firstSeenAt:entry.firstSeenAt,
      lastSeenAt:entry.lastSeenAt,
      channels:[...entry.channels],
      origins:[...entry.origins],
      rel:entry.at <= observation.validFrom ? 'before-observation'
        : (observation.validTo !== null && entry.at > observation.validTo) ? 'after-declared-end'
          : (observation.validTo !== null && entry.at === observation.validTo) ? 'at-declared-end' : 'truncates-observation'
    }))
  };
}

function coversAt(observation,deletions,at) {
  if (observation.validFrom > at) return false;
  const end = effectiveEnd(observation,deletions);
  return end === null || at < end;
}

function candidateOutcome(winner,candidate,sourceOrder) {
  if (candidate === winner) {
    return {selected:true,rule:'policy.sourceOrder',
      detail:`source ${JSON.stringify(candidate.source)} has priority rank ${candidate.sourceRank}${candidate.sourceRank >= sourceOrder.length ? ' (unlisted, ranked after all listed sources)' : ''} and covers the projection time`};
  }
  if (candidate.sourceRank !== winner.sourceRank) {
    return {selected:false,rule:'source-priority',
      detail:`source ${JSON.stringify(candidate.source)} ranks ${candidate.sourceRank} after ${JSON.stringify(winner.source)} (${winner.sourceRank}); validFrom ${candidate.validFrom} is ${candidate.validFrom > winner.validFrom ? 'newer but not higher priority' : 'not newer'}`};
  }
  if (candidate.validFrom !== winner.validFrom) {
    return {selected:false,rule:'same-source-recency',
      detail:'same source priority; the observation with the later validFrom is adopted'};
  }
  return {selected:false,rule:'deterministic-instance-order',
    detail:'equal priority and validFrom; instance identity breaks the tie'};
}

function compareCandidates(left,right) {
  const bySource = compareSources(left.source,right.source,left.sourceOrder);
  if (bySource !== 0) return bySource;
  if (left.validFrom !== right.validFrom) return right.validFrom - left.validFrom;
  const leftDataset = left.datasetId ?? '';
  const rightDataset = right.datasetId ?? '';
  if (leftDataset !== rightDataset) return leftDataset < rightDataset ? -1 : 1;
  const leftInstance = left.instanceId ?? '';
  const rightInstance = right.instanceId ?? '';
  return leftInstance < rightInstance ? -1 : leftInstance > rightInstance ? 1 : 0;
}

function selectCurrent(observations,deletions,at,sourceOrder) {
  const candidates = observations
    .filter(observation => coversAt(observation,deletions,at))
    .sort(compareCandidates);
  const winner = candidates[0] ?? null;
  return {
    winner,
    candidates:candidates.map(candidate => ({
      datasetId:candidate.datasetId ?? null,
      instanceId:candidate.instanceId ?? null,
      source:candidate.source,
      value:candidate.value,
      validFrom:candidate.validFrom,
      validTo:candidate.validTo,
      effectiveValidTo:effectiveEnd(candidate,deletions),
      sourceRank:candidate.sourceRank,
      outcome:candidateOutcome(winner,candidate,sourceOrder)
    }))
  };
}

function selectLastKnown(observations,deletions,at,sourceOrder) {
  const ended = observations
    .map(observation => ({observation,end:effectiveEnd(observation,deletions)}))
    .filter(entry => entry.observation.validFrom <= at && entry.end !== null && entry.end <= at)
    .sort((left,right) => left.end === right.end
      ? compareCandidates(left.observation,right.observation)
      : right.end - left.end);
  if (!ended.length) return null;
  const {observation,end} = ended[0];
  return {observation,end};
}

function projectInstances(entity,attribute,deletions,at,sourceOrder) {
  return observationsOf(entity,attribute,sourceOrder)
    .reduce((instances,observation) => {
      let entry = instances.find(candidate => candidate.key === observation.key);
      if (!entry) {
        entry = {key:observation.key,datasetId:observation.datasetId ?? null,instanceId:observation.instanceId ?? null,
          observations:[],firstSeenAt:null,lastSeenAt:null};
        instances.push(entry);
      }
      entry.observations.push(observation);
      entry.firstSeenAt = entry.firstSeenAt === null || observation.firstSeenAt < entry.firstSeenAt ? observation.firstSeenAt : entry.firstSeenAt;
      entry.lastSeenAt = entry.lastSeenAt === null || observation.lastSeenAt > entry.lastSeenAt ? observation.lastSeenAt : entry.lastSeenAt;
      return instances;
    },[])
    .map(instance => {
      const tombstones = tombstonesFor(deletions,instance.key);
      const ends = instance.observations.map(observation => effectiveEnd(observation,deletions));
      const open = ends.includes(null);
      const start = Math.min(...instance.observations.map(observation => observation.validFrom));
      const end = open ? null : Math.max(...ends);
      const deletionEndsInstance = end !== null && tombstones.some(deletion => deletion.at === end && deletion.at > start);
      let lifecycle = 'ended';
      if (start <= at && (end === null || at < end)) lifecycle = 'live';
      else if (start > at) lifecycle = 'future';
      else if (deletionEndsInstance) lifecycle = 'deleted';
      else if (end !== null && end <= at) lifecycle = 'expired';
      return {
        key:instance.key,
        attribute,
        datasetId:instance.datasetId,
        instanceId:instance.instanceId,
        firstSeenAt:instance.firstSeenAt,
        lastSeenAt:instance.lastSeenAt,
        interval:[start,end],
        lifecycle,
        observations:instance.observations
          .sort((left,right) => left.validFrom - right.validFrom || compareCandidates(left,right))
          .map(observation => describeObservation(observation,deletions)),
        deletions:tombstones.map(deletion => ({
          at:deletion.at,
          applied:deletion.applied,
          appliedAt:deletion.appliedAt ?? null,
          firstSeenAt:deletion.firstSeenAt,
          lastSeenAt:deletion.lastSeenAt,
          channels:[...deletion.channels],
          origins:[...deletion.origins],
          unobserved:deletion.unobserved ?? null
        })),
        anomalies:[
          ...instance.observations
            .filter(observation => tombstones.some(deletion => deletion.at <= observation.validFrom))
            .map(observation => ({kind:'observation-at-or-after-deletion',validFrom:observation.validFrom,
              deletionAt:tombstones.filter(deletion => deletion.at <= observation.validFrom).map(deletion => deletion.at),
              note:'the instance was observed again after a deletion; the deletion is not applied to this re-created interval'})),
          ...instance.observations
            .filter(observation => typeof observation.source !== 'string' || observation.source.length === 0)
            .map(observation => ({kind:'missing-source',validFrom:observation.validFrom,
              note:'provenance is incomplete; the source order cannot rank this observation'}))
        ]
      };
    })
    .sort((left,right) => String(left.instanceId ?? '') < String(right.instanceId ?? '') ? -1
      : String(left.instanceId ?? '') > String(right.instanceId ?? '') ? 1 : 0);
}

/**
 * Default projection time: the newest instant the centre actually observed an
 * event for. Coverage end alone is not used, otherwise a half-open history that
 * ends exactly at the last validTo would project the instant *after* the value.
 */
function defaultProjectionTime(entity,deletions) {
  const values = [0];
  for (const instance of Object.values(entity.instances)) {
    for (const observation of Object.values(instance.observations)) values.push(observation.validFrom);
  }
  for (const deletion of deletions) values.push(deletion.at);
  return Math.max(...normalizeNumbers(values));
}

function projectAttributes(entity,deletions,at,sourceOrder,unverified) {
  const attributes = {};
  for (const attribute of Object.keys(entity.attributes).sort()) {
    const observations = observationsOf(entity,attribute,sourceOrder);
    const {winner,candidates} = selectCurrent(observations,deletions,at,sourceOrder);
    const last = selectLastKnown(observations,deletions,at,sourceOrder);
    const coverage = coverageFor(entity,attribute,deletions,at);
    attributes[attribute] = {
      current:winner ? {
        value:winner.value,
        source:winner.source,
        datasetId:winner.datasetId ?? null,
        instanceId:winner.instanceId ?? null,
        validFrom:winner.validFrom,
        validTo:winner.validTo,
        effectiveValidTo:effectiveEnd(winner,deletions),
        attribution:{
          rule:'policy.sourceOrder',
          sourceOrder:[...sourceOrder],
          sourceRank:winner.sourceRank,
          sourceListed:winner.sourceRank < sourceOrder.length,
          chosenBecause:candidates.length === 1
            ? 'only-instance-covering-projection-time'
            : 'highest-priority-source-covering-projection-time; observation recency does not raise a source priority',
          rejected:candidates.slice(1).map(candidate => ({
            datasetId:candidate.datasetId ?? null,
            instanceId:candidate.instanceId ?? null,
            source:candidate.source,
            value:candidate.value,
            validFrom:candidate.validFrom,
            outcome:candidate.outcome
          }))
        },
        provenance:{
          channels:[...winner.channels],
          origins:[...winner.origins],
          firstSeenAt:winner.firstSeenAt,
          lastSeenAt:winner.lastSeenAt,
          revisions:winner.revisions.map(revision => ({...revision}))
        }
      } : null,
      reason:winner ? 'adopted' : 'no-instance-covers-projection-time',
      candidates,
      lastKnown:last ? {
        value:last.observation.value,
        source:last.observation.source,
        datasetId:last.observation.datasetId ?? null,
        instanceId:last.observation.instanceId ?? null,
        validFrom:last.observation.validFrom,
        effectiveValidTo:last.end,
        note:'shown only because no instance covers the projection time; not reported as current'
      } : null,
      coverage,
      instances:projectInstances(entity,attribute,deletions,at,sourceOrder)
    };
    if (unverified.length) {
      attributes[attribute].unverifiedDeletions = unverified.filter(entry => entry.attribute === attribute);
    }
  }
  return attributes;
}

/**
 * Current business projection plus the evidence behind it. Pure: operates on a
 * store snapshot and never mutates durable state.
 */
export function projectEntity(state,entityId,options = {}) {
  const entity = state.entities[entityId];
  if (!entity) return null;
  const sourceOrder = options.sourceOrder ?? [];
  const deletions = state.pendingDeletes.filter(entry => entry.entity === entityId);
  const requestedAt = finiteOrNull(options.at);
  const at = requestedAt === null ? defaultProjectionTime(entity,deletions) : requestedAt;
  const unverified = deletions.filter(entry => !entry.applied);
  return {
    entity:entityId,
    at,
    atSource:requestedAt === null ? 'newest-known-event' : 'parameter',
    sourceOrder:[...sourceOrder],
    lastSeenAt:entity.lastSeenAt ?? null,
    lastListedAt:entity.lastListedAt ?? null,
    backfill:entity.backfill,
    coverage:{
      complete:unionRanges([],entity.complete),
      gaps:gapsWithin(unionRanges([],entity.complete),evidenceHorizon(entity,deletions,at))
    },
    attributes:projectAttributes(entity,deletions,at,sourceOrder,unverified),
    unverifiedDeletions:unverified.map(entry => ({
      attribute:entry.attribute,
      datasetId:entry.datasetId,
      instanceId:entry.instanceId,
      at:entry.at,
      firstSeenAt:entry.firstSeenAt,
      channels:[...entry.channels],
      origins:[...entry.origins],
      note:entry.unobserved ?? 'instance not collected yet; deliberately not reported as a deleted instance'
    }))
  };
}

export function projectAll(state,options = {}) {
  return Object.keys(state.entities).sort()
    .map(entityId => projectEntity(state,entityId,options)).filter(Boolean);
}

export function projectCoverage(state,entityId,options = {}) {
  const projection = projectEntity(state,entityId,options);
  if (!projection) return null;
  const horizon = Math.max(...Object.values(projection.attributes).map(attribute => attribute.coverage.horizon),
    projection.at);
  return {
    entity:entityId,
    at:projection.at,
    horizon,
    complete:projection.coverage.complete,
    gaps:gapsWithin(projection.coverage.complete,horizon),
    attributes:Object.fromEntries(Object.entries(projection.attributes).map(([attribute,value]) => [attribute,{
      complete:value.coverage.complete,
      gaps:value.coverage.gaps,
      horizon:value.coverage.horizon
    }])),
    unverifiedDeletions:projection.unverifiedDeletions
  };
}
