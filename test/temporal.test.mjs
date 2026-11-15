import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
import {Archive} from '../src/archive.mjs';
import {TemporalStore} from '../src/store.mjs';
import {loadPolicy} from '../src/policy.mjs';
import {sync} from '../src/sync.mjs';
import {createApi} from '../src/api.mjs';

let policy;
before(async () => {policy = await loadPolicy();});
after(() => {});

async function harness(scenario) {
  const directory = await mkdtemp(join(tmpdir(),'temporal-'));
  const source = await createSource({scenario});
  const store = new TemporalStore(directory);
  const archive = new Archive(directory);
  const client = createClient(source.origin);
  const api = await createApi(archive,0,{store,policy});
  return {directory,source,store,archive,client,api,
    close:async () => {await Promise.all([source.close(),api.close()]);await rm(directory,{recursive:true,force:true});}};
}

test('属性历史通道收齐每个实例的来源与生效区间',async () => {
  const context = await harness('normal');
  try {
    await sync(context.client,context.store,context.archive,{origin:context.source.origin});
    const body = await (await fetch(`${context.api.origin}/temporal/gate1/history`)).json();
    const instances = body.attributes.status.instances;
    assert.equal(instances.length,1);
    assert.equal(instances[0].datasetId,'manual');
    assert.equal(instances[0].instanceId,'m1');
    assert.equal(instances[0].observations[0].source,'inspector');
    assert.deepEqual([instances[0].observations[0].validFrom,instances[0].observations[0].validTo],[0,20]);
    assert.deepEqual(instances[0].observations[0].channels,['history']);
    assert.match(instances[0].observations[0].origins[0],/^http:\/\/127\.0\.0\.1:/);
    const dto = await (await fetch(`${context.api.origin}/temporal/gate1`)).json();
    assert.equal(dto.attributes.status.current.value,'open');
    assert.equal(dto.attributes.status.current.attribution.rule,'policy.sourceOrder');
    assert.equal(dto.attributes.status.current.provenance.channels[0],'history');
  } finally {await context.close();}
});

test('观测时间较新的低优先级来源不会盖掉人工核验',async () => {
  const context = await harness('overlap');
  try {
    await sync(context.client,context.store,context.archive,{origin:context.source.origin});
    // sensor s1 is valid 10..15; manual m1 is 0..20. At 12 both cover the time.
    const at12 = await (await fetch(`${context.api.origin}/temporal/gate1?at=12`)).json();
    const current = at12.attributes.status.current;
    assert.equal(current.value,'open');
    assert.equal(current.source,'inspector');
    const rejected = current.attribution.rejected.find(candidate => candidate.source === 'sensor');
    assert.ok(rejected,'newer sensor instance must be listed as considered');
    assert.equal(rejected.outcome.selected,false);
    assert.equal(rejected.outcome.rule,'source-priority');
    const at14 = await (await fetch(`${context.api.origin}/temporal/gate1?at=14`)).json();
    assert.equal(at14.attributes.status.current.source,'inspector');
  } finally {await context.close();}
});

test('删除通知只作用于目标实例且不产生已删除的幻影实例',async () => {
  const context = await harness('overlap');
  try {
    await sync(context.client,context.store,context.archive,{origin:context.source.origin});
    const body = await (await fetch(`${context.api.origin}/temporal/gate1`)).json();
    const byInstance = Object.fromEntries(body.attributes.status.instances.map(instance => [instance.instanceId,instance]));
    assert.equal(byInstance.m1.lifecycle,'live');
    assert.equal(byInstance.s1.lifecycle,'deleted');
    assert.equal(byInstance.s1.deletions.length,1);
    assert.equal(byInstance.s1.deletions[0].at,15);
    assert.equal(body.unverifiedDeletions.length,0);
    const at13 = await (await fetch(`${context.api.origin}/temporal/gate1?at=13`)).json();
    assert.equal(at13.attributes.status.current.source,'inspector');
    // Before the deletion takes effect the sensor instance is still a live candidate.
    assert.equal(at13.attributes.status.candidates.some(candidate => candidate.datasetId === 'sensor'),true);
    const at16 = await (await fetch(`${context.api.origin}/temporal/gate1?at=16`)).json();
    // After the deleteInstance the sensor instance is no longer offered at all.
    assert.equal(at16.attributes.status.candidates.some(candidate => candidate.datasetId === 'sensor'),false);
    assert.equal(at16.attributes.status.current.source,'inspector');
  } finally {await context.close();}
});

test('尚未拉到的实例收到删除通知时保持未核验而不是凭空删除',async () => {
  const directory = await mkdtemp(join(tmpdir(),'temporal-'));
  const store = new TemporalStore(directory);
  try {
    await store.applyChanges({next:2,items:[{op:'deleteInstance',entity:'gate9',attribute:'status',datasetId:'sensor',instanceId:'s9',at:5}]});
    const body = await (await import('../src/project.mjs')).projectEntity(await store.snapshot(),'gate9',{sourceOrder:policy.sourceOrder});
    assert.equal(body.attributes.status,undefined);
    assert.equal(body.unverifiedDeletions.length,1);
    assert.equal(body.unverifiedDeletions[0].instanceId,'s9');
    assert.equal(body.unverifiedDeletions[0].at,5);
    // Later history confirms the instance: the deletion becomes applied evidence.
    await store.mergeHistory({entity:'gate9',complete:[[0,10]],instances:[{entity:'gate9',attribute:'status',datasetId:'sensor',instanceId:'s9',
      value:'closed',validFrom:0,validTo:10,source:'sensor'}]});
    const after = await (await import('../src/project.mjs')).projectEntity(await store.snapshot(),'gate9',{sourceOrder:policy.sourceOrder});
    assert.equal(after.unverifiedDeletions.length,0);
    assert.equal(after.attributes.status.instances[0].lifecycle,'deleted');
    assert.equal(after.attributes.status.instances[0].observations[0].effectiveValidTo,5);
  } finally {await rm(directory,{recursive:true,force:true});}
});

test('补采与实时变更交叠时保留两边实例并给出未采全区间',async () => {
  const context = await harness('overlap');
  try {
    const first = await sync(context.client,context.store,context.archive,{origin:context.source.origin});
    assert.equal(first.ok,true);
    const coverage = await (await fetch(`${context.api.origin}/temporal/gate1/coverage`)).json();
    assert.deepEqual(coverage.complete,[[0,12]]);
    assert.deepEqual(coverage.gaps,[[12,20]]);
    const history = await (await fetch(`${context.api.origin}/temporal/gate1/history`)).json();
    const instances = history.attributes.status.instances;
    // The change channel saw s1; a later history snapshot cannot erase it.
    assert.deepEqual(instances.map(instance => instance.instanceId).sort(),['m1','s1']);
    // A second cycle re-reads history and polls from the stored cursor only.
    const second = await sync(context.client,context.store,context.archive,{origin:context.source.origin});
    assert.equal(second.changes.pages[0].after,3);
    const audit = await context.client('/__audit');
    assert.ok(audit.requests.filter(request => request.path === '/entities').length >= 2);
    // Restart-style read: durable state still has both instances.
    const reopened = new TemporalStore(context.directory);
    const snapshot = await reopened.snapshot();
    assert.deepEqual(Object.keys(snapshot.entities.gate1.instances).length,2);
    assert.equal(snapshot.changeCursor,3);
  } finally {await context.close();}
});

test('重复领取幂等且坏消息不会变成业务值',async () => {
  const directory = await mkdtemp(join(tmpdir(),'temporal-'));
  const store = new TemporalStore(directory);
  try {
    const payload = {next:5,items:[
      {op:'upsert',entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',value:'closed',validFrom:10,source:'sensor'},
      {op:'explode',entity:'gate1',attribute:'status'}
    ]};
    const first = await store.applyChanges(payload);
    assert.equal(first.applied.length,1);
    assert.equal(first.rejected.length,1);
    const snapshot = await store.snapshot();
    assert.equal(snapshot.changeCursor,5);
    assert.equal(snapshot.quarantine.length,1);
    assert.equal(snapshot.ingest.rejected.at(-1).channel,'changes');
    // Replayed messages are recognised as duplicates and add no new versions.
    const replayed = await store.applyChanges(payload);
    assert.equal(replayed.duplicate.length,0);
    assert.equal(replayed.applied.length,1);
    const second = await store.snapshot();
    assert.equal(Object.keys(second.entities.gate1.instances['status\u0000sensor\u0000s1'].observations).length,1);
  } finally {await rm(directory,{recursive:true,force:true});}
});
