import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {TemporalStore} from '../src/store.mjs';
import {projectEntity} from '../src/project.mjs';
import {sync} from '../src/sync.mjs';
import {SourceError} from '../src/transport.mjs';

const policy = {sourceOrder:['inspector','sensor'],interval:'half-open'};

async function withStore(fn) {
  const directory = await mkdtemp(join(tmpdir(),'temporal-'));
  try {return await fn(new TemporalStore(directory),directory);}
  finally {await rm(directory,{recursive:true,force:true});}
}

function fakeClient(handler) {
  return async path => {
    const result = await handler(path);
    if (result instanceof Error) throw result;
    return result;
  };
}

test('deleteInstance 截断实例到通知时刻，早于观测的删除不截断重建实例',async () => {
  await withStore(async store => {
    await store.mergeHistory({entity:'gate1',complete:[[0,30]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',value:'closed',validFrom:10,validTo:30,source:'sensor'}
    ]});
    await store.applyChanges({next:1,items:[
      {op:'deleteInstance',entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',at:15}
    ]});
    const before = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:14});
    assert.equal(before.attributes.status.current.value,'closed');
    assert.equal(before.attributes.status.current.effectiveValidTo,15);
    const after = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:16});
    assert.equal(after.attributes.status.current,null);
    assert.equal(after.attributes.status.reason,'no-instance-covers-projection-time');
    assert.equal(after.attributes.status.lastKnown.value,'closed');
    assert.equal(after.attributes.status.lastKnown.effectiveValidTo,15);
    assert.equal(after.attributes.status.instances[0].lifecycle,'deleted');
    assert.equal(after.attributes.status.instances[0].observations[0].endReason.kind,'deleteInstance');

    // A later history read that re-creates the same instance after the deletion
    // is a new lifecycle; the old deletion must not swallow it.
    await store.mergeHistory({entity:'gate1',complete:[[0,40]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',value:'closed',validFrom:20,validTo:40,source:'sensor'}
    ]});
    const recreated = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:25});
    assert.equal(recreated.attributes.status.current.value,'closed');
    assert.equal(recreated.attributes.status.current.validFrom,20);
    // The pre-recreation deletion is kept visible as an anomaly, and it does not
    // truncate the new lifecycle.
    assert.equal(recreated.attributes.status.instances[0].anomalies.length,1);
    assert.equal(recreated.attributes.status.instances[0].anomalies[0].kind,'observation-at-or-after-deletion');
    assert.equal(recreated.attributes.status.instances[0].observations[0].endReason.kind,'deleteInstance');
    assert.equal(recreated.attributes.status.instances[0].observations[1].endReason.kind,'validTo');
  });
});

test('sourceOrder 之外的来源不会因观测更新而胜出',async () => {
  await withStore(async store => {
    await store.mergeHistory({entity:'gate1',complete:[[0,30]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',value:'closed',validFrom:10,validTo:30,source:'sensor'},
      {entity:'gate1',attribute:'status',datasetId:'radar',instanceId:'r1',value:'unknown',validFrom:20,validTo:30,source:'radar'}
    ]});
    const projection = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:25});
    assert.equal(projection.attributes.status.current.source,'sensor');
    const rejected = projection.attributes.status.current.attribution.rejected.find(candidate => candidate.source === 'radar');
    assert.equal(rejected.outcome.rule,'source-priority');
  });
});

test('同一来源相同时刻按实例身份确定性取舍并说明原因',async () => {
  await withStore(async store => {
    await store.mergeHistory({entity:'gate1',complete:[[0,30]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'a',instanceId:'a1',value:'first',validFrom:10,validTo:30,source:'sensor'},
      {entity:'gate1',attribute:'status',datasetId:'b',instanceId:'b1',value:'second',validFrom:10,validTo:30,source:'sensor'}
    ]});
    const first = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:15});
    const second = projectEntity(await store.snapshot(),'gate1',{sourceOrder:policy.sourceOrder,at:15});
    assert.equal(first.attributes.status.current.value,'first');
    assert.equal(JSON.stringify(first.attributes.status.current),JSON.stringify(second.attributes.status.current));
    assert.equal(first.attributes.status.current.attribution.rejected[0].outcome.rule,'deterministic-instance-order');
  });
});

test('单个设施补采失败不推进游标也不清空其他实例',async () => {
  const directory = await mkdtemp(join(tmpdir(),'temporal-'));
  const store = new TemporalStore(directory);
  try {
    await store.mergeHistory({entity:'gate1',complete:[[0,10]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'manual',instanceId:'m1',value:'open',validFrom:0,validTo:10,source:'inspector'}
    ]});
    const client = fakeClient(path => {
      if (path === '/entities') return {items:[{id:'gate1',status:{value:'open',datasetId:'manual',instanceId:'m1'}}]};
      if (path.startsWith('/temporal/')) return new SourceError(503,{error:'temporarily unavailable'});
      if (path.startsWith('/changes')) return new Error('network down');
      return new Error(`unexpected ${path}`);
    });
    const report = await sync(client,store,null,{origin:'http://127.0.0.1:1'});
    assert.equal(report.ok,false);
    assert.equal(report.errors.some(error => error.stage === 'history'),true);
    assert.equal(report.errors.some(error => error.stage === 'changes'),true);
    const snapshot = await store.snapshot();
    assert.equal(snapshot.changeCursor,null);
    assert.equal(snapshot.entities.gate1.instances['status\u0000manual\u0000m1'].datasetIds,undefined);
    assert.equal(Object.keys(snapshot.entities.gate1.instances).length,1);
    assert.equal(snapshot.entities.gate1.backfill.ok,false);
    assert.equal(snapshot.entities.gate1.backfill.lastError.status,503);
    assert.equal(snapshot.ingest.rejected.at(-1).entity,'gate1');
  } finally {await rm(directory,{recursive:true,force:true});}
});

test('changes 页面失败不推进游标，恢复后从原游标继续',async () => {
  await withStore(async store => {
    await store.applyChanges({next:7,items:[]});
    let failChanges = true;
    const client = fakeClient(path => {
      if (path === '/entities') return {items:[{id:'gate1',status:{value:'open'}}]};
      if (path.startsWith('/temporal/')) return {complete:[[0,10]],attributes:[]};
      if (path.startsWith('/changes')) {
        if (failChanges) return new Error('timeout');
        return {next:9,items:[{op:'upsert',entity:'gate1',attribute:'status',datasetId:'sensor',instanceId:'s1',
          value:'closed',validFrom:8,source:'sensor'}]};
      }
      return new Error(`unexpected ${path}`);
    });
    const failed = await sync(client,store,null,{origin:'http://127.0.0.1:1'});
    assert.equal(failed.ok,false);
    assert.equal((await store.snapshot()).changeCursor,7);
    failChanges = false;
    const recovered = await sync(client,store,null,{origin:'http://127.0.0.1:1'});
    assert.equal(recovered.ok,true);
    assert.equal(recovered.changes.pages[0].after,7);
    assert.equal((await store.snapshot()).changeCursor,9);
  });
});

test('来源返回错误响应不会被当成空历史',async () => {
  await withStore(async (store,directory) => {
    void directory;
    await store.mergeHistory({entity:'gate1',complete:[[0,10]],instances:[
      {entity:'gate1',attribute:'status',datasetId:'manual',instanceId:'m1',value:'open',validFrom:0,validTo:10,source:'inspector'}
    ]});
    const client = fakeClient(path => {
      if (path === '/entities') return {items:[{id:'gate1',status:{value:'open'}}]};
      if (path.startsWith('/temporal/')) return new SourceError(401,{error:'not authorised'});
      return {next:1,items:[]};
    });
    const report = await sync(client,store,null,{origin:'http://127.0.0.1:1'});
    assert.equal(report.ok,false);
    const snapshot = await store.snapshot();
    // The previously learned coverage claim is untouched, not replaced by an
    // empty range derived from the failed response.
    assert.deepEqual(snapshot.entities.gate1.complete,[[0,10]]);
    const projection = projectEntity(snapshot,'gate1',{sourceOrder:policy.sourceOrder,at:5});
    assert.equal(projection.attributes.status.current.value,'open');
    assert.equal(projection.backfill.lastError.status,401);
  });
});
