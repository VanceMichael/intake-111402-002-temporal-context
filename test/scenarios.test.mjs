import test from 'node:test';
import assert from 'node:assert/strict';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
test('来源变更与异常边界具有可观察的区别',async () => {
  const source = await createSource();
  try {
    const client = createClient(source.origin);
    const choose = name => client('/__scenario',{method:'POST',body:{name}});
await choose('overlap');
const change = (await client('/changes?after=0')).items.at(-1);
assert.equal(change.op,'deleteInstance');
assert.equal(change.datasetId,'sensor');
const history = await client('/temporal/gate1');
assert.equal(history.attributes[0].datasetId,'manual');
assert.deepEqual(history.complete,[[0,12]]);
} finally {await source.close();}
});
