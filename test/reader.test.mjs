import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {createSource} from '../src/source.mjs';
import {createClient} from '../src/transport.mjs';
import {collect} from '../src/reader.mjs';
let source,client;
before(async () => {source = await createSource();client = createClient(source.origin);});
after(async () => {await source.close();});

test('目录保留属性实例身份', async () => {
  const rows = await collect(client);
  assert.deepEqual(rows[0], {id:'gate1',value:'open',datasetId:'manual',instanceId:'m1'});
  const history = await client('/temporal/gate1');
  assert.deepEqual(history.complete, [[0,20]]);
});
