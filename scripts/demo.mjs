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

const directory = await mkdtemp(join(tmpdir(),'intake-'));
const policy = await loadPolicy();
const scenario = process.env.SCENARIO ?? 'overlap';
const source = await createSource({scenario});
const client = createClient(source.origin);
const archive = new Archive(directory);
const store = new TemporalStore(directory);
const api = await createApi(archive,0,{store,policy});
try {
  console.log('本地来源',source.origin,'场景',scenario);
  const first = await sync(client,store,archive,{origin:source.origin});
  console.log('首轮收件',JSON.stringify({ok:first.ok,entities:first.entities,next:first.changes?.next,errors:first.errors}));
  const second = await sync(client,store,archive,{origin:source.origin});
  console.log('次轮收件（游标增量）',JSON.stringify({ok:second.ok,applied:second.changes?.applied,rejected:second.changes?.rejected}));
  const entity = (await (await fetch(`${api.origin}/temporal/gate1`)).json());
  console.log('当前采用',JSON.stringify(entity.attributes.status.current,null,2));
  const history = await (await fetch(`${api.origin}/temporal/gate1/history`)).json();
  console.log('属性实例历史',JSON.stringify(history.attributes.status.instances,null,2));
  const coverage = await (await fetch(`${api.origin}/temporal/gate1/coverage`)).json();
  console.log('未采全区间',JSON.stringify(coverage,null,2));
  console.log('原始收件数',(await (await fetch(`${api.origin}/records`)).json()).length);
  console.log('来源交互',JSON.stringify(await client('/__audit')));
} finally {
  await Promise.all([source.close(),api.close()]);
  await rm(directory,{recursive:true,force:true});
}
