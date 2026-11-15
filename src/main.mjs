import {resolve} from 'node:path';
import {createSource} from './source.mjs';
import {createClient} from './transport.mjs';
import {Archive} from './archive.mjs';
import {TemporalStore} from './store.mjs';
import {loadPolicy} from './policy.mjs';
import {sync} from './sync.mjs';
import {createApi} from './api.mjs';

const interval = Math.max(200,Number(process.env.INTERVAL_MS ?? 2000));
const policy = await loadPolicy(process.env.POLICY_FILE ? resolve(process.env.POLICY_FILE) : undefined);
const source = await createSource({port:Number(process.env.SOURCE_PORT ?? 4010),scenario:process.env.SCENARIO ?? 'normal'});
const directory = resolve(process.env.DATA_DIR ?? '.state');
const archive = new Archive(directory);
const store = new TemporalStore(directory);
const client = createClient(source.origin);
const api = await createApi(archive,Number(process.env.PORT ?? 4020),{store,policy});
console.log(JSON.stringify({source:source.origin,api:api.origin,interval}));

let running = false;
let stopping = false;
let timer = null;
const cycle = async () => {
  if (running || stopping) return;
  running = true;
  try {
    const report = await sync(client,store,archive,{origin:source.origin});
    console.log(JSON.stringify({sync:report.ok,entities:report.entities.length,next:report.changes?.next ?? null,errors:report.errors}));
  } catch(error) {
    console.error(JSON.stringify({sync:false,error:error.message}));
  } finally {
    running = false;
    if (!stopping) timer = setTimeout(cycle,interval);
  }
};

const stop = async () => {
  if (stopping) return;
  stopping = true;
  if (timer) clearTimeout(timer);
  await Promise.all([source.close(),api.close()]);
};
process.on('SIGINT',stop);
process.on('SIGTERM',stop);
cycle();
