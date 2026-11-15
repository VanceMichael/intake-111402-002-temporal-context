import {createServer} from 'node:http';
import {projectAll,projectCoverage,projectEntity} from './project.mjs';

function send(response,status,data) {
  response.writeHead(status,{'content-type':'application/json; charset=utf-8'});
  response.end(JSON.stringify(data,null,2));
}

function queryOf(url) {
  return Object.fromEntries(url.searchParams);
}

function atParameter(query) {
  if (query.at === undefined) return undefined;
  const at = Number(query.at);
  if (!Number.isFinite(at)) throw Object.assign(new Error('at must be a finite number'),{status:400});
  return at;
}

export async function createApi(archive,port = 0,options = {}) {
  const store = options.store ?? null;
  const policy = options.policy ?? {sourceOrder:[],interval:'half-open'};
  const server = createServer(async (request,response) => {
    try {
      const url = new URL(request.url,'http://127.0.0.1');
      const path = url.pathname;
      if (path === '/health') return send(response,200,{status:'ok'});
      if (request.method !== 'GET') return send(response,404,{});
      if (path === '/records') return send(response,200,await archive.list());
      if (!store) {
        if (path.startsWith('/temporal')) return send(response,503,{error:'temporal store not configured'});
        return send(response,404,{});
      }
      const at = atParameter(queryOf(url));
      const snapshot = await store.snapshot();
      if (path === '/temporal') {
        const projection = projectAll(snapshot,{sourceOrder:policy.sourceOrder,at});
        return send(response,200,{generatedFrom:'temporal-store',policy,entities:projection,
          ingest:{cursor:snapshot.changeCursor,recent:snapshot.ingest.runs.slice(-5),rejected:snapshot.ingest.rejected.slice(-20)}});
      }
      const match = path.match(/^\/temporal\/([^/]+)(?:\/([^/]+))?(?:\/(history|coverage))?$/);
      if (!match) return send(response,404,{});
      const entityId = decodeURIComponent(match[1]);
      const attribute = match[2] === undefined ? undefined : decodeURIComponent(match[2]);
      const view = match[2] !== undefined && ['history','coverage'].includes(match[2]) && match[3] === undefined
        ? match[2] : match[3] ?? null;
      const attributeName = match[2] !== undefined && ['history','coverage'].includes(match[2]) ? undefined : attribute;
      const entity = snapshot.entities[entityId];
      if (!entity) return send(response,404,{error:'unknown entity',entity:entityId});
      if (attributeName !== undefined && !entity.attributes[attributeName]) return send(response,404,{error:'unknown attribute',entity:entityId,attribute:attributeName});
      if (view === 'coverage') {
        const coverage = projectCoverage(snapshot,entityId,{sourceOrder:policy.sourceOrder,at});
        if (attributeName !== undefined) return send(response,200,{entity:entityId,attribute:attributeName,...coverage.attributes[attributeName]});
        return send(response,200,coverage);
      }
      if (view === 'history') {
        const projection = projectEntity(snapshot,entityId,{sourceOrder:policy.sourceOrder,at});
        const attributes = attributeName === undefined ? projection.attributes
          : {[attributeName]:projection.attributes[attributeName]};
        return send(response,200,{entity:entityId,at:projection.at,sourceOrder:projection.sourceOrder,
          unverifiedDeletions:projection.unverifiedDeletions,
          attributes:Object.fromEntries(Object.entries(attributes).map(([key,value]) => [key,{
            instances:value.instances,coverage:value.coverage,candidates:value.candidates
          }]))
        });
      }
      const projection = projectEntity(snapshot,entityId,{sourceOrder:policy.sourceOrder,at});
      if (attributeName !== undefined) {
        return send(response,200,{entity:entityId,at:projection.at,sourceOrder:projection.sourceOrder,
          ...projection.attributes[attributeName]});
      }
      return send(response,200,projection);
    } catch(error) {
      send(response,error.status ?? 500,{error:error.message});
    }
  });
  await new Promise((resolve,reject) => {server.once('error',reject);server.listen(port,'0.0.0.0',resolve);});
  return {origin:`http://127.0.0.1:${server.address().port}`,close:() => new Promise(resolve => server.close(resolve))};
}
