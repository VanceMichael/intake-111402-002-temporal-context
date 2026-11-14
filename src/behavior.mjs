export function behavior(entry,state,fixture,scenario) {
if (entry.path === '/changes' && Number(entry.query.after) > 0) return {body:{next:Number(entry.query.after),items:[]}};
return null;
}
