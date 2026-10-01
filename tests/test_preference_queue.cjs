// Exercise the actual dashboard handler with delayed HTTP responses.
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync('ultimate_v1/templates/dashboard.html','utf8');
const start=html.indexOf('    let desiredRiskPreference');
const end=html.indexOf('    async function updateMarginUsage',start);
const context={assert,console};
vm.createContext(context);
vm.runInContext(`
let riskPreferenceRevision=0,riskPreferenceSaving=false,confirmedRiskPreference=null;
const select={value:'激进'};
const document={getElementById:()=>select};
const window={latestRiskPayload:{risk_preference:'激进'}};
let calls=[],pending=[],shown=null;
function previewPreference(v){shown=v;}
function postJson(path,body){calls.push(body.risk_preference);return new Promise(resolve=>pending.push(resolve));}
function alert(msg){throw Error(msg);}
async function loadAll(){}
`+html.slice(start,end)+`
(async()=>{
 const first=updateRiskPreference('中性');
 await updateRiskPreference('激进');
 await updateRiskPreference('保守');
 assert.equal(shown,'保守');
 assert.equal(calls.join(','),'中性');
 pending.shift()({ok:true,risk_preference:'中性'});
 await Promise.resolve();await Promise.resolve();
 assert.equal(calls.join(','),'中性,保守');
 pending.shift()({ok:true,risk_preference:'保守'});
 await first;
 assert.equal(confirmedRiskPreference,'保守');
 assert.equal(shown,'保守');
 assert.equal(riskPreferenceSaving,false);
 console.log('Rapid preference changes: last selection wins');
})()
`,context).catch(e=>{console.error(e);process.exitCode=1});
