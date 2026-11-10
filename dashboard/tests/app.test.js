const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { JSDOM } = require('jsdom');

async function loadApp(rules = []) {
    const root=path.join(__dirname,'../public');
    const dom=new JSDOM(fs.readFileSync(path.join(root,'index.html'),'utf8'),{url:'http://localhost:3000',runScripts:'outside-only'});
    const w=dom.window, sockets=[], timers=[];
    class Socket {
        static OPEN=1;static CONNECTING=0;
        constructor(){this.readyState=1;this.sent=[];sockets.push(this);queueMicrotask(()=>this.onopen?.());}
        send(value){this.sent.push(JSON.parse(value));}
        message(tabId,payload){this.onmessage({data:JSON.stringify({tabId,payload})});}
    }
    w.localStorage.setItem("rill_alert_settings", JSON.stringify({soundEnabled:false,webhookEnabled:false}));
    w.localStorage.setItem('rill_alert_rules', JSON.stringify(rules));
    w.WebSocket=Socket;
    w.fetch=async()=>({ok:true,json:async()=>({authenticated:true})});
    w.setTimeout=(fn)=>{timers.push(fn);return timers.length;};
    w.Chart=class {static defaults={font:{}};constructor(ctx,config){this.data=config.data;this.options=config.options;}update(){}destroy(){}resetZoom(){}};
    w.HTMLCanvasElement.prototype.getContext=()=>({});
    w.eval(fs.readFileSync(path.join(root,'state.js'),'utf8'));
    w.eval(fs.readFileSync(path.join(root,'app.js'),'utf8'));
    await new Promise(resolve=>setImmediate(resolve));
    function connect(address){const d=w.document;d.getElementById('quack-address').value=address;d.getElementById('connect-form').dispatchEvent(new w.Event('submit',{cancelable:true}));}
    return {dom,w,sockets,timers,connect};
}
test('query and alert data remain text, including attribute-breaking values',async()=>{
    const {dom,w}=await loadApp();
    const attack='<img src=x onerror="window.pwned=1">';
    w.renderQueryResults({columns:[attack],rows:[{[attack]:attack}],elapsed_ms:1});
    assert.equal(w.document.querySelector('#query-results-table img'),null);
    assert.ok(w.document.getElementById('query-results-tbody').textContent.includes(attack));
    w.handleEngineAlerts([{ts:'2026-11-09T12:00:00',severity:'error',message:attack,detail:attack}],{address:attack});
    w.renderAlertLogs();
    assert.equal(w.document.querySelector('#alert-toast-container img, #alert-logs-table img'),null);
    assert.equal(w.pwned,undefined);dom.window.close();
});
test('business alerts support isolated metric maps and independent connection cooldowns', async () => {
    const {dom,w} = await loadApp([{id:'revenue',enabled:true,metricField:'Revenue - value',operator:'>',threshold:50,cooldownSeconds:300}]);
    const a = w.RillState.ensureConnection({id:1,address:'Server A'});
    const b = w.RillState.ensureConnection({id:2,address:'Server B'});
    a.businessMetrics['Revenue - value'] = 100;
    b.businessMetrics['Revenue - value'] = 200;
    await w.evaluateAlerts({businessMetrics:a.businessMetrics},a);
    await w.evaluateAlerts({businessMetrics:a.businessMetrics},a);
    await w.evaluateAlerts({businessMetrics:b.businessMetrics},b);
    const logs = JSON.parse(w.localStorage.getItem('rill_alert_logs'));
    assert.equal(logs.length,2);
    assert.ok(a.ruleTriggers.revenue);
    assert.ok(b.ruleTriggers.revenue);
    dom.window.close();
});
test('one socket, background query sync, server switching, and bridge reconnect',async()=>{
    const {dom,w,sockets,timers,connect}=await loadApp();
    assert.equal(sockets.length,1);
    connect('quack:localhost:9494');sockets[0].message(1,{status:'connected'});
    connect('quack:localhost:9495');sockets[0].message(2,{status:'connected'});
    sockets[0].message(1,{type:'scheduled_query_sync',config:{queryId:'q',name:'Server A',sql:'SELECT 1',intervalSeconds:3,type:'none'}});
    sockets[0].message(1,{type:'scheduled_result',queryId:'q',rows:[{value:99}],columns:['value'],elapsed_ms:1,ranAt:Date.now()});
    assert.ok(!w.document.getElementById('scheduled-queries-tbody').textContent.includes('Server A'));
    w.selectTab(1);
    assert.ok(w.document.getElementById('scheduled-queries-tbody').textContent.includes('Server A'));
    assert.ok(w.document.getElementById('scheduled-widgets-grid').textContent.includes('99'));
    sockets[0].readyState=3;sockets[0].onclose();await timers.pop()();
    await new Promise(resolve=>setImmediate(resolve));
    assert.equal(sockets.length,2);
    assert.deepEqual(sockets[1].sent.filter(m=>m.action==='connect').map(m=>m.tabId),[1,2]);
    sockets[1].message(1,{status:'connected'});
    assert.ok(sockets[1].sent.some(m=>m.action==='schedule_query'&&m.queryId==='q'&&m.intervalSeconds===3));
    dom.window.close();
});
