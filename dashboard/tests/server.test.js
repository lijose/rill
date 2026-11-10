const test = require('node:test');
const assert = require('node:assert/strict');
const { once, EventEmitter } = require('node:events');
const { PassThrough } = require('node:stream');
const { WebSocket } = require('ws');
const { createDashboard } = require('../server');

async function start(t, options={}) {
    const dashboard=createDashboard({host:'127.0.0.1',...options});
    dashboard.server.listen(0,'127.0.0.1');await once(dashboard.server,'listening');
    const origin=`http://127.0.0.1:${dashboard.server.address().port}`;
    t.after(async()=>{
        for(const ws of dashboard.wss.clients)ws.terminate();
        await new Promise(resolve=>dashboard.server.close(resolve));
        dashboard.wss.close();
    });
    return {...dashboard, origin};
}
test('remote binding requires a token',()=>{
    assert.throws(()=>createDashboard({host:'0.0.0.0',token:''}),/requires/);
});
test('session, origin, and webhook destination boundaries',async t=>{
    const {origin}=await start(t,{token:'test-token'});
    assert.equal((await fetch(`${origin}/api/webhook/send`,{method:'POST',headers:{Origin:origin}})).status,401);
    assert.equal((await fetch(`${origin}/api/session`,{method:'POST',headers:{'Content-Type':'application/json',Origin:'https://attacker.example'},body:JSON.stringify({token:'test-token'})})).status,403);
    const login=await fetch(`${origin}/api/session`,{method:'POST',headers:{'Content-Type':'application/json',Origin:origin},body:JSON.stringify({token:'test-token'})});
    assert.equal(login.status,200);
    const cookie=login.headers.get('set-cookie').split(';')[0];
    assert.ok(login.headers.get('set-cookie').includes('HttpOnly'));
    const response=await fetch(`${origin}/api/webhook/send`,{method:'POST',headers:{'Content-Type':'application/json',Origin:origin,Cookie:cookie},body:JSON.stringify({url:'https://unapproved.example',payload:{}})});
    assert.equal(response.status,400);
    const socket=new WebSocket(origin.replace('http','ws'),{headers:{Origin:'https://attacker.example',Cookie:cookie}});
    const [error]=await once(socket,'error');assert.match(error.message,/403/);
});
test('replaced worker close does not discard its successor',async t=>{
    const workers=[];
    const spawnWorker=()=>{
        const w=new EventEmitter();w.stdout=new PassThrough();w.stderr=new PassThrough();w.stdin=new PassThrough();w.kill=()=>{};workers.push(w);return w;
    };
    const {origin,activeWorkers}=await start(t,{spawnWorker});
    const socket=new WebSocket(origin.replace('http','ws'),{headers:{Origin:origin}});await once(socket,'open');
    const connect={action:'connect',tabId:1,address:'quack:localhost:9494'};
    socket.send(JSON.stringify(connect));
    while(workers.length<1)await new Promise(resolve=>setImmediate(resolve));
    socket.send(JSON.stringify(connect));
    while(workers.length<2)await new Promise(resolve=>setImmediate(resolve));
    workers[0].emit('close',0);
    assert.equal([...activeWorkers.values()][0].get(1),workers[1]);
    const delivered=once(socket,'message');workers[1].stdout.write(JSON.stringify({status:'connected'})+'\n');
    assert.equal(JSON.parse((await delivered)[0]).payload.status,'connected');
    socket.close();await once(socket,'close');
});
