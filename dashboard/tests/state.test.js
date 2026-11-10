const test = require('node:test');
const assert = require('node:assert/strict');
const { ensureConnection, receiveResult, reconnectMessages } = require('../public/state');
const { publicAddress, validateWebhook } = require('../security');

test('inactive connections and table-only queries retain independent business results', () => {
    const a=ensureConnection({id:1}), b=ensureConnection({id:2});
    for(const conn of [a,b])conn.scheduledConfigs.q={name:'Revenue',sql:'SELECT 1',type:'none',intervalSeconds:5};
    receiveResult(a,{queryId:'q',rows:[{ts:'2026-11-09T12:00:00',value:100},{ts:'2026-11-09T11:00:00',value:10}]});
    receiveResult(b,{queryId:'q',rows:[{value:200}]});
    assert.equal(a.businessMetrics['Revenue - value'],100);
    assert.equal(b.businessMetrics['Revenue - value'],200);
    a.stoppedQueries.add('q');receiveResult(a,{queryId:'q',rows:[{value:300}]});
    assert.equal(a.businessMetrics['Revenue - value'],100);
});
test('reconnect restores intervals and stopped backend queries', () => {
    const conn=ensureConnection({id:7});conn.scheduledConfigs.q={sql:'SELECT 1',intervalSeconds:3};conn.stoppedQueries.add('removed');
    assert.deepEqual(reconnectMessages(conn),[{action:'unschedule_query',tabId:7,queryId:'removed'}, {action:'schedule_query',tabId:7,queryId:'q',sql:'SELECT 1',intervalSeconds:3}]);
});
test('numeric timestamps choose the latest row and failed or empty queries clear stale metrics', () => {
    const conn = ensureConnection({id:1, lastMetrics:{}});
    conn.scheduledConfigs.q = {name:'Revenue'};
    receiveResult(conn, {queryId:'q', rows:[{ts:10,value:100},{ts:2,value:20}]});
    assert.equal(conn.businessMetrics['Revenue - value'],100);
    receiveResult(conn, {queryId:'q', error:'Unavailable'});
    assert.equal(conn.lastMetrics.businessMetrics['Revenue - value'],undefined);
    receiveResult(conn, {queryId:'q', rows:[{value:50}]});
    receiveResult(conn, {queryId:'q', rows:[]});
    assert.equal(conn.businessMetrics['Revenue - value'],undefined);
});
test('webhooks require approved HTTPS origins and public destination addresses', () => {
    const allow=new Set(['https://hooks.slack.com']);
    assert.equal(validateWebhook('https://hooks.slack.com/services/test',allow).hostname,'hooks.slack.com');
    for(const url of ['http://hooks.slack.com','https://evil.example','https://user:pass@hooks.slack.com'])assert.throws(()=>validateWebhook(url,allow));
    for(const ip of ['127.0.0.1','10.0.0.1','169.254.169.254','192.168.1.1','::1','::ffff:127.0.0.1','fe80::1','fc00::1']) assert.equal(publicAddress(ip),false,ip);
    assert.equal(publicAddress('8.8.8.8'),true);
    for (const origin of ['https://127.0.0.1', 'https://[::1]', 'https://169.254.169.254']) {
        assert.throws(() => validateWebhook(origin, new Set([origin])), /Private/);
    }
});
