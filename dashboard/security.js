const crypto = require('node:crypto');
const dns = require('node:dns');
const https = require('node:https');
const ipaddr = require('ipaddr.js');

function equalSecret(a, b) {
    const left = crypto.createHash('sha256').update(String(a)).digest();
    const right = crypto.createHash('sha256').update(String(b)).digest();
    return crypto.timingSafeEqual(left, right);
}
function publicAddress(address) {
    let ip = ipaddr.parse(address);
    if (ip.isIPv4MappedAddress?.()) ip = ip.toIPv4Address();
    return ip.range() === 'unicast';
}
function validateWebhook(url, allowedOrigins) {
    const target = new URL(url);
    if (target.protocol !== 'https:' || target.username || target.password || !allowedOrigins.has(target.origin)) {
        throw new Error('Webhook must use HTTPS and an administrator-approved origin');
    }
    const hostname = target.hostname.replace(/^\[|\]$/g, '');
    // Literal IPs bypass https.request's DNS lookup callback.
    if (ipaddr.isValid(hostname) && !publicAddress(hostname)) {
        throw new Error('Private webhook destinations are not allowed');
    }
    return target;
}
function sendWebhook(url, body, allowedOrigins) {
    const target = validateWebhook(url, allowedOrigins);
    return new Promise((resolve, reject) => {
        const request = https.request(target, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            // Validate the actual address used by the socket; no second DNS lookup.
            lookup(hostname, options, callback) {
                dns.lookup(hostname, { all: true }, (error, addresses) => {
                    if (error) return callback(error);
                    if (!addresses.length || addresses.some(a => !publicAddress(a.address))) {
                        return callback(new Error('Private webhook destinations are not allowed'));
                    }
                    const entry = addresses[0];
                    callback(null, options.all ? [entry] : entry.address, entry.family);
                });
            }
        }, response => {
            response.resume();
            const status = response.statusCode;
            if (status < 200 || status >= 300) return reject(new Error(`Webhook returned HTTP ${status}; redirects are disabled`));
            resolve(status);
        });
        request.setTimeout(10000, () => request.destroy(new Error('Webhook timed out')));
        request.on('error', reject);
        request.end(JSON.stringify(body));
    });
}
module.exports = { equalSecret, publicAddress, validateWebhook, sendWebhook };
