const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');

function getPythonBinary() {
    if (process.env.VIRTUAL_ENV) {
        const p = path.join(process.env.VIRTUAL_ENV, 'bin', 'python3');
        if (fs.existsSync(p)) return p;
    }
    if (process.env.CONDA_PREFIX) {
        const p = path.join(process.env.CONDA_PREFIX, 'bin', 'python3');
        if (fs.existsSync(p)) return p;
    }
    const condaDefault = '/home/lijo/miniconda3/bin/python3';
    if (fs.existsSync(condaDefault)) return condaDefault;
    return 'python3';
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Map of active Quack workers per WebSocket connection: ws -> Map<tabId, ChildProcess>
const activeWorkers = new Map();

wss.on('connection', (ws) => {
    activeWorkers.set(ws, new Map());

    ws.on('message', (message) => {
        try {
            const req = JSON.parse(message);
            const workersMap = activeWorkers.get(ws);

            if (req.action === 'connect') {
                const { tabId, address, token } = req;
                // Kill existing worker for this tab if already open
                if (workersMap.has(tabId)) {
                    workersMap.get(tabId).kill();
                    workersMap.delete(tabId);
                }

                const pythonBin = getPythonBinary();
                console.log(`[Dashboard] Connecting tab '${tabId}' to Quack server: ${address} using ${pythonBin}`);
                const args = ['quack_worker.py', '--address', address];
                if (token) {
                    args.push('--token', token);
                }

                const worker = spawn(pythonBin, args, { cwd: __dirname });
                workersMap.set(tabId, worker);

                worker.stdout.on('data', (data) => {
                    const lines = data.toString().split('\n').filter(Boolean);
                    for (const line of lines) {
                        try {
                            const parsed = JSON.parse(line);
                            if (parsed.error) {
                                console.error(`[Worker Quack Error ${tabId}] ${parsed.error}`);
                            }
                            ws.send(JSON.stringify({ tabId, payload: parsed }));
                        } catch (err) {
                            console.error(`[Worker Non-JSON Output] ${line}`);
                        }
                    }
                });

                worker.stderr.on('data', (data) => {
                    console.error(`[Worker Error ${tabId}] ${data.toString()}`);
                });

                worker.on('close', (code) => {
                    console.log(`[Worker Closed ${tabId}] Exit code: ${code}`);
                    workersMap.delete(tabId);
                    if (ws.readyState === ws.OPEN) {
                        ws.send(JSON.stringify({
                            tabId,
                            payload: { error: `Quack connection closed (Exit code ${code})` }
                        }));
                    }
                });
            } else if (req.action === 'disconnect') {
                const { tabId } = req;
                const workersMap = activeWorkers.get(ws);
                if (workersMap && workersMap.has(tabId)) {
                    console.log(`[Dashboard] Disconnecting tab '${tabId}'`);
                    workersMap.get(tabId).kill();
                    workersMap.delete(tabId);
                }
            }
        } catch (err) {
            console.error('[WebSocket Error processing message]:', err);
        }
    });

    ws.on('close', () => {
        const workersMap = activeWorkers.get(ws);
        if (workersMap) {
            for (const [tabId, worker] of workersMap.entries()) {
                worker.kill();
            }
            activeWorkers.delete(ws);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Dashboard server running at http://localhost:${PORT}`);
    console.log(`WebSocket Quack Bridge listening on ws://localhost:${PORT}`);
});
