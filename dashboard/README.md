# Rill Dashboard

Dashboard version **1.0.1**, shipped with Rill **0.1.2**.
The browser connects to a Node.js WebSocket bridge, which runs a Python Quack
worker for each engine connection. The worker queries the engine's metrics
database; it cannot automatically query arbitrary live input tables.

## Setup

Requirements: Node.js **18+**, npm, and a Python environment with compatible
DuckDB installed. Follow the [root setup guide](../README.md#install) first.

From the repository root, in the terminal that will run Node:

```sh
. .venv/bin/activate
cd dashboard
npm ci --ignore-scripts
npm start
```

Open `http://127.0.0.1:3000`, enter the running engine's Quack address and token,
and select **Connect Console**. To start a development engine, use
`python examples/quack_dashboard_demo.py` in another activated terminal. Its
Quack address is `quack:127.0.0.1:9494` and token is `demo`; the demo server binds
to all interfaces, so run it only in a trusted development environment.

The bridge selects Python using `RILL_PYTHON`, then `VIRTUAL_ENV` or
`CONDA_PREFIX`, then `python3` on PATH. `RILL_PYTHON` accepts an executable path.
`RILL_EXTENSION_DIRECTORY` optionally selects a preinstalled DuckDB extension
cache. Chart.js, Hammer.js, and the zoom plugin are served from local, pinned
npm packages.

## Views

- **Overview:** throughput, latency, CPU, and system memory; expandable
  Arrow/DuckDB memory details; scheduled-query widgets and resource history.
- **Queries:** run one read-only SQL statement, inspect results, configure
  Line/Bar/Scatter charts, save SQL, and schedule repeated queries. Advanced
  controls are expandable. Use `remote_db.rill_metrics`, `remote_db.rill_alerts`,
  or a persisted SQL output table such as `remote_db.tier_revenue`.
- **Alert settings:** configure thresholds, per-rule cooldowns, sounds, and webhooks.
- **Alert Logs:** engine/user alerts and browser rule events, with severity and
  source filters. Browser rule events are not persisted to the engine database.

The default theme is fixed. Options contains **Disconnect Server**. Connection
state, scheduled results, and cooldown timers are isolated per connection.
Background tabs and table-only queries still evaluate business alerts. Socket
reconnects restore schedules and stopped-query choices within the current page
session. Page reload does not restore engine credentials or running schedules.
Rules, sound/webhook settings, saved SQL, and the bounded local alert log persist
in browser localStorage.

## Alert behavior

Default CPU and memory rules have a **15-second** cooldown. The new-rule form
starts at **10 seconds** and accepts zero. Cooldown is per rule and connection;
timers reset on page reload and separate browser sessions have separate timers.
While a threshold remains exceeded, a rule may fire again after its cooldown.

Engine and user alerts have **no cooldown or duplicate suppression**. Error and
warning events produce toasts; error events also invoke the sound handler. The
current engine-error path does not check the sound-enabled preference, unlike
browser threshold rules. See [operations notes](../docs/operations.md) for this
known limitation and alert-volume risks.

Sound options are Classic Beep, Warning Siren, Gentle Chime, and Urgent Buzzer
(`buzzer`); duration controls allow **1–60 seconds**. Webhook formats include
Slack, Google Chat, and custom JSON. The **Test Webhook Delivery** action sends
a test request to the configured destination.

## Access and webhook restrictions

The Node server defaults to **127.0.0.1:3000**. `PORT` overrides the port.
Remote binding requires `RILL_DASHBOARD_TOKEN`; the dashboard exchanges it for
an HttpOnly, SameSite=Strict cookie valid for eight hours. This is separate from
an engine's Quack token. For remote deployment, use HTTPS via a trusted reverse
proxy and set exact allowed hosts/origins:

```sh
HOST=0.0.0.0 \
RILL_DASHBOARD_TOKEN='<a-long-random-secret>' \
RILL_ALLOWED_HOSTS='rill.example.com' \
RILL_ALLOWED_ORIGINS='https://rill.example.com' \
RILL_COOKIE_SECURE=1 \
npm start
```

`RILL_WEBHOOK_ORIGINS` is a comma-separated allowlist of exact HTTPS origins;
defaults are `https://hooks.slack.com,https://chat.googleapis.com`. Private or
reserved IP addresses, URL credentials, and redirects are rejected. Custom
endpoints must also be administrator-approved public HTTPS origins.

The SQL validator rejects destructive statements and multiple statements, but
it does not sandbox SQL functions or file access. Dashboard access is intended
for trusted administrators.

## Verification

```sh
npm test
```

Tests cover rendering of untrusted values, per-connection results and alerts,
reconnection, worker replacement, authentication, origins, and webhook validation.
See [migration notes](../docs/review-changes.md) and
[operations notes](../docs/operations.md) for storage limits and verification scope.
