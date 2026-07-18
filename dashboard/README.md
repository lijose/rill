# 📊 Rill Real-Time Streaming & Quack Dashboard

The Rill Dashboard is a modernized single-page application and Express/WebSocket bridge designed to monitor live Rill/Quack streaming engine nodes, visualize real-time PyArrow business metrics, configure acoustic sound alerts, and dispatch server-side webhook notifications.

---

## ✨ Features Overview

### 1. 🎛️ Multi-View Glassmorphic Navigation
Once connected to one or more Quack servers (`quack:ip:port`), the dashboard unlocks a sleek top navigation switcher:
- **`[📊 Metrics Console]`**: Displays live KPI metric cards, system resource gauges, memory progress bars, and interactive Chart.js time-series charts (`Last 30s` to `All stored`).
- **`[⚙️ Alert Rules & Settings]`**: Houses acoustic sound controls, webhook endpoints, and dynamic trigger rules management.
- **`[🔔 Alert Logs]`**: A live log table tracking historical alert triggers, complete with an animated notification badge counter.

---

### 2. 🔊 Browser-Native Sound Alert Engine
Built on the **Web Audio API (`AudioContext`)**, the dashboard synthesizes acoustic alarms in real time without requiring external `.mp3` or `.wav` downloads:
- **Tone Selector**: Choose between:
  - **Classic Beep (`beep`)**: Pulsing 800Hz sine wave gated at 250ms intervals.
  - **Warning Siren (`siren`)**: Sawtooth frequency ramping smoothly between 600Hz and 1200Hz.
  - **Gentle Chime (`chime`)**: Exponentially decaying C5 (523Hz) + C6 (1046Hz) bell harmonics.
  - **Urgent Buzzer (`square`)**: Continuous 150Hz square wave buzzer.
- **Duration Control**: Syncs a smooth slider and numeric input box (`1s` to `60s`).
- **Instant Audition**: Use the **🔊 Test Alert Sound** button to preview your configured sound before enabling.

---

### 3. ⚪ Offline & Disconnected Server Grey Tabs
When monitoring multiple remote Quack instances across tabs, the dashboard tracks connection health continuously:
- **Active State**: Healthy tabs show a green status dot (`🟢`) and vibrant colors.
- **Offline / Error State (`.tab-offline-grey`)**: If a server drops connection, exits, or fails authentication, the tab automatically desaturates into a distinct grey style (`⚪`). Hovering over the grey tab displays the exact error reason (`Server Offline / Error: Disconnected`).

---

### 4. 🔗 Server-Side Webhook Proxy (`POST /api/webhook/send`)
Browser security policies (CORS) block frontend web applications from sending direct `POST` requests to third-party endpoints like Slack or Google Chat. The Rill Dashboard solves this with a built-in server proxy:
- **Slack Incoming Webhook**: Automatically formats rich markdown messages with metric values, operators, thresholds, and server addresses.
- **Google Meet / Google Chat**: Formats card/text alerts compatible with Google Chat webhooks.
- **Custom JSON Endpoints**: Relays pure JSON payloads (`{ ruleName, metricField, observedValue, threshold, serverAddress, timestamp }`) to custom backends, incident management platforms, or automated pipelines.
- **Test Delivery Button**: Click **🔗 Test Webhook Delivery** inside the settings view to instantly verify endpoint connectivity.

---

### 5. ⚡ Alert Evaluation Engine & Storage
- **Live Rule Evaluation**: Every incoming metric snapshot (`1s` interval) is evaluated against enabled trigger rules (`>`, `>=`, `<`, `<=`, `==`). Supports both system metrics (`cpu`, `memPct`, `rps`, `latency`) and dynamically registered `businessMetrics`.
- **Cooldown Protection**: Prevents alarm fatigue by enforcing a configurable wait period (`cooldownSeconds`) after a rule triggers before firing again.
- **Persistent State**: All user preferences, trigger rules, and historical log records persist cleanly in `localStorage` across page refreshes.

---

## 🚀 Running the Dashboard

### Prerequisites
- Node.js (`v18+` recommended)
- `npm`

### Start the Server
1. Navigate to the dashboard directory:
   ```bash
   cd dashboard
   ```
2. Install dependencies:
   ```bash
   npm install
   ```
3. Start the Node.js WebSocket Bridge & Express Server:
   ```bash
   npm start
   ```
4. Open `http://localhost:3000` in your browser.

---

## 🔌 API & Proxy Architecture

```
+-----------------------------------------------------------------+
|                       Browser Frontend                          |
|  (app.js - AudioContext, Chart.js, LocalStorage, Rules Engine)  |
+------------------+---------------------------+------------------+
                   | WebSocket (JSON)          | HTTP POST
                   v                           v
+-----------------------------------------------------------------+
|              Express / WebSocket Bridge (server.js)             |
|   + WebSocket <-> Quack TCP Socket (`quack_worker.py`)          |
|   + POST /api/webhook/send (CORS Proxy -> Slack / Google Chat)  |
+-----------------------------------------------------------------+
                   | TCP Socket (Native Quack)
                   v
+-----------------------------------------------------------------+
|          Rill Streaming Engine Node (`quack:ip:9494`)           |
+-----------------------------------------------------------------+
```
