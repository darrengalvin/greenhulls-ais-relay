const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

const AIS_API_KEY = process.env.AIS_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MARINETRAFFIC_API_KEY = process.env.MARINETRAFFIC_API_KEY || "";

if (!AIS_API_KEY || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing required env vars. Need: AIS_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const BOUNDING_BOXES = [[[54.0, -1.0], [62.5, 4.0]]];

// ── Health tracking ──────────────────────────────────────────────
let stats = {
  aisstream: { messages: 0, vessels: 0, errors: 0, lastMsg: null, lastError: null, connected: false },
  marinetraffic: { messages: 0, vessels: 0, errors: 0, lastMsg: null, lastError: null },
};
const seenVessels = new Map();
let ws = null;
let pingInterval = null;
let reconnectTimer = null;
let connectTime = null;

let reconnectDelay = 15_000;
const RECONNECT_MIN = 15_000;
const RECONNECT_MAX = 300_000;

const STALE_WARN_MS = 120_000;
const STALE_RECONNECT_MS = 300_000;
const EMPTY_CONNECT_MS = 120_000; // reconnect if 0 messages after 2 min

async function updateSourceHealth(source, status) {
  const s = stats[source];
  try {
    await supabase.from("data_source_health").upsert({
      source,
      status,
      last_message_at: s.lastMsg,
      messages_last_hour: s.messages,
      vessels_last_hour: s.vessels,
      error_count: s.errors,
      last_error: s.lastError,
      updated_at: new Date().toISOString(),
    }, { onConflict: "source" });
  } catch (e) {
    console.error(`[${ts()}] Health update failed:`, e.message);
  }
}

// ── AISStream (primary source) ───────────────────────────────────
function scheduleReconnect(delay) {
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connectAisStream, delay);
}

function cleanup() {
  if (pingInterval) { clearInterval(pingInterval); pingInterval = null; }
  if (ws) {
    ws.removeAllListeners();
    try { ws.terminate(); } catch (_) {}
    ws = null;
  }
  stats.aisstream.connected = false;
  connectTime = null;
}

function connectAisStream() {
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

  cleanup();

  console.log(`[${ts()}] Connecting to AISStream.io...`);
  console.log(`[${ts()}] API key: ${AIS_API_KEY.slice(0, 8)}...`);
  seenVessels.clear();
  stats.aisstream.vessels = 0;

  try {
    ws = new WebSocket("wss://stream.aisstream.io/v0/stream");
  } catch (e) {
    console.error(`[${ts()}] WebSocket create failed:`, e.message);
    scheduleReconnect(reconnectDelay);
    return;
  }

  const currentWs = ws;
  let gotRateLimited = false;

  currentWs.on("open", () => {
    if (ws !== currentWs) return;
    console.log(`[${ts()}] AISStream WebSocket open. Sending subscription...`);
    stats.aisstream.connected = true;
    connectTime = Date.now();
    updateSourceHealth("aisstream", "healthy");

    const sub = {
      APIKey: AIS_API_KEY,
      BoundingBoxes: BOUNDING_BOXES,
      FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport"],
    };
    console.log(`[${ts()}] Subscription:`, JSON.stringify(sub).slice(0, 100));
    currentWs.send(JSON.stringify(sub));

    pingInterval = setInterval(() => {
      if (currentWs.readyState === WebSocket.OPEN) {
        currentWs.ping();
      }
    }, 30000);
  });

  currentWs.on("message", async (data) => {
    if (ws !== currentWs) return;
    try {
      const msg = JSON.parse(data.toString());
      const type = msg.MessageType;
      if (type !== "PositionReport" && type !== "StandardClassBPositionReport") return;

      const meta = msg.MetaData;
      const report = msg.Message?.[type];
      if (!meta || !report) return;

      const mmsi = meta.MMSI;
      const lat = report.Latitude ?? meta.latitude;
      const lon = report.Longitude ?? meta.longitude;
      if (!lat || !lon || lat === 91 || lon === 181) return;

      const sog = report.Sog ?? 0;
      if (sog > 102.2) return;

      const cog = report.Cog ?? 0;
      const heading = report.TrueHeading;
      const shipName = (meta.ShipName ?? "").trim();

      await writePosition({
        vesselId: `ais_${mmsi}`,
        mmsi: String(mmsi),
        shipName,
        lat, lon, sog,
        heading: heading && heading < 360 ? heading : cog < 360 ? cog : null,
        cog: cog < 360 ? cog : null,
        source: "ais",
      });

      stats.aisstream.messages++;
      stats.aisstream.lastMsg = new Date().toISOString();
      reconnectDelay = RECONNECT_MIN;

      if (!seenVessels.has(mmsi)) {
        seenVessels.set(mmsi, { name: shipName, source: "aisstream" });
        stats.aisstream.vessels = seenVessels.size;
        console.log(`[${ts()}] ${shipName || mmsi} (${sog.toFixed(1)}kn) at ${lat.toFixed(4)},${lon.toFixed(4)} — total: ${seenVessels.size}`);
      }

    } catch (e) {
      stats.aisstream.errors++;
      stats.aisstream.lastError = e.message;
    }
  });

  currentWs.on("error", (err) => {
    const msg = String(err.message || err);
    console.error(`[${ts()}] AISStream WebSocket error:`, msg);
    stats.aisstream.errors++;
    stats.aisstream.lastError = msg;
    if (msg.includes("429")) gotRateLimited = true;
  });

  currentWs.on("close", (code, reason) => {
    if (ws !== currentWs) return;
    const r = reason ? reason.toString() : 'no reason';
    cleanup();

    if (gotRateLimited) {
      reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
      console.warn(`[${ts()}] Rate limited (429). Backing off — next retry in ${Math.round(reconnectDelay/1000)}s`);
      updateSourceHealth("aisstream", "rate_limited");
    } else {
      reconnectDelay = RECONNECT_MIN;
      console.log(`[${ts()}] AISStream disconnected (code=${code}, reason=${r}). Reconnecting in ${Math.round(reconnectDelay/1000)}s...`);
      updateSourceHealth("aisstream", "down");
    }

    scheduleReconnect(reconnectDelay);
  });
}

// ── Write position to Supabase ───────────────────────────────────
async function writePosition({ vesselId, mmsi, shipName, lat, lon, sog, heading, cog, source }) {
  const { error } = await supabase.from("vessel_positions").upsert({
    vessel_id: vesselId,
    latitude: lat,
    longitude: lon,
    speed_knots: sog,
    heading_degrees: heading,
    course_over_ground: cog,
    source,
    ship_name: shipName || null,
    mmsi: mmsi || null,
    reported_at: new Date().toISOString(),
    received_at: new Date().toISOString(),
  }, { onConflict: "vessel_id" });

  if (error) {
    console.error(`[${ts()}] DB write error:`, error.message);
  }
}

// ── MarineTraffic polling (failover / cross-validation) ──────────
async function pollMarineTraffic() {
  if (!MARINETRAFFIC_API_KEY) return;

  try {
    const url = `https://services.marinetraffic.com/api/exportvessel/v:8/${MARINETRAFFIC_API_KEY}/MINLAT:54/MAXLAT:62.5/MINLON:-1/MAXLON:4/protocol:jsono`;
    const res = await fetch(url);
    if (!res.ok) {
      stats.marinetraffic.errors++;
      stats.marinetraffic.lastError = `HTTP ${res.status}`;
      updateSourceHealth("marinetraffic", "degraded");
      return;
    }

    const vessels = await res.json();
    stats.marinetraffic.lastMsg = new Date().toISOString();
    stats.marinetraffic.messages++;
    stats.marinetraffic.vessels = vessels.length;

    for (const v of vessels) {
      const mmsi = v.MMSI;
      const existing = seenVessels.get(Number(mmsi));

      if (existing && existing.source === "aisstream") continue;

      if (!seenVessels.has(Number(mmsi))) {
        await writePosition({
          vesselId: `ais_${mmsi}`,
          mmsi: String(mmsi),
          shipName: v.SHIPNAME || "",
          lat: Number(v.LAT),
          lon: Number(v.LON),
          sog: Number(v.SPEED) / 10,
          heading: v.HEADING ? Number(v.HEADING) : null,
          cog: v.COURSE ? Number(v.COURSE) : null,
          source: "ais",
        });
      }
    }

    updateSourceHealth("marinetraffic", "healthy");
    console.log(`[${ts()}] MarineTraffic poll: ${vessels.length} vessels`);
  } catch (e) {
    stats.marinetraffic.errors++;
    stats.marinetraffic.lastError = e.message;
    updateSourceHealth("marinetraffic", "degraded");
    console.error(`[${ts()}] MarineTraffic error:`, e.message);
  }
}

// ── Staleness detection + auto-reconnect ─────────────────────────
function checkStaleness() {
  if (!stats.aisstream.connected) return;
  const now = Date.now();

  if (stats.aisstream.lastMsg) {
    const age = now - new Date(stats.aisstream.lastMsg).getTime();
    if (age > STALE_RECONNECT_MS) {
      console.warn(`[${ts()}] AISStream zombie — no data for ${Math.round(age/1000)}s. Forcing reconnect...`);
      updateSourceHealth("aisstream", "down");
      reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
      connectAisStream();
      return;
    }
    if (age > STALE_WARN_MS) {
      console.warn(`[${ts()}] AISStream stale — no data for ${Math.round(age/1000)}s`);
      updateSourceHealth("aisstream", "degraded");
    }
  } else if (connectTime) {
    const connAge = now - connectTime;
    if (connAge > EMPTY_CONNECT_MS) {
      console.warn(`[${ts()}] Connected ${Math.round(connAge/1000)}s ago but 0 messages received. Reconnecting...`);
      updateSourceHealth("aisstream", "empty");
      reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX);
      connectAisStream();
    }
  }
}

function ts() {
  return new Date().toISOString().slice(11, 19);
}

// ── Health + status logging ──────────────────────────────────────
setInterval(() => {
  const s = stats.aisstream;
  const staleAge = s.lastMsg ? Date.now() - new Date(s.lastMsg).getTime() : null;
  const connAge = connectTime ? Math.round((Date.now() - connectTime) / 1000) : null;
  let extra = '';
  if (staleAge) extra += `, last_data=${Math.round(staleAge/1000)}s ago`;
  if (connAge) extra += `, connected_for=${connAge}s`;
  extra += `, backoff=${Math.round(reconnectDelay/1000)}s`;
  console.log(`[${ts()}] AISStream: ${s.messages} msgs, ${seenVessels.size} vessels, ${s.errors} errors, connected=${s.connected}${extra}`);

  if (s.connected && staleAge && staleAge < STALE_WARN_MS) {
    updateSourceHealth("aisstream", "healthy");
  }
  checkStaleness();
}, 60000);

// Poll MarineTraffic every 60s if key is set
if (MARINETRAFFIC_API_KEY) {
  console.log(`[${ts()}] MarineTraffic failover enabled`);
  setInterval(pollMarineTraffic, 60000);
  pollMarineTraffic();
}

// ── HTTP health endpoint ─────────────────────────────────────────
const http = require("http");
const PORT = process.env.PORT || 3000;

http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: stats.aisstream.connected ? "healthy" : "degraded",
    reconnect_delay_s: Math.round(reconnectDelay / 1000),
    sources: {
      aisstream: {
        connected: stats.aisstream.connected,
        messages: stats.aisstream.messages,
        vessels: seenVessels.size,
        errors: stats.aisstream.errors,
        last_message: stats.aisstream.lastMsg,
        connected_since: connectTime ? new Date(connectTime).toISOString() : null,
      },
      marinetraffic: {
        enabled: !!MARINETRAFFIC_API_KEY,
        messages: stats.marinetraffic.messages,
        vessels: stats.marinetraffic.vessels,
        last_message: stats.marinetraffic.lastMsg,
      },
    },
    uptime_seconds: Math.floor(process.uptime()),
  }));
}).listen(PORT, () => {
  console.log(`[${ts()}] GreenHulls AIS Relay v3 — health on :${PORT}`);
  connectAisStream();
});
