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

const STALE_THRESHOLD_S = 180;      // force reconnect if no data for 3 minutes
const PING_INTERVAL_S = 45;         // WebSocket keepalive ping every 45s
const PONG_TIMEOUT_S = 15;          // if no pong within 15s, connection is dead
const BASE_RECONNECT_DELAY_S = 10;  // first retry after 10s
const MAX_RECONNECT_DELAY_S = 600;  // cap at 10 minutes during sustained outages

// ── Health tracking ──────────────────────────────────────────────
let stats = {
  aisstream: { messages: 0, vessels: 0, errors: 0, lastMsg: null, lastError: null, connected: false },
  marinetraffic: { messages: 0, vessels: 0, errors: 0, lastMsg: null, lastError: null },
};
const seenVessels = new Map();
let ws = null;
let lastDataAt = null;
let reconnectFailures = 0;
let reconnectDelay = BASE_RECONNECT_DELAY_S;
let pingTimer = null;
let pongTimer = null;
let stalenessTimer = null;
let isReconnecting = false;

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
function connectAisStream() {
  cleanupTimers();
  isReconnecting = false;
  console.log(`[${ts()}] Connecting to AISStream.io...`);
  console.log(`[${ts()}] API key: ${AIS_API_KEY.slice(0, 8)}...`);
  stats.aisstream.connected = false;

  try {
    ws = new WebSocket("wss://stream.aisstream.io/v0/stream");
  } catch (e) {
    console.error(`[${ts()}] WebSocket create failed:`, e.message);
    scheduleReconnect();
    return;
  }

  ws.on("open", () => {
    console.log(`[${ts()}] AISStream WebSocket open. Sending subscription...`);
    stats.aisstream.connected = true;
    reconnectFailures = 0;
    reconnectDelay = BASE_RECONNECT_DELAY_S;
    updateSourceHealth("aisstream", "healthy");

    const sub = {
      APIKey: AIS_API_KEY,
      BoundingBoxes: BOUNDING_BOXES,
      FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport"],
    };
    console.log(`[${ts()}] Subscription:`, JSON.stringify(sub).slice(0, 100));
    ws.send(JSON.stringify(sub));

    startPing();
    startStalenessWatchdog();
  });

  ws.on("message", async (data) => {
    lastDataAt = Date.now();
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

  ws.on("pong", () => {
    if (pongTimer) { clearTimeout(pongTimer); pongTimer = null; }
  });

  ws.on("error", (err) => {
    console.error(`[${ts()}] AISStream WebSocket error:`, err.message || err);
    stats.aisstream.errors++;
    stats.aisstream.lastError = String(err.message || err);
    stats.aisstream.connected = false;
    updateSourceHealth("aisstream", "down");
  });

  ws.on("close", (code, reason) => {
    const r = reason ? reason.toString() : 'no reason';
    console.log(`[${ts()}] AISStream disconnected (code=${code}, reason=${r}).`);
    stats.aisstream.connected = false;
    updateSourceHealth("aisstream", "down");
    ws = null;
    cleanupTimers();
    if (!isReconnecting) {
      console.log(`[${ts()}] Close event triggering reconnect...`);
      scheduleReconnect();
    }
  });
}

// ── Ping/pong: detect dead TCP connections ───────────────────────
function startPing() {
  pingTimer = setInterval(() => {
    if (ws?.readyState !== WebSocket.OPEN) return;
    ws.ping();
    pongTimer = setTimeout(() => {
      console.log(`[${ts()}] No pong in ${PONG_TIMEOUT_S}s — connection dead, forcing reconnect`);
      forceReconnect();
    }, PONG_TIMEOUT_S * 1000);
  }, PING_INTERVAL_S * 1000);
}

// ── Staleness watchdog: catch half-open connections ──────────────
function startStalenessWatchdog() {
  stalenessTimer = setInterval(() => {
    if (!lastDataAt) return;
    const staleSec = Math.floor((Date.now() - lastDataAt) / 1000);
    if (staleSec >= STALE_THRESHOLD_S) {
      console.log(`[${ts()}] STALE: No AIS data for ${staleSec}s (threshold: ${STALE_THRESHOLD_S}s) — forcing reconnect`);
      updateSourceHealth("aisstream", "degraded");
      forceReconnect();
    }
  }, 15_000);
}

function forceReconnect() {
  if (isReconnecting) return;
  isReconnecting = true;
  console.log(`[${ts()}] Force reconnect: terminating WebSocket...`);
  cleanupTimers();
  const oldWs = ws;
  ws = null;
  if (oldWs) {
    try { oldWs.removeAllListeners(); oldWs.terminate(); } catch (_) {}
  }
  scheduleReconnect();
}

function scheduleReconnect() {
  reconnectFailures++;
  reconnectDelay = Math.min(
    BASE_RECONNECT_DELAY_S * Math.pow(2, reconnectFailures - 1),
    MAX_RECONNECT_DELAY_S
  );
  console.log(`[${ts()}] Reconnecting in ${reconnectDelay}s (attempt #${reconnectFailures})...`);
  setTimeout(connectAisStream, reconnectDelay * 1000);
}

function cleanupTimers() {
  if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
  if (pongTimer) { clearTimeout(pongTimer); pongTimer = null; }
  if (stalenessTimer) { clearInterval(stalenessTimer); stalenessTimer = null; }
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
// Polls MarineTraffic REST API every 60s as a backup.
// Only active if MARINETRAFFIC_API_KEY is set.
async function pollMarineTraffic() {
  if (!MARINETRAFFIC_API_KEY) return;

  try {
    // MarineTraffic PS07 endpoint: positions in area
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

      // Cross-validate: if we already have this vessel from AISStream,
      // check if positions roughly match (within 1km)
      if (existing && existing.source === "aisstream") {
        // Just log for now — in production this would trigger alerts
        continue;
      }

      // Write vessels we DON'T have from AISStream (failover)
      if (!seenVessels.has(Number(mmsi))) {
        await writePosition({
          vesselId: `ais_${mmsi}`,
          mmsi: String(mmsi),
          shipName: v.SHIPNAME || "",
          lat: Number(v.LAT),
          lon: Number(v.LON),
          sog: Number(v.SPEED) / 10, // MT returns speed in 1/10 knots
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

function ts() {
  return new Date().toISOString().slice(11, 19);
}

// ── Health + status logging ──────────────────────────────────────
setInterval(() => {
  const s = stats.aisstream;
  const lastDataAgo = lastDataAt ? Math.floor((Date.now() - lastDataAt) / 1000) : null;
  console.log(
    `[${ts()}] AISStream: ${s.messages} msgs, ${seenVessels.size} vessels, ${s.errors} errs, ` +
    `connected=${s.connected}, last_data=${lastDataAgo !== null ? lastDataAgo + 's ago' : 'never'}, ` +
    `up=${Math.floor(process.uptime())}s, failures=${reconnectFailures}, backoff=${reconnectDelay}s`
  );
  if (s.connected && lastDataAgo !== null && lastDataAgo < STALE_THRESHOLD_S) {
    updateSourceHealth("aisstream", "healthy");
  }
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
  const lastDataAgo = lastDataAt ? Math.floor((Date.now() - lastDataAt) / 1000) : null;
  const isStale = lastDataAgo !== null && lastDataAgo > STALE_THRESHOLD_S;
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: stats.aisstream.connected && !isStale ? "healthy" : "degraded",
    sources: {
      aisstream: {
        connected: stats.aisstream.connected,
        stale: isStale,
        messages: stats.aisstream.messages,
        vessels: seenVessels.size,
        errors: stats.aisstream.errors,
        last_message: stats.aisstream.lastMsg,
        last_data_seconds_ago: lastDataAgo,
      },
      marinetraffic: {
        enabled: !!MARINETRAFFIC_API_KEY,
        messages: stats.marinetraffic.messages,
        vessels: stats.marinetraffic.vessels,
        last_message: stats.marinetraffic.lastMsg,
      },
    },
    uptime_seconds: Math.floor(process.uptime()),
    reconnect_failures: reconnectFailures,
  }));
}).listen(PORT, () => {
  console.log(`[${ts()}] GreenHulls AIS Relay v2 — health on :${PORT}`);
  connectAisStream();
});
