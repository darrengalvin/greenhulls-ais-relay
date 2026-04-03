const WebSocket = require("ws");
const { createClient } = require("@supabase/supabase-js");

// ── Config from environment variables ──────────────────────────
const AIS_API_KEY = process.env.AIS_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!AIS_API_KEY || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error("Missing environment variables:");
  console.error("  AIS_API_KEY:", AIS_API_KEY ? "set" : "MISSING");
  console.error("  SUPABASE_URL:", SUPABASE_URL ? "set" : "MISSING");
  console.error("  SUPABASE_SERVICE_ROLE_KEY:", SUPABASE_KEY ? "set" : "MISSING");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// Bounding box: covers all GreenHulls installations in the North Sea
// Britannia (58.07, 0.99), Penguins (61.58, 1.55), GPIII (58.36, 0.87)
// Dogger Bank A (55.04, 2.84), Dogger Bank B (54.98, 2.23)
const BOUNDING_BOXES = [[[54.0, -1.0], [62.5, 4.0]]];

let messageCount = 0;
let vesselCount = 0;
const seenVessels = new Map(); // mmsi → last seen
let ws = null;

function connect() {
  console.log(`[${ts()}] Connecting to AISStream.io...`);

  ws = new WebSocket("wss://stream.aisstream.io/v0/stream");

  ws.on("open", () => {
    console.log(`[${ts()}] Connected. Subscribing to North Sea bounding box...`);

    ws.send(JSON.stringify({
      APIKey: AIS_API_KEY,
      BoundingBoxes: BOUNDING_BOXES,
      FilterMessageTypes: ["PositionReport", "StandardClassBPositionReport"],
    }));
  });

  ws.on("message", async (data) => {
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

      const vesselId = `ais_${mmsi}`;

      const { error } = await supabase.from("vessel_positions").upsert({
        vessel_id: vesselId,
        latitude: lat,
        longitude: lon,
        speed_knots: sog,
        heading_degrees: heading && heading < 360 ? heading : cog < 360 ? cog : null,
        course_over_ground: cog < 360 ? cog : null,
        source: "ais",
        reported_at: new Date().toISOString(),
        received_at: new Date().toISOString(),
      }, { onConflict: "vessel_id" });

      if (error) {
        console.error(`[${ts()}] Supabase error:`, error.message);
        return;
      }

      messageCount++;
      if (!seenVessels.has(mmsi)) {
        seenVessels.set(mmsi, { name: shipName, sog, lat, lon });
        vesselCount = seenVessels.size;
        console.log(`[${ts()}] New vessel: ${shipName || mmsi} (${sog.toFixed(1)}kn) at ${lat.toFixed(4)}, ${lon.toFixed(4)} — total: ${vesselCount}`);
      } else {
        seenVessels.set(mmsi, { name: shipName, sog, lat, lon });
      }

      if (messageCount % 100 === 0) {
        console.log(`[${ts()}] ${messageCount} messages, ${vesselCount} unique vessels`);
      }

    } catch (e) {
      console.error(`[${ts()}] Parse error:`, e.message);
    }
  });

  ws.on("error", (err) => {
    console.error(`[${ts()}] WebSocket error:`, err.message);
  });

  ws.on("close", (code, reason) => {
    console.log(`[${ts()}] Disconnected (code=${code}). Reconnecting in 10s...`);
    setTimeout(connect, 10000);
  });
}

function ts() {
  return new Date().toISOString().slice(11, 19);
}

// ── Health check HTTP server (Railway needs a port) ────────────
const http = require("http");
const PORT = process.env.PORT || 3000;

http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "running",
    connected: ws?.readyState === WebSocket.OPEN,
    messages: messageCount,
    vessels: vesselCount,
    uptime_seconds: Math.floor(process.uptime()),
  }));
}).listen(PORT, () => {
  console.log(`[${ts()}] Health check on port ${PORT}`);
  console.log(`[${ts()}] GreenHulls AIS Relay starting...`);
  connect();
});
