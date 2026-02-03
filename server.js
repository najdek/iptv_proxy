import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { pipeline } from "stream";
import { promisify } from "util";
import crypto from "crypto";
import { spawn } from "child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = process.env.PORT ? Number(process.env.PORT) : 8787;
const pipe = promisify(pipeline);

const dataDir = path.join(__dirname, "data");
const configPath = path.join(dataDir, "config.json");

const defaultConfig = {
  m3uUrl: "",
  epgUrl: "",
  allowedHosts: [],
  maxStreams: 1,
  denyImageUrl: "",
  denyImagePath: ""
};

let config = loadConfig();

const logBuffer = [];
const LOG_LIMIT = 400;

const streams = [];
const LOCK_TTL_MS = 20_000;
const denyCache = {
  url: "",
  buffer: null,
  contentType: "image/svg+xml",
  fetchedAt: 0
};
const denyVideoState = {
  key: "",
  building: null
};
const denyVideoPath = path.join(dataDir, "deny.ts");
const ffmpegState = {
  available: null,
  checkedAt: 0
};

app.use(express.json({ limit: "1mb" }));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") {
    res.status(204).end();
    return;
  }
  next();
});

app.use(express.static(path.join(__dirname, "public")));

function log(level, message, meta = null) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    message,
    meta
  };
  logBuffer.push(entry);
  if (logBuffer.length > LOG_LIMIT) {
    logBuffer.shift();
  }
  if (level === "error") {
    console.error(message, meta || "");
  } else {
    console.log(message, meta || "");
  }
}

function loadConfig() {
  try {
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    if (!fs.existsSync(configPath)) {
      fs.writeFileSync(configPath, JSON.stringify(defaultConfig, null, 2));
      return { ...defaultConfig };
    }
    const raw = fs.readFileSync(configPath, "utf8");
    const parsed = JSON.parse(raw);
    return { ...defaultConfig, ...parsed };
  } catch (err) {
    console.error("Failed to load config:", err);
    return { ...defaultConfig };
  }
}

function saveConfig(nextConfig) {
  config = { ...defaultConfig, ...nextConfig };
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  denyVideoState.key = "";
}

async function checkFfmpeg() {
  const now = Date.now();
  if (ffmpegState.available !== null && now - ffmpegState.checkedAt < 60_000) {
    return ffmpegState.available;
  }
  const available = await new Promise((resolve) => {
    const child = spawn("ffmpeg", ["-version"], { stdio: "ignore" });
    child.on("error", () => resolve(false));
    child.on("exit", (code) => resolve(code === 0));
  });
  ffmpegState.available = available;
  ffmpegState.checkedAt = now;
  return available;
}

function getDenySvg() {
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="1280" height="720" viewBox="0 0 1280 720">
  <defs>
    <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
      <stop offset="0%" stop-color="#1a2a33"/>
      <stop offset="50%" stop-color="#1c3b47"/>
      <stop offset="100%" stop-color="#2f5b6b"/>
    </linearGradient>
    <radialGradient id="glow" cx="0.2" cy="0.1" r="0.6">
      <stop offset="0%" stop-color="#ff7a59" stop-opacity="0.6"/>
      <stop offset="100%" stop-color="#ff7a59" stop-opacity="0"/>
    </radialGradient>
  </defs>
  <rect width="1280" height="720" fill="url(#bg)" />
  <rect width="1280" height="720" fill="url(#glow)" />
  <g font-family="Space Grotesk, Arial, sans-serif" fill="#f7f4f2">
    <text x="120" y="300" font-size="64" font-weight="700">Too many streams</text>
    <text x="120" y="360" font-size="28" fill="#d5dee6">This provider allows limited concurrent playback.</text>
    <text x="120" y="410" font-size="24" fill="#ffcfbe">Close another player to resume.</text>
  </g>
</svg>`;
}

function decodeDataUrl(dataUrl) {
  const match = /^data:([^;,]+)?(;base64)?,(.*)$/i.exec(dataUrl || "");
  if (!match) return null;
  const contentType = match[1] || "application/octet-stream";
  const isBase64 = Boolean(match[2]);
  const data = match[3] || "";
  const buffer = isBase64 ? Buffer.from(data, "base64") : Buffer.from(decodeURIComponent(data));
  return { buffer, contentType };
}

async function loadDenyImage() {
  const url = config.denyImageUrl || "";
  if (config.denyImagePath && fs.existsSync(config.denyImagePath)) {
    const buffer = fs.readFileSync(config.denyImagePath);
    const ext = path.extname(config.denyImagePath).toLowerCase();
    const contentType = ext === ".png" ? "image/png" : "image/jpeg";
    denyCache.url = "file";
    denyCache.buffer = buffer;
    denyCache.contentType = contentType;
    denyCache.fetchedAt = Date.now();
    return denyCache;
  }
  const now = Date.now();
  if (denyCache.buffer && denyCache.url === url && now - denyCache.fetchedAt < 5 * 60_000) {
    return denyCache;
  }
  if (!url) {
    denyCache.url = "";
    denyCache.buffer = Buffer.from(getDenySvg(), "utf8");
    denyCache.contentType = "image/svg+xml";
    denyCache.fetchedAt = now;
    return denyCache;
  }
  if (url.startsWith("data:")) {
    const decoded = decodeDataUrl(url);
    if (decoded) {
      denyCache.url = url;
      denyCache.buffer = decoded.buffer;
      denyCache.contentType = decoded.contentType;
      denyCache.fetchedAt = now;
      return denyCache;
    }
  }
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const buffer = Buffer.from(await response.arrayBuffer());
    denyCache.url = url;
    denyCache.buffer = buffer;
    denyCache.contentType = response.headers.get("content-type") || "image/jpeg";
    denyCache.fetchedAt = now;
    return denyCache;
  } catch (err) {
    log("error", "Failed to load deny image", { url, error: String(err) });
    denyCache.url = "";
    denyCache.buffer = Buffer.from(getDenySvg(), "utf8");
    denyCache.contentType = "image/svg+xml";
    denyCache.fetchedAt = now;
    return denyCache;
  }
}

async function ensureDenyVideo() {
  const key = config.denyImagePath || config.denyImageUrl || "default";
  if (denyVideoState.building) return denyVideoState.building;
  if (denyVideoState.key === key && fs.existsSync(denyVideoPath)) {
    return true;
  }
  denyVideoState.building = new Promise(async (resolve) => {
    try {
      const ffmpegOk = await checkFfmpeg();
      if (!ffmpegOk) {
        resolve(false);
        return;
      }
      if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true });
      }
      let inputPath = "";
      const imageUrl = config.denyImageUrl || "";
      if (config.denyImagePath && fs.existsSync(config.denyImagePath)) {
        const ext = path.extname(config.denyImagePath).toLowerCase();
        inputPath = path.join(dataDir, `deny-image${ext}`);
        fs.copyFileSync(config.denyImagePath, inputPath);
      }
      if (!inputPath && imageUrl) {
        if (imageUrl.startsWith("data:")) {
          const decoded = decodeDataUrl(imageUrl);
          if (decoded) {
            const ext = decoded.contentType.includes("png") ? "png" : "jpg";
            inputPath = path.join(dataDir, `deny-image.${ext}`);
            fs.writeFileSync(inputPath, decoded.buffer);
          }
        } else {
          const response = await fetch(imageUrl);
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          const buffer = Buffer.from(await response.arrayBuffer());
          const contentType = response.headers.get("content-type") || "";
          const ext = contentType.includes("png") ? "png" : "jpg";
          inputPath = path.join(dataDir, `deny-image.${ext}`);
          fs.writeFileSync(inputPath, buffer);
        }
      }

      const args = [];
      if (inputPath) {
        args.push(
          "-y",
          "-loop",
          "1",
          "-i",
          inputPath,
          "-f",
          "lavfi",
          "-i",
          "anullsrc=channel_layout=stereo:sample_rate=48000",
          "-t",
          "4",
          "-vf",
          "scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2,format=yuv420p",
          "-r",
          "25",
          "-c:v",
          "libx264",
          "-profile:v",
          "baseline",
          "-level:v",
          "3.0",
          "-preset",
          "veryfast",
          "-tune",
          "stillimage",
          "-c:a",
          "aac",
          "-b:a",
          "96k",
          "-shortest",
          "-f",
          "mpegts",
          denyVideoPath
        );
      } else {
        args.push(
          "-y",
          "-f",
          "lavfi",
          "-i",
          "color=c=0x1a2a33:s=1280x720:d=4",
          "-f",
          "lavfi",
          "-i",
          "anullsrc=channel_layout=stereo:sample_rate=48000",
          "-vf",
          "format=yuv420p",
          "-r",
          "25",
          "-c:v",
          "libx264",
          "-profile:v",
          "baseline",
          "-level:v",
          "3.0",
          "-preset",
          "veryfast",
          "-tune",
          "stillimage",
          "-c:a",
          "aac",
          "-b:a",
          "96k",
          "-shortest",
          "-f",
          "mpegts",
          denyVideoPath
        );
      }

      const ok = await new Promise((resolveSpawn) => {
        const child = spawn("ffmpeg", args, { stdio: "ignore" });
        child.on("error", (err) => {
          log("error", "ffmpeg failed to start", { error: String(err) });
          resolveSpawn(false);
        });
        child.on("exit", (code) => resolveSpawn(code === 0));
      });

      if (!ok) {
        resolve(false);
        return;
      }
      denyVideoState.key = key;
      log("info", "Deny video built", { path: denyVideoPath });
      resolve(true);
    } catch (err) {
      log("error", "Failed to build deny video", { error: String(err) });
      resolve(false);
    } finally {
      denyVideoState.building = null;
    }
  });
  return denyVideoState.building;
}

function pruneExpiredStreams() {
  const now = Date.now();
  for (let i = streams.length - 1; i >= 0; i -= 1) {
    if (now - streams[i].lastSeen >= LOCK_TTL_MS) {
      streams.splice(i, 1);
    }
  }
}

function touchStream(stream) {
  stream.lastSeen = Date.now();
}

function getStreamByToken(token) {
  return streams.find((item) => item.token === token) || null;
}

function getMaxStreams() {
  const val = Number(config.maxStreams || 1);
  if (!Number.isFinite(val) || val < 1) return 1;
  return Math.floor(val);
}

function getClientId(req) {
  const headerId = req.get("x-client-id");
  const queryId = req.query.clientId;
  return headerId || queryId || "";
}

function getToken(req) {
  const headerToken = req.get("x-stream-token");
  const queryToken = req.query.token;
  return headerToken || queryToken || "";
}

function isHostAllowed(targetUrl) {
  if (!config.allowedHosts || config.allowedHosts.length === 0) {
    return true;
  }
  try {
    const host = new URL(targetUrl).host;
    return config.allowedHosts.includes(host);
  } catch {
    return false;
  }
}

function ensureLock(req, res, targetUrl = "") {
  const token = getToken(req);
  if (!token) {
    res.status(401).json({ error: "Missing stream token" });
    return null;
  }
  pruneExpiredStreams();
  let stream = getStreamByToken(token);
  if (!stream) {
    if (streams.length >= getMaxStreams()) {
      if (isPlaylistUrl(targetUrl)) {
        sendDenyPlaylist(req, res);
        return null;
      }
      res.status(429).json({
        error: "Maximum concurrent streams reached",
        activeCount: streams.length
      });
      return null;
    }
    stream = {
      token,
      lastSeen: Date.now(),
      label: "auto",
      clientId: getClientIdentity(req),
      channelKey: ""
    };
    streams.push(stream);
    log("info", "Stream lock re-acquired", { clientId: stream.clientId });
  }
  if (!applyChannelLock(req, res, targetUrl, stream)) return null;
  touchStream(stream);
  return token;
}

function autoAcquireLock(req, res, { label = "auto", clientId = "" } = {}) {
  pruneExpiredStreams();
  if (streams.length >= getMaxStreams()) {
    return null;
  }
  const token = crypto.randomUUID();
  const stream = {
    token,
    lastSeen: Date.now(),
    label,
    clientId,
    channelKey: ""
  };
  streams.push(stream);
  log("info", "Stream lock auto-acquired", { label, clientId });
  return token;
}

function getClientIdentity(req) {
  const explicit = getClientId(req);
  if (explicit) return explicit;
  const forwarded = req.headers["x-forwarded-for"];
  if (typeof forwarded === "string" && forwarded.length > 0) {
    return forwarded.split(",")[0].trim();
  }
  return req.ip || "";
}

function isPlaylistUrl(targetUrl) {
  try {
    const { pathname } = new URL(targetUrl);
    return pathname.endsWith(".m3u8") || pathname.endsWith(".m3u");
  } catch {
    return false;
  }
}

function getChannelKey(targetUrl) {
  try {
    const parsed = new URL(targetUrl);
    const parts = parsed.pathname.split("/");
    parts.pop();
    const dir = parts.join("/") || "/";
    return `${parsed.origin}${dir}`;
  } catch {
    return "";
  }
}

function applyChannelLock(req, res, targetUrl, stream) {
  if (!targetUrl || !isPlaylistUrl(targetUrl)) {
    return true;
  }
  const channelKey = getChannelKey(targetUrl);
  if (!channelKey) return true;
  if (!stream.channelKey) {
    stream.channelKey = channelKey;
    return true;
  }
  if (stream.channelKey !== channelKey) {
    sendDenyPlaylist(req, res);
    return false;
  }
  return true;
}

function sendDenyPlaylist(req, res) {
  const base = `${req.protocol}://${req.get("host")}`;
  const playlist = [
    "#EXTM3U",
    "#EXT-X-VERSION:3",
    "#EXT-X-TARGETDURATION:4",
    "#EXT-X-MEDIA-SEQUENCE:0",
    "#EXT-X-ALLOW-CACHE:NO",
    "#EXT-X-PLAYLIST-TYPE:VOD",
    "#EXTINF:4.0,",
    `${base}/deny/segment`,
    "#EXT-X-ENDLIST"
  ].join("\n");
  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.setHeader("Cache-Control", "no-store");
  res.status(200).send(playlist);
}

function rewritePlaylist(content, baseUrl, token, proxyBase) {
  const lines = content.split(/\r?\n/);
  const rewritten = lines.map((line) => {
    const trimmed = line.trim();
    if (!trimmed) return line;
    if (trimmed.startsWith("#")) {
      return rewriteTagLine(line, baseUrl, token);
    }
    try {
      const resolved = new URL(trimmed, baseUrl).toString();
      return buildProxyUrl(resolved, token, proxyBase);
    } catch {
      return line;
    }
  });
  return rewritten.join("\n");
}

function rewriteTagLine(line, baseUrl, token, proxyBase) {
  if (!line.includes("URI=\"")) {
    return line;
  }
  return line.replace(/URI=\"([^\"]+)\"/g, (match, uri) => {
    try {
      const resolved = new URL(uri, baseUrl).toString();
      return `URI=\"${buildProxyUrl(resolved, token, proxyBase)}\"`;
    } catch {
      return match;
    }
  });
}

function buildProxyUrl(targetUrl, token, proxyBase) {
  const encoded = encodeURIComponent(targetUrl);
  const tokenPart = token ? `&token=${encodeURIComponent(token)}` : "";
  const path = `/proxy/hls?u=${encoded}${tokenPart}`;
  if (!proxyBase) return path;
  return `${proxyBase}${path}`;
}

async function proxyRequest(req, res, targetUrl, { rewrite = false, token = "", proxyBase = "" } = {}) {
  if (!isHostAllowed(targetUrl)) {
    res.status(403).json({ error: "Target host is not allowed" });
    return;
  }

  let upstream;
  try {
    upstream = await fetch(targetUrl, {
      headers: {
        "User-Agent": "iptv-proxy/0.1",
        "Accept": req.get("accept") || "*/*"
      }
    });
  } catch (err) {
    log("error", "Fetch failed", { targetUrl, error: String(err) });
    res.status(502).json({ error: "Upstream fetch failed" });
    return;
  }

  if (!upstream.ok) {
    res.status(upstream.status).json({ error: "Upstream error" });
    return;
  }

  const contentType = upstream.headers.get("content-type") || "";
  const isPlaylist = contentType.includes("mpegurl") || targetUrl.toLowerCase().includes(".m3u8");

  if (rewrite && isPlaylist) {
    const text = await upstream.text();
    const rewritten = rewritePlaylist(text, upstream.url || targetUrl, token, proxyBase);
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.status(200).send(rewritten);
    return;
  }

  res.status(200);
  if (contentType) {
    res.setHeader("Content-Type", contentType);
  }
  try {
    await pipe(upstream.body, res);
  } catch (err) {
    const message = String(err);
    if (message.includes("ERR_STREAM_PREMATURE_CLOSE")) {
      log("info", "Client closed stream early", { targetUrl });
      return;
    }
    log("error", "Stream pipeline error", { targetUrl, error: message });
  }
}

app.get("/api/config", (req, res) => {
  res.json(config);
});

app.post(
  "/api/deny-image",
  express.raw({ type: ["image/png", "image/jpeg"], limit: "5mb" }),
  (req, res) => {
    if (!req.body || !req.body.length) {
      res.status(400).json({ error: "Missing image body" });
      return;
    }
    const contentType = req.headers["content-type"] || "";
    const ext = contentType.includes("png") ? "png" : "jpg";
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    const filePath = path.join(dataDir, `deny-upload.${ext}`);
    fs.writeFileSync(filePath, req.body);
    saveConfig({
      ...config,
      denyImagePath: filePath,
      denyImageUrl: ""
    });
    log("info", "Deny image uploaded", { filePath });
    res.json({ ok: true, path: filePath });
  }
);

app.get("/api/deny-image/preview", async (req, res) => {
  const image = await loadDenyImage();
  res.setHeader("Content-Type", image.contentType);
  res.setHeader("Cache-Control", "no-store");
  res.status(200).send(image.buffer);
});

app.post("/api/deny-image/clear", (req, res) => {
  if (config.denyImagePath && fs.existsSync(config.denyImagePath)) {
    try {
      fs.unlinkSync(config.denyImagePath);
    } catch {
      // ignore
    }
  }
  saveConfig({
    ...config,
    denyImagePath: ""
  });
  res.json({ ok: true });
});

app.post("/api/deny-video/regenerate", async (req, res) => {
  denyVideoState.key = "";
  const ok = await ensureDenyVideo();
  if (!ok) {
    res.status(503).json({ error: "Failed to generate deny video (ffmpeg missing?)" });
    return;
  }
  log("info", "Deny video regenerated");
  res.json({ ok: true });
});

app.post("/api/config", (req, res) => {
  const { m3uUrl, epgUrl, allowedHosts, maxStreams, denyImageUrl } = req.body || {};
  const nextMaxStreams = Number(maxStreams);
  saveConfig({
    m3uUrl: m3uUrl || "",
    epgUrl: epgUrl || "",
    allowedHosts: Array.isArray(allowedHosts) ? allowedHosts : config.allowedHosts,
    maxStreams: Number.isFinite(nextMaxStreams) && nextMaxStreams > 0 ? Math.floor(nextMaxStreams) : config.maxStreams,
    denyImageUrl: typeof denyImageUrl === "string" ? denyImageUrl.trim() : config.denyImageUrl,
    denyImagePath:
      typeof denyImageUrl === "string" && denyImageUrl.trim() ? "" : config.denyImagePath
  });
  log("info", "Config updated", { m3uUrl: config.m3uUrl, epgUrl: config.epgUrl });
  res.json({ ok: true, config });
});

app.get("/api/status", (req, res) => {
  pruneExpiredStreams();
  checkFfmpeg().then((ffmpegAvailable) => {
    res.json({
      activeCount: streams.length,
      maxStreams: getMaxStreams(),
      streams,
      ffmpegAvailable,
      denyImagePath: config.denyImagePath || ""
    });
  });
});

app.post("/api/stream/start", (req, res) => {
  const { label = "", clientId = "" } = req.body || {};
  const token = autoAcquireLock(req, res, { label, clientId });
  if (!token) return;
  res.json({ token, expiresInMs: LOCK_TTL_MS });
});

app.post("/api/stream/stop", (req, res) => {
  const { token } = req.body || {};
  if (!token) {
    res.status(400).json({ error: "Token is required" });
    return;
  }
  const index = streams.findIndex((item) => item.token === token);
  if (index === -1) {
    res.json({ ok: true });
    return;
  }
  streams.splice(index, 1);
  log("info", "Stream lock released");
  res.json({ ok: true });
});

app.get("/api/logs", (req, res) => {
  const limit = Math.min(Number(req.query.limit || 200), LOG_LIMIT);
  res.json({ logs: logBuffer.slice(-limit) });
});

app.get("/proxy/m3u", async (req, res) => {
  const url = req.query.u || config.m3uUrl;
  if (!url) {
    res.status(400).json({ error: "Missing m3u URL" });
    return;
  }
  let token = getToken(req);
  if (!token) {
    token = autoAcquireLock(req, res, {
      label: "playlist",
      clientId: getClientIdentity(req)
    });
    if (!token) {
      res.status(429).json({ error: "Maximum concurrent streams reached" });
      return;
    }
  }
  const proxyBase = `${req.protocol}://${req.get("host")}`;
  log("info", "Proxy m3u", { url });
  await proxyRequest(req, res, url, { rewrite: true, token, proxyBase });
});

app.get("/deny/segment", async (req, res) => {
  const ok = await ensureDenyVideo();
  if (!ok) {
    res.status(503).json({ error: "Deny video not available (ffmpeg missing?)" });
    return;
  }
  res.setHeader("Content-Type", "video/mp2t");
  res.setHeader("Cache-Control", "no-store");
  const stream = fs.createReadStream(denyVideoPath);
  stream.on("error", () => res.status(500).end());
  stream.pipe(res);
});

app.get("/proxy/epg", async (req, res) => {
  const url = req.query.u || config.epgUrl;
  if (!url) {
    res.status(400).json({ error: "Missing epg URL" });
    return;
  }
  log("info", "Proxy epg", { url });
  await proxyRequest(req, res, url, { rewrite: false });
});

app.get("/proxy/hls", async (req, res) => {
  const url = req.query.u;
  if (!url) {
    res.status(400).json({ error: "Missing HLS URL" });
    return;
  }
  if (!ensureLock(req, res, url)) return;
  const proxyBase = `${req.protocol}://${req.get("host")}`;
  log("info", "Proxy hls", { url });
  await proxyRequest(req, res, url, { rewrite: true, token: getToken(req), proxyBase });
});

app.listen(port, () => {
  log("info", `IPTV proxy listening on :${port}`);
});
