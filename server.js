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
const clientsPath = path.join(dataDir, "clients.json");

const defaultConfig = {
  m3uUrl: "",
  epgUrl: "",
  allowedHosts: [],
  maxStreams: 1,
  denyImageUrl: "",
  denyImagePath: "",
  cacheEnabled: false,
  cacheMaxBytes: 50 * 1024 * 1024
};

let config = loadConfig();

const logBuffer = [];
const LOG_LIMIT = 400;

const streams = [];
let clients = loadClients();
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
const tokenModes = new Map();
const hlsCache = new Map();
let hlsCacheBytes = 0;

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

function loadClients() {
  try {
    if (!fs.existsSync(dataDir)) {
      fs.mkdirSync(dataDir, { recursive: true });
    }
    if (!fs.existsSync(clientsPath)) {
      fs.writeFileSync(clientsPath, JSON.stringify({ clients: [] }, null, 2));
      return [];
    }
    const raw = fs.readFileSync(clientsPath, "utf8");
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed.clients)) {
      return parsed.clients;
    }
    return [];
  } catch (err) {
    console.error("Failed to load clients:", err);
    return [];
  }
}

function saveClients(nextClients) {
  clients = nextClients;
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }
  fs.writeFileSync(clientsPath, JSON.stringify({ clients }, null, 2));
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
          "drawtext=fontcolor=white:fontsize=48:text='Too many streams':x=60:y=220,drawtext=fontcolor=white:fontsize=24:text='Close another player to resume':x=60:y=290,format=yuv420p",
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

function refreshClientActivity() {
  pruneExpiredStreams();
  const activeByToken = new Map();
  for (const stream of streams) {
    activeByToken.set(stream.token, stream.channelUrl || "");
  }
  let changed = false;
  for (const client of clients) {
    const activeChannel = activeByToken.get(client.token) || "";
    if (client.activeChannel !== activeChannel) {
      client.activeChannel = activeChannel;
      if (!activeChannel) {
        // keep lastSeen as-is
      }
      changed = true;
    }
  }
  if (changed) {
    saveClients(clients);
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

function isCacheEnabled() {
  return Boolean(config.cacheEnabled);
}

function getCacheMaxBytes() {
  const val = Number(config.cacheMaxBytes);
  if (!Number.isFinite(val) || val < 1024 * 1024) {
    return 50 * 1024 * 1024;
  }
  return Math.floor(val);
}

function cacheSizeOf(entry) {
  if (!entry) return 0;
  if (entry.type === "text") {
    return Buffer.byteLength(entry.body || "", "utf8");
  }
  if (entry.type === "buffer") {
    return entry.body ? entry.body.length : 0;
  }
  return 0;
}

function pruneCache() {
  const now = Date.now();
  for (const [key, entry] of hlsCache) {
    if (entry.expiresAt && entry.expiresAt <= now) {
      hlsCacheBytes -= cacheSizeOf(entry);
      hlsCache.delete(key);
    }
  }
  const maxBytes = getCacheMaxBytes();
  if (hlsCacheBytes <= maxBytes) return;
  for (const [key, entry] of hlsCache) {
    if (hlsCacheBytes <= maxBytes) break;
    hlsCacheBytes -= cacheSizeOf(entry);
    hlsCache.delete(key);
  }
}

function getCacheEntry(key) {
  const entry = hlsCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt && entry.expiresAt <= Date.now()) {
    hlsCacheBytes -= cacheSizeOf(entry);
    hlsCache.delete(key);
    return null;
  }
  return entry;
}

function setCacheEntry(key, entry) {
  const existing = hlsCache.get(key);
  if (existing) {
    hlsCacheBytes -= cacheSizeOf(existing);
  }
  hlsCache.set(key, entry);
  hlsCacheBytes += cacheSizeOf(entry);
  pruneCache();
}

function getClientId(req) {
  const headerId = req.get("x-client-id");
  const queryId = req.query.clientId;
  return headerId || queryId || "";
}

function getClientToken(req) {
  const headerToken = req.get("x-client-token");
  const queryToken = req.query.client;
  const legacyToken = req.query.token;
  return headerToken || queryToken || legacyToken || "";
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

function ensureLock(req, res) {
  const token = getClientToken(req);
  if (!token) {
    res.status(401).json({ error: "Missing client token" });
    return null;
  }
  const client = clients.find((item) => item.token === token);
  if (!client) {
    res.status(403).json({ error: "Unknown client token" });
    return null;
  }
  pruneExpiredStreams();
  let stream = getStreamByToken(token);
  if (!stream) {
    if (streams.length >= getMaxStreams()) {
      res.status(429).json({
        error: "Maximum concurrent streams reached",
        activeCount: streams.length
      });
      return null;
    }
    stream = {
      token,
      lastSeen: Date.now(),
      label: client.name || "client",
      clientId: client.id,
      channelKey: "",
      channelUrl: "",
      lastSwitchAt: 0,
      deniedChannels: new Map()
    };
    streams.push(stream);
    log("info", "Stream activated", { clientId: stream.clientId });
  }
  return { stream, client };
}

function autoAcquireLock() {
  return null;
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
  if (!targetUrl) return true;
  const channelKey = getChannelKey(targetUrl);
  if (!channelKey) return true;
  const now = Date.now();
  const SWITCH_COOLDOWN_MS = 500;
  const REJECT_OLD_CHANNEL_MS = 3000;
  for (const [key, until] of stream.deniedChannels.entries()) {
    if (until <= now) {
      stream.deniedChannels.delete(key);
    }
  }
  const deniedUntil = stream.deniedChannels.get(channelKey);
  if (deniedUntil && deniedUntil > now) {
    return false;
  }
  if (isPlaylistUrl(targetUrl)) {
    if (!stream.channelKey) {
      stream.channelKey = channelKey;
      stream.channelUrl = targetUrl;
      stream.lastSwitchAt = now;
      return true;
    }
    if (stream.channelKey !== channelKey) {
      if (now - stream.lastSwitchAt < SWITCH_COOLDOWN_MS) {
        return false;
      }
      stream.deniedChannels.set(stream.channelKey, now + REJECT_OLD_CHANNEL_MS);
      stream.channelKey = channelKey;
      stream.channelUrl = targetUrl;
      stream.lastSwitchAt = now;
      tokenModes.set(stream.token, "switch");
      log("info", "Channel switched", { clientId: stream.clientId });
      return true;
    }
    return true;
  }
  if (!stream.channelKey) {
    return false;
  }
  if (!channelKey.startsWith(stream.channelKey)) {
    return false;
  }
  return true;
}

function sendDenyPlaylist(req, res, token = "") {
  const base = `${req.protocol}://${req.get("host")}`;
  const seq = Math.floor(Date.now() / 1000);
  const playlist = [
    "#EXTM3U",
    "#EXT-X-VERSION:3",
    "#EXT-X-TARGETDURATION:4",
    `#EXT-X-MEDIA-SEQUENCE:${seq}`,
    "#EXT-X-ALLOW-CACHE:NO",
    "#EXTINF:4.0,",
    `${base}/deny/segment`
  ].join("\n");
  res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
  res.setHeader("Cache-Control", "no-store");
  res.status(200).send(playlist);
  if (token) {
    tokenModes.set(token, "deny");
  }
}

function rewritePlaylist(content, baseUrl, token, proxyBase, { forceDiscontinuity = false } = {}) {
  const lines = content.split(/\r?\n/);
  const rewritten = lines.map((line) => {
    const trimmed = line.trim();
    if (!trimmed) return line;
    if (trimmed.startsWith("#")) {
      return rewriteTagLine(line, baseUrl, token, proxyBase);
    }
    try {
      const resolved = new URL(trimmed, baseUrl).toString();
      return buildProxyUrl(resolved, token, proxyBase);
    } catch {
      return line;
    }
  });
  if (forceDiscontinuity) {
    if (rewritten[0] && rewritten[0].trim() === "#EXTM3U") {
      rewritten.splice(1, 0, "#EXT-X-DISCONTINUITY");
    } else {
      rewritten.unshift("#EXT-X-DISCONTINUITY");
    }
  }
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
  const tokenPart = token ? `&client=${encodeURIComponent(token)}` : "";
  const path = `/proxy/hls?u=${encoded}${tokenPart}`;
  if (!proxyBase) return path;
  return `${proxyBase}${path}`;
}

async function proxyRequest(
  req,
  res,
  targetUrl,
  { rewrite = false, token = "", proxyBase = "", forceDiscontinuity = false } = {}
) {
  if (!isHostAllowed(targetUrl)) {
    res.status(403).json({ error: "Target host is not allowed" });
    return;
  }

  const cacheable = isCacheEnabled();
  const looksLikePlaylist = isPlaylistUrl(targetUrl);
  const cacheKey = targetUrl;
  if (cacheable) {
    const entry = getCacheEntry(cacheKey);
    if (entry) {
      if (entry.type === "text" && looksLikePlaylist && rewrite) {
        const rewritten = rewritePlaylist(entry.body, entry.baseUrl, token, proxyBase, {
          forceDiscontinuity
        });
        res.setHeader("Content-Type", entry.contentType || "application/vnd.apple.mpegurl");
        res.status(200).send(rewritten);
        return;
      } else if (entry.type === "buffer" && !looksLikePlaylist) {
        if (entry.contentType) {
          res.setHeader("Content-Type", entry.contentType);
        }
        res.status(200).send(entry.body);
        return;
      }
    }
  }

  let upstream;
  let lastError = null;
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": req.get("accept") || "*/*",
    "Connection": "keep-alive"
  };
  const retryStatuses = new Set([502, 503, 504]);
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      upstream = await fetch(targetUrl, { headers });
      if (upstream.ok || !retryStatuses.has(upstream.status) || attempt === 2) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 150 * (attempt + 1)));
    } catch (err) {
      lastError = err;
      if (attempt === 2) {
        log("error", "Fetch failed", { targetUrl, error: String(err) });
        res.status(502).json({ error: "Upstream fetch failed" });
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 150 * (attempt + 1)));
    }
  }
  if (!upstream && lastError) {
    res.status(502).json({ error: "Upstream fetch failed" });
    return;
  }

  if (!upstream.ok) {
    const status = upstream.status;
    const statusText = upstream.statusText;
    let bodyPreview = "";
    try {
      const text = await upstream.text();
      bodyPreview = text.slice(0, 200);
    } catch {
      bodyPreview = "";
    }
    log("error", "Upstream error", { targetUrl, status, statusText, bodyPreview });
    res.status(status).json({ error: "Upstream error", status, statusText });
    return;
  }

  const contentType = upstream.headers.get("content-type") || "";
  const isPlaylist = contentType.includes("mpegurl") || targetUrl.toLowerCase().includes(".m3u8");

  if (rewrite && isPlaylist) {
    const text = await upstream.text();
    if (cacheable) {
      setCacheEntry(cacheKey, {
        type: "text",
        body: text,
        contentType,
        baseUrl: upstream.url || targetUrl,
        expiresAt: Date.now() + 4_000
      });
    }
    const rewritten = rewritePlaylist(text, upstream.url || targetUrl, token, proxyBase, {
      forceDiscontinuity
    });
    res.setHeader("Content-Type", "application/vnd.apple.mpegurl");
    res.status(200).send(rewritten);
    return;
  }

  res.status(200);
  if (contentType) {
    res.setHeader("Content-Type", contentType);
  }
  try {
    if (cacheable && !isPlaylist) {
      const buffer = Buffer.from(await upstream.arrayBuffer());
      setCacheEntry(cacheKey, {
        type: "buffer",
        body: buffer,
        contentType,
        expiresAt: Date.now() + 120_000
      });
      res.send(buffer);
    } else {
      await pipe(upstream.body, res);
    }
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

app.get("/api/clients", (req, res) => {
  refreshClientActivity();
  res.json({ clients });
});

app.post("/api/clients", (req, res) => {
  const { name } = req.body || {};
  const client = {
    id: crypto.randomUUID(),
    name: typeof name === "string" && name.trim() ? name.trim() : "New client",
    token: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    lastSeen: null,
    activeChannel: ""
  };
  clients = [...clients, client];
  saveClients(clients);
  res.json({ ok: true, client });
});

app.post("/api/clients/:token/rename", (req, res) => {
  const token = req.params.token;
  const { name } = req.body || {};
  const client = clients.find((item) => item.token === token);
  if (!client) {
    res.status(404).json({ error: "Client not found" });
    return;
  }
  client.name = typeof name === "string" && name.trim() ? name.trim() : client.name;
  saveClients(clients);
  res.json({ ok: true, client });
});

app.delete("/api/clients/:token", (req, res) => {
  const token = req.params.token;
  const nextClients = clients.filter((item) => item.token !== token);
  if (nextClients.length === clients.length) {
    res.status(404).json({ error: "Client not found" });
    return;
  }
  clients = nextClients;
  saveClients(clients);
  res.json({ ok: true });
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
  const { m3uUrl, epgUrl, allowedHosts, maxStreams, denyImageUrl, cacheEnabled, cacheMaxBytes } =
    req.body || {};
  const nextMaxStreams = Number(maxStreams);
  const nextCacheMaxBytes = Number(cacheMaxBytes);
  saveConfig({
    m3uUrl: m3uUrl || "",
    epgUrl: epgUrl || "",
    allowedHosts: Array.isArray(allowedHosts) ? allowedHosts : config.allowedHosts,
    maxStreams: Number.isFinite(nextMaxStreams) && nextMaxStreams > 0 ? Math.floor(nextMaxStreams) : config.maxStreams,
    denyImageUrl: typeof denyImageUrl === "string" ? denyImageUrl.trim() : config.denyImageUrl,
    denyImagePath:
      typeof denyImageUrl === "string" && denyImageUrl.trim() ? "" : config.denyImagePath,
    cacheEnabled: typeof cacheEnabled === "boolean" ? cacheEnabled : config.cacheEnabled,
    cacheMaxBytes: Number.isFinite(nextCacheMaxBytes) && nextCacheMaxBytes > 0
      ? Math.floor(nextCacheMaxBytes)
      : config.cacheMaxBytes
  });
  log("info", "Config updated", { m3uUrl: config.m3uUrl, epgUrl: config.epgUrl });
  res.json({ ok: true, config });
});

app.get("/api/status", (req, res) => {
  refreshClientActivity();
  checkFfmpeg().then((ffmpegAvailable) => {
    res.json({
      activeCount: streams.length,
      maxStreams: getMaxStreams(),
      streams,
      ffmpegAvailable,
      denyImagePath: config.denyImagePath || "",
      cacheEnabled: isCacheEnabled(),
      cacheBytes: hlsCacheBytes,
      cacheMaxBytes: getCacheMaxBytes()
    });
  });
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
  log("info", "Stream ended");
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
  const token = getClientToken(req);
  if (!token) {
    res.status(401).json({ error: "Missing client token" });
    return;
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
  const token = getClientToken(req);
  if (!token) {
    res.status(401).json({ error: "Missing client token" });
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
  const ensured = ensureLock(req, res);
  if (!ensured) return;
  const { stream, client } = ensured;
  if (!applyChannelLock(req, res, url, stream)) {
    if (isPlaylistUrl(url)) {
      sendDenyPlaylist(req, res, stream.token);
      return;
    }
    const ok = await ensureDenyVideo();
    if (!ok) {
      res.status(503).json({ error: "Deny video not available (ffmpeg missing?)" });
      return;
    }
    res.setHeader("Content-Type", "video/mp2t");
    res.setHeader("Cache-Control", "no-store");
    const denyStream = fs.createReadStream(denyVideoPath);
    denyStream.on("error", () => res.status(500).end());
    denyStream.pipe(res);
    return;
  }
  touchStream(stream);
  client.lastSeen = stream.lastSeen;
  if (stream.channelUrl) {
    client.activeChannel = stream.channelUrl;
  }
  saveClients(clients);
  const proxyBase = `${req.protocol}://${req.get("host")}`;
  const token = getClientToken(req);
  const mode = tokenModes.get(token);
  const forceDiscontinuity = mode === "deny" || mode === "switch";
  if (forceDiscontinuity) {
    tokenModes.delete(token);
  }
  log("info", "Proxy hls", { url });
  await proxyRequest(req, res, url, {
    rewrite: true,
    token,
    proxyBase,
    forceDiscontinuity
  });
});

app.listen(port, () => {
  log("info", `IPTV proxy listening on :${port}`);
});
