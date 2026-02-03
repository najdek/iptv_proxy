import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { pipeline } from "stream";
import { promisify } from "util";
import crypto from "crypto";

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
  maxStreams: 1
};

let config = loadConfig();

const logBuffer = [];
const LOG_LIMIT = 400;

const streams = [];
const LOCK_TTL_MS = 20_000;

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
  if (!applyChannelLock(res, targetUrl, stream)) return null;
  touchStream(stream);
  return token;
}

function autoAcquireLock(req, res, { label = "auto", clientId = "" } = {}) {
  pruneExpiredStreams();
  if (streams.length >= getMaxStreams()) {
    res.status(429).json({
      error: "Maximum concurrent streams reached",
      activeCount: streams.length
    });
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

function applyChannelLock(res, targetUrl, stream) {
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
    res.status(429).json({
      error: "Another channel is active",
      active: {
        channelKey: stream.channelKey
      }
    });
    return false;
  }
  return true;
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

app.post("/api/config", (req, res) => {
  const { m3uUrl, epgUrl, allowedHosts, maxStreams } = req.body || {};
  const nextMaxStreams = Number(maxStreams);
  saveConfig({
    m3uUrl: m3uUrl || "",
    epgUrl: epgUrl || "",
    allowedHosts: Array.isArray(allowedHosts) ? allowedHosts : config.allowedHosts,
    maxStreams: Number.isFinite(nextMaxStreams) && nextMaxStreams > 0 ? Math.floor(nextMaxStreams) : config.maxStreams
  });
  log("info", "Config updated", { m3uUrl: config.m3uUrl, epgUrl: config.epgUrl });
  res.json({ ok: true, config });
});

app.get("/api/status", (req, res) => {
  pruneExpiredStreams();
  res.json({
    activeCount: streams.length,
    maxStreams: getMaxStreams(),
    streams
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
    if (!token) return;
  }
  const proxyBase = `${req.protocol}://${req.get("host")}`;
  log("info", "Proxy m3u", { url });
  await proxyRequest(req, res, url, { rewrite: true, token, proxyBase });
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
