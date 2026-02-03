const state = {
  token: null,
  baseUrl: ""
};

const els = {
  m3uUrl: document.getElementById("m3uUrl"),
  epgUrl: document.getElementById("epgUrl"),
  allowedHosts: document.getElementById("allowedHosts"),
  maxStreams: document.getElementById("maxStreams"),
  denyImageUrl: document.getElementById("denyImageUrl"),
  denyImageFile: document.getElementById("denyImageFile"),
  uploadDenyImage: document.getElementById("uploadDenyImage"),
  clearDenyImage: document.getElementById("clearDenyImage"),
  regenerateDenyVideo: document.getElementById("regenerateDenyVideo"),
  denyImagePreview: document.getElementById("denyImagePreview"),
  ffmpegStatus: document.getElementById("ffmpegStatus"),
  cacheEnabled: document.getElementById("cacheEnabled"),
  cacheMaxMb: document.getElementById("cacheMaxMb"),
  cacheStatus: document.getElementById("cacheStatus"),
  saveConfig: document.getElementById("saveConfig"),
  refreshConfig: document.getElementById("refreshConfig"),
  configStatus: document.getElementById("configStatus"),
  clientLabel: document.getElementById("clientLabel"),
  clientId: document.getElementById("clientId"),
  acquireLock: document.getElementById("acquireLock"),
  releaseLock: document.getElementById("releaseLock"),
  tokenValue: document.getElementById("tokenValue"),
  lockStatus: document.getElementById("lockStatus"),
  m3uProxy: document.getElementById("m3uProxy"),
  epgProxy: document.getElementById("epgProxy"),
  hlsProxy: document.getElementById("hlsProxy"),
  refreshLogs: document.getElementById("refreshLogs"),
  logList: document.getElementById("logList")
};

function api(path, options = {}) {
  return fetch(path, {
    headers: {
      "Content-Type": "application/json"
    },
    ...options
  });
}

function setStatus(message) {
  els.configStatus.textContent = message;
}

function setLockStatus(message) {
  els.lockStatus.textContent = message;
}

function renderEndpoints() {
  const origin = window.location.origin;
  els.m3uProxy.textContent = `${origin}/proxy/m3u`;
  els.epgProxy.textContent = `${origin}/proxy/epg`;
  if (state.token) {
    els.hlsProxy.textContent = `${origin}/proxy/hls?u=...&token=${state.token}`;
  } else {
    els.hlsProxy.textContent = `${origin}/proxy/hls?u=...&token=YOUR_TOKEN`;
  }
}

async function loadConfig() {
  const res = await api("/api/config");
  const data = await res.json();
  els.m3uUrl.value = data.m3uUrl || "";
  els.epgUrl.value = data.epgUrl || "";
  els.allowedHosts.value = Array.isArray(data.allowedHosts) ? data.allowedHosts.join(", ") : "";
  els.maxStreams.value = data.maxStreams || 1;
  els.denyImageUrl.value = data.denyImageUrl || "";
  els.cacheEnabled.checked = Boolean(data.cacheEnabled);
  els.cacheMaxMb.value = Math.max(1, Math.round((data.cacheMaxBytes || 0) / (1024 * 1024)) || 50);
  setStatus("Config loaded.");
  refreshPreview();
}

async function saveConfig() {
  const allowedHosts = els.allowedHosts.value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);

  const payload = {
    m3uUrl: els.m3uUrl.value.trim(),
    epgUrl: els.epgUrl.value.trim(),
    allowedHosts,
    maxStreams: Number(els.maxStreams.value || 1),
    denyImageUrl: els.denyImageUrl.value.trim(),
    cacheEnabled: Boolean(els.cacheEnabled.checked),
    cacheMaxBytes: Number(els.cacheMaxMb.value || 50) * 1024 * 1024
  };

  const res = await api("/api/config", {
    method: "POST",
    body: JSON.stringify(payload)
  });

  if (!res.ok) {
    setStatus("Failed to save config.");
    return;
  }
  setStatus("Config saved.");
  refreshPreview();
}

async function acquireLock() {
  const payload = {
    label: els.clientLabel.value.trim(),
    clientId: els.clientId.value.trim()
  };

  const res = await api("/api/stream/start", {
    method: "POST",
    body: JSON.stringify(payload)
  });

  const data = await res.json();
  if (!res.ok) {
    setLockStatus(data.error || "Failed to acquire lock.");
    return;
  }
  state.token = data.token;
  els.tokenValue.textContent = state.token;
  setLockStatus("Lock acquired.");
  renderEndpoints();
}

async function releaseLock() {
  if (!state.token) {
    setLockStatus("No active lock.");
    return;
  }
  const res = await api("/api/stream/stop", {
    method: "POST",
    body: JSON.stringify({ token: state.token })
  });

  if (!res.ok) {
    const data = await res.json();
    setLockStatus(data.error || "Failed to release lock.");
    return;
  }
  state.token = null;
  els.tokenValue.textContent = "None";
  setLockStatus("Lock released.");
  renderEndpoints();
}

async function refreshLogs() {
  const res = await api("/api/logs");
  const data = await res.json();
  if (!Array.isArray(data.logs) || data.logs.length === 0) {
    els.logList.textContent = "No logs yet.";
    return;
  }
  els.logList.textContent = data.logs
    .map((item) => `[${item.ts}] ${item.level.toUpperCase()} ${item.message}`)
    .join("\n");
}

async function refreshStatus() {
  const res = await api("/api/status");
  const data = await res.json();
  if (!data.activeCount) {
    setLockStatus("No active stream lock.");
  } else {
    setLockStatus(`Active streams: ${data.activeCount}/${data.maxStreams}`);
  }
  if (typeof data.ffmpegAvailable === "boolean") {
    els.ffmpegStatus.textContent = data.ffmpegAvailable ? "ffmpeg: available" : "ffmpeg: missing";
  }
  if (typeof data.cacheBytes === "number" && typeof data.cacheMaxBytes === "number") {
    const mb = (data.cacheBytes / (1024 * 1024)).toFixed(1);
    const maxMb = (data.cacheMaxBytes / (1024 * 1024)).toFixed(0);
    els.cacheStatus.textContent = `cache: ${mb} MB / ${maxMb} MB`;
  }
}

async function uploadDenyImage() {
  const file = els.denyImageFile.files[0];
  if (!file) {
    setStatus("Pick an image first.");
    return;
  }
  const res = await fetch("/api/deny-image", {
    method: "POST",
    headers: {
      "Content-Type": file.type
    },
    body: file
  });
  if (!res.ok) {
    setStatus("Failed to upload image.");
    return;
  }
  setStatus("Image uploaded.");
  refreshPreview();
}

async function clearDenyImage() {
  const res = await api("/api/deny-image/clear", { method: "POST" });
  if (!res.ok) {
    setStatus("Failed to clear upload.");
    return;
  }
  setStatus("Upload cleared.");
  refreshPreview();
}

async function regenerateDenyVideo() {
  const res = await api("/api/deny-video/regenerate", { method: "POST" });
  if (!res.ok) {
    setStatus("Failed to regenerate video (ffmpeg missing?).");
    return;
  }
  setStatus("Deny video regenerated.");
}

function refreshPreview() {
  if (!els.denyImagePreview) return;
  els.denyImagePreview.src = `/api/deny-image/preview?ts=${Date.now()}`;
}

els.saveConfig.addEventListener("click", saveConfig);
els.refreshConfig.addEventListener("click", loadConfig);
els.acquireLock.addEventListener("click", acquireLock);
els.releaseLock.addEventListener("click", releaseLock);
els.refreshLogs.addEventListener("click", refreshLogs);
els.uploadDenyImage.addEventListener("click", uploadDenyImage);
els.clearDenyImage.addEventListener("click", clearDenyImage);
els.regenerateDenyVideo.addEventListener("click", regenerateDenyVideo);

renderEndpoints();
loadConfig();
refreshLogs();
refreshStatus();
