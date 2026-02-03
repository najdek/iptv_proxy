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
  refreshClients: document.getElementById("refreshClients"),
  newClientName: document.getElementById("newClientName"),
  createClient: document.getElementById("createClient"),
  clientList: document.getElementById("clientList"),
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
  if (typeof data.ffmpegAvailable === "boolean") {
    els.ffmpegStatus.textContent = data.ffmpegAvailable ? "ffmpeg: available" : "ffmpeg: missing";
  }
  if (typeof data.cacheBytes === "number" && typeof data.cacheMaxBytes === "number") {
    const mb = (data.cacheBytes / (1024 * 1024)).toFixed(1);
    const maxMb = (data.cacheMaxBytes / (1024 * 1024)).toFixed(0);
    els.cacheStatus.textContent = `cache: ${mb} MB / ${maxMb} MB`;
  }
}

async function loadClients() {
  const res = await api("/api/clients");
  const data = await res.json();
  const list = Array.isArray(data.clients) ? data.clients : [];
  if (list.length === 0) {
    els.clientList.textContent = "No clients yet.";
    return;
  }
  els.clientList.innerHTML = "";
  list.forEach((client) => {
    const card = document.createElement("div");
    card.className = "client-card";

    const titleRow = document.createElement("div");
    titleRow.className = "row";
    const title = document.createElement("div");
    title.textContent = client.name || "Client";
    const meta = document.createElement("div");
    meta.className = "meta";
    meta.textContent = client.lastSeen ? `Last seen: ${client.lastSeen}` : "Never connected";
    titleRow.append(title, meta);

    const token = document.createElement("div");
    token.className = "client-token";
    token.textContent = client.token;

    const links = document.createElement("div");
    links.className = "link-row";
    const origin = window.location.origin;
    const m3u = `${origin}/proxy/m3u?client=${client.token}`;
    const epg = `${origin}/proxy/epg?client=${client.token}`;
    const m3uEl = document.createElement("code");
    m3uEl.textContent = m3u;
    const epgEl = document.createElement("code");
    epgEl.textContent = epg;
    const copyM3u = document.createElement("button");
    copyM3u.className = "copy-btn";
    copyM3u.textContent = "Copy M3U";
    copyM3u.addEventListener("click", () => navigator.clipboard.writeText(m3u));
    const copyEpg = document.createElement("button");
    copyEpg.className = "copy-btn";
    copyEpg.textContent = "Copy EPG";
    copyEpg.addEventListener("click", () => navigator.clipboard.writeText(epg));
    links.append(m3uEl, copyM3u, epgEl, copyEpg);

    const active = document.createElement("div");
    active.className = "meta";
    active.textContent = client.activeChannel ? `Active: ${client.activeChannel}` : "Active: idle";

    const actions = document.createElement("div");
    actions.className = "client-actions";
    const rename = document.createElement("button");
    rename.className = "ghost";
    rename.textContent = "Rename";
    rename.addEventListener("click", async () => {
      const next = prompt("Client name", client.name || "");
      if (!next) return;
      await api(`/api/clients/${client.token}/rename`, {
        method: "POST",
        body: JSON.stringify({ name: next })
      });
      loadClients();
    });
    const remove = document.createElement("button");
    remove.className = "ghost";
    remove.textContent = "Remove";
    remove.addEventListener("click", async () => {
      await fetch(`/api/clients/${client.token}`, { method: "DELETE" });
      loadClients();
    });
    actions.append(rename, remove);

    card.append(titleRow, token, links, active, actions);
    els.clientList.append(card);
  });
}

async function createClient() {
  const name = els.newClientName.value.trim();
  const res = await api("/api/clients", {
    method: "POST",
    body: JSON.stringify({ name })
  });
  if (!res.ok) {
    setStatus("Failed to create client.");
    return;
  }
  els.newClientName.value = "";
  loadClients();
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
els.refreshLogs.addEventListener("click", refreshLogs);
els.uploadDenyImage.addEventListener("click", uploadDenyImage);
els.clearDenyImage.addEventListener("click", clearDenyImage);
els.regenerateDenyVideo.addEventListener("click", regenerateDenyVideo);
els.refreshClients.addEventListener("click", loadClients);
els.createClient.addEventListener("click", createClient);

loadConfig();
refreshLogs();
refreshStatus();
loadClients();
