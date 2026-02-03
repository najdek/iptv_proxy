# IPTV Proxy Console

Small local proxy to fix CORS for M3U/HLS + enforce a single active stream.

## Quick start

```bash
npm install
npm run start
```

Open http://localhost:8787 to configure sources and view logs.

## Endpoints

- `GET /proxy/m3u` returns the configured playlist, rewritten to route stream URLs through the proxy (auto-acquires a lock if none is provided)
- `GET /proxy/epg` returns the configured EPG
- `GET /proxy/hls?u=<encoded>&token=<token>` streams HLS playlists/segments (requires a lock token)
- `POST /api/stream/start` acquires a lock token
- `POST /api/stream/stop` releases a lock token

## Notes

- HLS playlists are rewritten so relative segment URLs keep working.
- The stream lock expires after ~20 seconds of inactivity; active playback keeps it alive.
- `allowedHosts` is optional. If empty, any upstream host is allowed.
