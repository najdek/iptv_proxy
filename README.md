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
- `maxStreams` controls concurrent streams (default `1`).
- If the stream limit is reached, the proxy returns a tiny HLS playlist that loops `/deny/segment` (a short video generated from the image).
- Set `denyImageUrl` to a custom HTTP(S) image or `data:` URL to customize the over-limit graphic. This requires `ffmpeg` installed.
- You can also upload a PNG/JPEG via `POST /api/deny-image` (content-type `image/png` or `image/jpeg`).
- `allowedHosts` is optional. If empty, any upstream host is allowed.
