# IPTV Proxy Console

Small local proxy to fix CORS for M3U/HLS + enforce a single active stream.

## Quick start

```bash
npm install
npm run start
```

Open http://localhost:8787 to configure sources and view logs.

## Endpoints

- `GET /proxy/m3u?client=<token>` returns the configured playlist, rewritten to route stream URLs through the proxy
- `GET /proxy/epg?client=<token>` returns the configured EPG
- `GET /proxy/hls?u=<encoded>&client=<token>` streams HLS playlists/segments

## Notes

- HLS playlists are rewritten so relative segment URLs keep working.
- Streams expire after ~20 seconds of inactivity; active playback keeps them alive.
- `maxStreams` controls concurrent streams (default `1`).
- If the stream limit is reached, the proxy returns a tiny HLS playlist that loops `/deny/segment` (a short video generated from the image).
- Set `denyImageUrl` to a custom HTTP(S) image or `data:` URL to customize the over-limit graphic. This requires `ffmpeg` installed.
- You can also upload a PNG/JPEG via `POST /api/deny-image` (content-type `image/png` or `image/jpeg`).
 - Clients are managed via `/api/clients` and must provide a `client` token for access.
- `allowedHosts` is optional. If empty, any upstream host is allowed.
