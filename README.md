# librespot-server

[![CI & Release](https://github.com/slaesh/librespot-server/actions/workflows/release.yml/badge.svg)](https://github.com/slaesh/librespot-server/actions/workflows/release.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg)](https://slaesh.github.io/librespot-server/)
[![Release](https://img.shields.io/github/v/release/slaesh/librespot-server?include_prereleases)](https://github.com/slaesh/librespot-server/releases)
[![Container Image](https://img.shields.io/badge/ghcr.io-slaesh%2Flibrespot--server-blue?logo=docker)](https://github.com/slaesh/librespot-server/pkgs/container/librespot-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A headless, production-hardened **Spotify Connect** daemon written in Rust (built on `librespot` + `axum`). Designed for **Snapcast**, **Home Assistant**, and custom multi-room audio servers.

## Key Features

- **Bit-Perfect FIFO Sink**: Writes raw PCM (`44100:16:2` s16le) to a named pipe (FIFO) with resilient non-crashing reconnect logic if the pipe reader restarts.
- **Versioned HTTP REST API (`/api/v1`)**: Full remote playback control (`pause`, `resume`, `next`, `previous`, `volume`, `seek`, `shuffle`, `repeat`, `play-context`) plus live `GET /api/v1/status` and `GET /api/v1/health` endpoints.
- **Real-Time WebSocket Stream (`/api/v1/ws`)**: Subscribe to live player and metadata events without needing an external webhook server. Immediately emits an `initial_state` snapshot on connect.
- **Optional Outgoing Webhooks (`WEBHOOK_URL`)**: Push player and metadata events via HTTP POST with exponential backoff.
- **Rich Metadata & Context Resolution**: Automatically resolves track title, artist, album, duration, CDN cover art URL, and playlist/album/artist context names via `librespot-metadata` with in-memory deduplicated caching.
- **Cold-Boot Session Recovery & Autoplay**: Persists `playback_state.json` every 10s. Automatically restores active sessions or enters Warm Standby on container restart, and converts single tracks into `spotify:station:track:...` endless radio when Autoplay is active.

## Quick Start

### Docker CLI
Pull and run the pre-built image directly from GitHub Container Registry:

```bash
docker run -d \
  --name spotify-living-room \
  --network host \
  -e NODE_ID="living-room" \
  -e DEVICE_NAME="Living Room" \
  -e PIPE_PATH="/tmp/audio_fifos/living-room" \
  -e API_PORT="25000" \
  -v /tmp/audio_fifos:/tmp/audio_fifos \
  -v librespot_cache:/var/cache/librespot \
  ghcr.io/slaesh/librespot-server:latest
```

### Docker Compose
A ready-to-run [docker-compose.yml](file:///home/slaesh/work/librespot-server/docker-compose.yml) is included in the repository:

```bash
docker compose up -d
```

### Container Image Tags

| Tag | Target | Description |
| :--- | :--- | :--- |
| `latest` | Production | Latest stable release. |
| `0.3` | Production | Floating minor release (auto-receives `0.3.x` patches). |
| `0.3.0` | Production | Exact pinned release version. |
| `experimental` | Development | Bleeding-edge build from the `main` branch. |
| `pr-<number>` | Testing | Ephemeral preview image built from internal pull requests. |

## Configuration (Environment Variables)

| Variable | Alias / Fallback | Default | Description |
| :--- | :--- | :--- | :--- |
| `NODE_ID` | `GROUP_ID` | `"default"` | Unique ID for the instance (used for SHA-1 `device_id` and cache subfolder). |
| `DEVICE_NAME` | — | `"Spotify Node"` | Spotify Connect speaker name shown in the Spotify app. |
| `PIPE_PATH` | `FIFO_PATH` | `"/tmp/snapfifo"` | Path to the named pipe (FIFO) for raw PCM output. |
| `API_PORT` | `HTTP_PORT` | `"25000"` | Port for the HTTP REST API and WebSocket server. |
| `CACHE_PATH` | `CACHE_DIR` | `"/var/cache/librespot"` | Directory for cached Spotify credentials and `playback_state.json`. |
| `WEBHOOK_URL` | — | *Optional* | If set, all events are POSTed as JSON to this URL. |
| `INITIAL_VOLUME` | — | `"50"` | Initial volume (`0–100`) when no persisted state exists. |

## REST & WebSocket API (`/api/v1`)

### Interactive Docs & Machine-Readable Specifications
Live documentation is hosted directly on GitHub Pages without needing to run a container:
- 🌐 **[Live API Reference (GitHub Pages)](https://slaesh.github.io/librespot-server/)**: Interactive **Scalar REST API UI** & **AsyncAPI WebSocket Documentation**.

Every running `librespot-server` instance also serves its own live, code-first API specifications:
- **`GET /docs`**: Interactive **Scalar API Reference UI** in the browser.
- **`GET /api/v1/openapi.json`** (alias `/openapi.json`): **OpenAPI 3.1** specification covering all REST endpoints **plus** all WebSocket/Webhook event schemas (`WebSocketEventPacket`, `InitialStateEvent`, `WebhookPayload`, `MetadataPayload`) in `components.schemas`.
- **`GET /api/v1/asyncapi.json`** (alias `/asyncapi.json`): **AsyncAPI 2.6** specification describing the `/api/v1/ws` WebSocket channel and its message payloads.

#### Automatic Client Codegen (No Manual API Coding Required)
Because both REST and WebSocket event models are exported in `openapi.json` and `asyncapi.json`, you can generate strongly-typed clients in any language:
- **TypeScript / JavaScript**:
  ```bash
  npx openapi-typescript https://raw.githubusercontent.com/slaesh/librespot-server/main/openapi.json -o librespot-server-api.d.ts
  ```
- **C# (.NET / NSwag / Kiota)**:
  ```bash
  nswag openapi2csclient /input:https://raw.githubusercontent.com/slaesh/librespot-server/main/openapi.json /output:LibrespotServerClient.cs /namespace:LibrespotServer.Client /className:LibrespotServerClient
  ```
- **Python**:
  ```bash
  openapi-python-client generate --url https://raw.githubusercontent.com/slaesh/librespot-server/main/openapi.json
  ```

### Read Endpoints
- **`GET /api/v1/health`**: Lightweight health check (`{ "status": "ok", "version": "0.3.0", "node_id": "...", "device_name": "...", "is_connected": true }`).
- **`GET /api/v1/status`**: Full player status including live estimated `position_ms`, active context name/URI, and cached `track` metadata (`title`, `artist`, `album`, `duration_ms`, `cover_url`).
- **`GET /api/v1/ws`**: WebSocket stream emitting `InitialStateEvent` (`Event = "initial_state"`) upon connect followed by real-time `WebhookPayload` and `MetadataPayload` frames.

### Control Endpoints
- **`POST /api/v1/volume`** (or `/api/v1/player/volume`): `{ "volume": 50 }` (`0–100`)
- **`POST /api/v1/pause`** (or `/api/v1/player/pause`)
- **`POST /api/v1/resume`** (or `/api/v1/player/resume`)
- **`POST /api/v1/next`** (or `/api/v1/player/next`)
- **`POST /api/v1/previous`** (or `/api/v1/player/previous`)
- **`POST /api/v1/play-context`** (or `/api/v1/player/play-context`): `{ "context_uri": "spotify:playlist:..." }`
- **`POST /api/v1/seek`** (or `/api/v1/player/seek`): `{ "position_ms": 60000 }`
- **`POST /api/v1/shuffle`** (or `/api/v1/player/shuffle`): `{ "enabled": true }`
- **`POST /api/v1/repeat`** (or `/api/v1/player/repeat`): `{ "context": true, "track": false }`

## License

MIT
