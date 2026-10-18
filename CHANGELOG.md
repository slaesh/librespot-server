# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-18

### Features
- Headless Spotify Connect daemon built on `librespot` + `axum`.
- Versioned HTTP REST API (`/api/v1`) with full playback controls (`pause`, `resume`, `next`, `previous`, `volume`, `seek`, `shuffle`, `repeat`, `play-context`).
- Real-time WebSocket stream (`/api/v1/ws`) with `initial_state` snapshot, player state updates, and metadata updates.
- Optional outgoing HTTP webhooks with retry and exponential backoff.
- In-memory metadata caching for track title, artist, album, duration, context names, and CDN cover art.
- Cold-boot session recovery from `playback_state.json` and endless Spotify station radio for autoplay.
- Built-in interactive Scalar API docs (`/docs`), OpenAPI 3.1 (`/api/v1/openapi.json`), and AsyncAPI 2.6 (`/api/v1/asyncapi.json`).
- Non-crashing bit-perfect PCM FIFO sink (`44100:16:2` s16le) for Snapcast.
- Multi-stage Docker image with `cargo-chef` caching, symlink compatibility (`spotify-node`), and healthcheck.
