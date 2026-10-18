# Contributing to librespot-server

Thank you for your interest in contributing to `librespot-server`!

## Development Setup

`librespot-server` is written in Rust (2021 edition) and uses Tokio and Axum.

### Prerequisites
- [Rust toolchain](https://rustup.rs/) (Rust 1.75+)
- Linux audio & mDNS development libraries:
  ```bash
  sudo apt-get install -y pkg-config libssl-dev libavahi-compat-libdnssd-dev libasound2-dev
  ```

### Running Tests & Linter
Before submitting changes, ensure all tests and linter checks pass:
```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --release
```

### Updating API Specifications (`openapi.json` & `asyncapi.json`)
The API specifications in the repository root are generated directly from the Rust structs (`utoipa`).
Whenever you add or modify API routes, payloads, or schemas, re-generate the spec files using:
```bash
cargo run -- --dump-specs
```
A unit test (`test_committed_specs_are_in_sync`) in CI enforces that `openapi.json` and `asyncapi.json` remain 100% in sync with the codebase.

---

## Conventional Commits & Pull Requests

This repository follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for automated release management (`release-please`):

- `feat: ...` — Introduces a new feature (triggers a **Minor** version bump, e.g. `0.2.0` -> `0.3.0`).
- `fix: ...` — Fixes a bug (triggers a **Patch** version bump, e.g. `0.2.0` -> `0.2.1`).
- `feat!: ...` or `fix!: ...` — Introduces a breaking change (triggers a **Major** version bump).
- `docs: ...` — Documentation changes only.
- `chore: ...` — Build process, dependencies, or tooling changes.
- `ci: ...` — CI/CD workflow updates.

A GitHub Action validates pull request titles against Conventional Commits formatting.

---

## Release Process

Releases are fully automated via **Release Please**:
1. When conventional commits land on `main`, Release Please automatically opens/updates a release PR (`chore(main): release X.Y.Z`) updating `Cargo.toml`, `openapi.json`, `asyncapi.json`, and `CHANGELOG.md`.
2. When the maintainer merges this release PR, Release Please tags the release `vX.Y.Z` and creates the GitHub Release notes.
3. The release workflow builds and pushes multi-architecture container images (`linux/amd64` and `linux/arm64`) to GitHub Container Registry (`ghcr.io/slaesh/librespot-server`).
