use crate::events::{MetadataPayload, WebhookPayload};
use crate::metadata_cache::MetadataCache;
use crate::shared_mixer::SharedMixer;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Json, State,
    },
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use librespot_connect::Spirc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};
use utoipa::{OpenApi, ToSchema};

/// Request payload to set the playback volume percentage (`0..=100`).
#[derive(Deserialize, Serialize, ToSchema)]
pub struct VolumeRequest {
    /// Target volume percentage (`0` to `100`).
    #[schema(example = 50, minimum = 0, maximum = 100)]
    pub volume: u32,
}

/// Request payload to load and play a Spotify context URI (playlist, album, artist, or station).
#[derive(Deserialize, Serialize, ToSchema)]
pub struct PlayContextRequest {
    /// Spotify context URI (`spotify:playlist:...`, `spotify:album:...`, `spotify:artist:...`).
    #[schema(example = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M")]
    pub context_uri: String,
}

/// Request payload to seek to a specific track position in milliseconds.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct SeekRequest {
    /// Target playback position in milliseconds.
    #[schema(example = 60000)]
    pub position_ms: u32,
}

/// Request payload to enable or disable shuffle mode.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct ShuffleRequest {
    /// `true` to enable shuffle, `false` to disable.
    #[schema(example = true)]
    pub enabled: bool,
}

/// Request payload to configure context and single-track repeat modes.
#[derive(Deserialize, Serialize, ToSchema)]
pub struct RepeatRequest {
    /// Repeat the entire context (playlist/album).
    #[serde(default)]
    #[schema(example = true)]
    pub context: bool,
    /// Repeat the current track continuously.
    #[serde(default)]
    #[schema(example = false)]
    pub track: bool,
}

/// Health check response returned by `GET /api/v1/health`.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct HealthResponse {
    /// Service health status (`"ok"`).
    #[schema(example = "ok")]
    pub status: String,
    /// Running `spotify-node` version.
    #[schema(example = "0.3.0")]
    pub version: String,
    /// Configured node / room group identifier.
    #[schema(example = "living-room")]
    pub node_id: String,
    /// Display name advertised via Spotify Connect / mDNS.
    #[schema(example = "Living Room")]
    pub device_name: String,
    /// Whether an authenticated Spotify session is currently connected.
    #[schema(example = true)]
    pub is_connected: bool,
}

/// Resolved metadata for a Spotify track.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct TrackMetadataDto {
    /// Spotify track URI (`spotify:track:...`).
    #[schema(example = "spotify:track:4uLU6hMCjMI75M1A2tKUQC")]
    pub track_id: String,
    /// Track title.
    #[schema(example = "Never Gonna Give You Up")]
    pub title: String,
    /// Comma-separated artist names.
    #[schema(example = "Rick Astley")]
    pub artist: String,
    /// Album title.
    #[schema(example = "Whenever You Need Somebody")]
    pub album: String,
    /// Track duration in milliseconds.
    #[schema(example = 213573)]
    pub duration_ms: u32,
    /// HTTPS URL to the album cover artwork on Spotify's CDN.
    #[schema(example = "https://i.scdn.co/image/ab67616d0000b2735755e164993798e0c9ef7d7a")]
    pub cover_url: Option<String>,
}

/// Comprehensive live status snapshot returned by `GET /api/v1/status` and inside the WebSocket `initial_state` packet.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct NodeStatusResponse {
    /// Configured node / room group identifier.
    #[schema(example = "living-room")]
    pub node_id: String,
    /// Display name advertised via Spotify Connect / mDNS.
    #[schema(example = "Living Room")]
    pub device_name: String,
    /// Running `spotify-node` version.
    #[schema(example = "0.3.0")]
    pub version: String,
    /// Whether an authenticated Spotify session is currently connected.
    #[schema(example = true)]
    pub is_connected: bool,
    /// Username of the currently connected Spotify account.
    #[schema(example = "alice")]
    pub connected_user: Option<String>,
    /// Whether audio is actively playing right now.
    #[schema(example = true)]
    pub is_playing: bool,
    /// Whether this node is the active Spotify Connect playback target.
    #[schema(example = true)]
    pub is_active_device: bool,
    /// Current volume percentage (`0..=100`).
    #[schema(example = 50)]
    pub volume: u32,
    /// Live estimated playback position in milliseconds.
    #[schema(example = 45210)]
    pub position_ms: u32,
    /// Whether shuffle mode is enabled.
    #[schema(example = false)]
    pub shuffle: bool,
    /// Whether context repeat is enabled.
    #[schema(example = false)]
    pub repeat_context: bool,
    /// Whether single-track repeat is enabled.
    #[schema(example = false)]
    pub repeat_track: bool,
    /// Whether endless autoplay radio is enabled.
    #[schema(example = true)]
    pub autoplay: bool,
    /// Active context URI (`spotify:playlist:...`, `spotify:album:...`, etc.).
    #[schema(example = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M")]
    pub context_uri: Option<String>,
    /// Human-readable name of the active context.
    #[schema(example = "Today's Top Hits")]
    pub context_name: Option<String>,
    /// Currently loaded track URI (`spotify:track:...`).
    #[schema(example = "spotify:track:4uLU6hMCjMI75M1A2tKUQC")]
    pub current_track_uri: Option<String>,
    /// Cached rich metadata for the current track (if resolved).
    pub track: Option<TrackMetadataDto>,
}

/// Initial state packet sent immediately as the first WebSocket message upon connecting to `GET /api/v1/ws`.
#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
pub struct InitialStateEvent {
    /// Unique identifier of the Spotify node / room group.
    #[serde(rename = "GroupId")]
    #[schema(example = "living-room")]
    pub group_id: String,
    /// Always `"initial_state"`.
    #[serde(rename = "Event")]
    #[schema(example = "initial_state")]
    pub event: String,
    /// Complete node status snapshot at the moment of WebSocket connection.
    #[serde(rename = "Status")]
    pub status: NodeStatusResponse,
}

/// Union (`oneOf`) of all real-time JSON packets emitted over the WebSocket (`GET /api/v1/ws`) and optional HTTP Webhook (`WEBHOOK_URL`).
///
/// Code generators (NSwag, Kiota, openapi-typescript, etc.) generate strongly-typed models for all three variants directly from `openapi.json`.
#[derive(Serialize, Clone, Debug, ToSchema)]
#[serde(untagged)]
#[allow(dead_code)]
pub enum WebSocketEventPacket {
    /// Emitted immediately upon WebSocket connection (`Event = "initial_state"`).
    InitialState(InitialStateEvent),
    /// Emitted on player state changes (`Event = "playing" | "paused" | "stopped" | "volume_set" | "heartbeat" | "set_queue" | ...`).
    PlayerEvent(WebhookPayload),
    /// Emitted when rich track metadata is resolved (`Event = "metadata_update"`).
    MetadataUpdate(MetadataPayload),
}

struct VersionModifier;

impl utoipa::Modify for VersionModifier {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        openapi.info.version = env!("CARGO_PKG_VERSION").to_string();
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Librespot Server API (librespot-server)",
        version = "0.3.0",
        description = "Headless Spotify Connect daemon with versioned REST control API (`/api/v1`), real-time WebSocket event stream (`/api/v1/ws`), AsyncAPI specification (`/api/v1/asyncapi.json`), and optional outgoing HTTP webhooks.",
        license(name = "MIT")
    ),
    modifiers(&VersionModifier),
    paths(
        get_health,
        get_status,
        ws_handler,
        set_volume,
        pause,
        resume,
        next,
        previous,
        play_context,
        seek,
        set_shuffle,
        set_repeat
    ),
    components(
        schemas(
            HealthResponse,
            NodeStatusResponse,
            TrackMetadataDto,
            VolumeRequest,
            PlayContextRequest,
            SeekRequest,
            ShuffleRequest,
            RepeatRequest,
            InitialStateEvent,
            WebhookPayload,
            MetadataPayload,
            WebSocketEventPacket
        )
    ),
    tags(
        (name = "Status & Monitoring", description = "Health, live playback status, and real-time WebSocket event stream"),
        (name = "Player Control", description = "Remote playback control commands via Spotify Connect (Spirc)")
    )
)]
pub struct ApiDoc;

/// Builds the AsyncAPI 2.6.0 specification JSON describing the `/api/v1/ws` WebSocket channel
/// and reusing the exact component schemas generated by `utoipa`.
pub fn build_asyncapi_spec() -> serde_json::Value {
    let openapi = ApiDoc::openapi();
    let openapi_val = serde_json::to_value(&openapi).unwrap_or_default();
    let schemas = openapi_val
        .get("components")
        .and_then(|c| c.get("schemas"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    serde_json::json!({
        "asyncapi": "2.6.0",
        "info": {
            "title": "Spotify Node WebSocket & Webhook Events API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Real-time event stream emitted by `spotify-node` over WebSocket (`GET /api/v1/ws`) and optional HTTP POST Webhook (`WEBHOOK_URL`).",
            "license": { "name": "MIT" }
        },
        "defaultContentType": "application/json",
        "channels": {
            "/api/v1/ws": {
                "description": "Real-time WebSocket stream. Immediately sends `InitialStateEvent` upon connection, followed by `WebhookPayload` and `MetadataPayload` messages.",
                "subscribe": {
                    "operationId": "subscribeNodeEvents",
                    "summary": "Receive real-time player and metadata events from the Spotify node",
                    "message": {
                        "oneOf": [
                            { "$ref": "#/components/messages/InitialStateMessage" },
                            { "$ref": "#/components/messages/PlayerEventMessage" },
                            { "$ref": "#/components/messages/MetadataUpdateMessage" }
                        ]
                    }
                }
            }
        },
        "components": {
            "messages": {
                "InitialStateMessage": {
                    "name": "InitialStateEvent",
                    "title": "Initial State Snapshot",
                    "summary": "Sent immediately as the first frame after WebSocket upgrade (`Event = \"initial_state\"`).",
                    "payload": { "$ref": "#/components/schemas/InitialStateEvent" }
                },
                "PlayerEventMessage": {
                    "name": "WebhookPayload",
                    "title": "Player State Event",
                    "summary": "Sent on playback state changes, volume changes, queue changes, and 10s heartbeats.",
                    "payload": { "$ref": "#/components/schemas/WebhookPayload" }
                },
                "MetadataUpdateMessage": {
                    "name": "MetadataPayload",
                    "title": "Track Metadata Update",
                    "summary": "Sent once per track when title, artist, album, duration, and cover URL are resolved (`Event = \"metadata_update\"`).",
                    "payload": { "$ref": "#/components/schemas/MetadataPayload" }
                }
            },
            "schemas": schemas
        }
    })
}

pub struct AppState {
    pub node_id: String,
    pub device_name: String,
    pub spirc: Arc<RwLock<Option<Arc<Spirc>>>>,
    pub shared_state: crate::state::SharedPlaybackState,
    pub metadata_cache: Arc<MetadataCache>,
    pub ws_broadcast: broadcast::Sender<String>,
    pub state_file_path: Option<std::path::PathBuf>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let v1_routes = Router::new()
        .route("/docs", get(get_scalar_docs))
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .route("/ws", get(ws_handler))
        .route("/openapi.json", get(get_openapi_json))
        .route("/asyncapi.json", get(get_asyncapi_json))
        // Flat v1 control routes (direct migration path from legacy routes)
        .route("/volume", post(set_volume))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/next", post(next))
        .route("/previous", post(previous))
        .route("/play-context", post(play_context))
        .route("/seek", post(seek))
        .route("/shuffle", post(set_shuffle))
        .route("/repeat", post(set_repeat))
        // Grouped player sub-routes for clean REST semantics
        .route("/player/volume", post(set_volume))
        .route("/player/pause", post(pause))
        .route("/player/resume", post(resume))
        .route("/player/next", post(next))
        .route("/player/previous", post(previous))
        .route("/player/play-context", post(play_context))
        .route("/player/seek", post(seek))
        .route("/player/shuffle", post(set_shuffle))
        .route("/player/repeat", post(set_repeat));

    Router::new()
        .route("/docs", get(get_scalar_docs))
        .nest("/api/v1", v1_routes)
        // Legacy root aliases for full backwards compatibility
        .route("/openapi.json", get(get_openapi_json))
        .route("/asyncapi.json", get(get_asyncapi_json))
        .route("/health", get(get_health))
        .route("/status", get(get_status))
        .route("/ws", get(ws_handler))
        .route("/volume", post(set_volume))
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/next", post(next))
        .route("/previous", post(previous))
        .route("/play-context", post(play_context))
        .with_state(state)
}

async fn get_scalar_docs() -> Html<&'static str> {
    Html(
        r#"<!doctype html>
<html>
  <head>
    <title>Spotify Node API Reference</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
  </head>
  <body>
    <script id="api-reference" data-url="/api/v1/openapi.json"></script>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  </body>
</html>"#,
    )
}

async fn get_openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

async fn get_asyncapi_json() -> Json<serde_json::Value> {
    Json(build_asyncapi_spec())
}

#[allow(clippy::too_many_arguments)]
pub async fn start_api(
    node_id: String,
    device_name: String,
    spirc: Arc<RwLock<Option<Arc<Spirc>>>>,
    _mixer: SharedMixer,
    shared_state: crate::state::SharedPlaybackState,
    metadata_cache: Arc<MetadataCache>,
    ws_broadcast: broadcast::Sender<String>,
    port: u16,
    state_file_path: Option<std::path::PathBuf>,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        node_id,
        device_name,
        spirc,
        shared_state,
        metadata_cache,
        ws_broadcast,
        state_file_path,
    });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!(
        "API server listening on port {} (Scalar UI: /docs, OpenAPI: /api/v1/openapi.json, AsyncAPI: /api/v1/asyncapi.json)",
        port
    );
    axum::serve(listener, app).await?;

    Ok(())
}

/// Get node health and connection status.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    tag = "Status & Monitoring",
    responses(
        (status = 200, description = "Node is healthy", body = HealthResponse)
    )
)]
async fn get_health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let is_connected = {
        let s = crate::lock_state!(state.shared_state);
        s.is_connected
    };

    Json(HealthResponse {
        status: "ok".to_string(),
        version: crate::app_version().to_string(),
        node_id: state.node_id.clone(),
        device_name: state.device_name.clone(),
        is_connected,
    })
}

async fn build_status_snapshot(state: &AppState) -> NodeStatusResponse {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    let (
        is_connected,
        connected_user,
        is_playing,
        is_active_device,
        volume,
        position_ms,
        shuffle,
        repeat_context,
        repeat_track,
        autoplay,
        context_uri,
        context_name,
        current_track_uri,
    ) = {
        let s = crate::lock_state!(state.shared_state);
        (
            s.is_connected,
            s.connected_user.clone(),
            s.was_playing,
            s.was_active_device,
            s.volume,
            s.current_position_ms(now_ms),
            s.shuffle,
            s.repeat_context,
            s.repeat_track,
            s.autoplay,
            s.context_uri.clone(),
            s.context_name.clone(),
            s.last_track_uri.clone(),
        )
    };

    let track = if let Some(ref uri) = current_track_uri {
        state
            .metadata_cache
            .get_cached_by_uri(uri)
            .await
            .map(|m| TrackMetadataDto {
                track_id: uri.clone(),
                title: m.title,
                artist: m.artist,
                album: m.album,
                duration_ms: m.duration_ms,
                cover_url: m.cover_url,
            })
    } else {
        None
    };

    NodeStatusResponse {
        node_id: state.node_id.clone(),
        device_name: state.device_name.clone(),
        version: crate::app_version().to_string(),
        is_connected,
        connected_user,
        is_playing,
        is_active_device,
        volume,
        position_ms,
        shuffle,
        repeat_context,
        repeat_track,
        autoplay,
        context_uri,
        context_name,
        current_track_uri,
        track,
    }
}

/// Get complete playback status and cached track metadata.
#[utoipa::path(
    get,
    path = "/api/v1/status",
    tag = "Status & Monitoring",
    responses(
        (status = 200, description = "Current playback state and track metadata", body = NodeStatusResponse)
    )
)]
async fn get_status(State(state): State<Arc<AppState>>) -> Json<NodeStatusResponse> {
    Json(build_status_snapshot(&state).await)
}

/// Subscribe to the real-time WebSocket event stream (`ws://<host>:<port>/api/v1/ws`).
///
/// Immediately upon connection, the server sends an `InitialStateEvent` (`Event = "initial_state"`).
/// Afterwards, all live `WebhookPayload` and `MetadataPayload` events (see `WebSocketEventPacket` schema and `/api/v1/asyncapi.json`) are streamed as JSON text frames.
#[utoipa::path(
    get,
    path = "/api/v1/ws",
    tag = "Status & Monitoring",
    responses(
        (status = 101, description = "Switching Protocols to WebSocket stream emitting `WebSocketEventPacket` JSON frames", body = WebSocketEventPacket)
    )
)]
async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_client(socket, state))
}

async fn handle_ws_client(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.ws_broadcast.subscribe();

    // Immediately emit initial_state snapshot upon connection using the typed InitialStateEvent struct
    let snapshot = build_status_snapshot(&state).await;
    let init_payload = InitialStateEvent {
        group_id: state.node_id.clone(),
        event: "initial_state".to_string(),
        status: snapshot,
    };
    if let Ok(init_str) = serde_json::to_string(&init_payload) {
        if socket.send(Message::Text(init_str.into())).await.is_err() {
            return;
        }
    }

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(msg) => {
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("WebSocket client lagged by {} messages", skipped);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    #[allow(clippy::collapsible_match)]
                    Some(Ok(Message::Ping(p))) => {
                        if socket.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        }
    }
}

/// Set playback volume (`0..=100`).
#[utoipa::path(
    post,
    path = "/api/v1/volume",
    tag = "Player Control",
    request_body = VolumeRequest,
    responses(
        (status = 200, description = "Volume updated", body = String, example = "OK")
    )
)]
async fn set_volume(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<VolumeRequest>,
) -> (axum::http::StatusCode, &'static str) {
    let volume_percent = payload.volume.min(100);
    let volume_u16 = ((volume_percent as f64 / 100.0) * 65535.0) as u16;

    info!(
        "Setting volume to {}% (u16: {})",
        volume_percent, volume_u16
    );

    {
        let mut s = crate::lock_state!(state.shared_state);
        s.volume = volume_percent;
        s.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if let Some(ref path) = state.state_file_path {
            s.save(path);
        }
    }

    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        info!(
            "No active Spotify session; cached volume {}% (u16: {}) for when session connects",
            volume_percent, volume_u16
        );

        let payload = WebhookPayload {
            group_id: state.node_id.clone(),
            event: "volume_set".to_string(),
            volume: Some(volume_u16 as u32),
            position_ms: None,
            duration_ms: None,
            track_id: None,
            interval_ms: None,
            is_playing: None,
            context_uri: None,
            context_name: None,
        };
        if let Ok(json_str) = serde_json::to_string(&payload) {
            let _ = state.ws_broadcast.send(json_str);
        }

        return (axum::http::StatusCode::OK, "OK");
    };

    info!("[SPIRC] -> set_volume({})", volume_u16);
    let _ = spirc.set_volume(volume_u16);
    (axum::http::StatusCode::OK, "OK")
}

/// Pause playback.
#[utoipa::path(
    post,
    path = "/api/v1/pause",
    tag = "Player Control",
    responses(
        (status = 200, description = "Playback paused", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn pause(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, &'static str) {
    info!("Pausing playback");
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Pause request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    info!("[SPIRC] -> pause()");
    let _ = spirc.pause();
    (axum::http::StatusCode::OK, "OK")
}

/// Resume playback (automatically activates device and restores context/station if needed).
#[utoipa::path(
    post,
    path = "/api/v1/resume",
    tag = "Player Control",
    responses(
        (status = 200, description = "Playback resumed", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn resume(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, &'static str) {
    info!("Resuming playback");
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Resume request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let (last_track, pos, context_uri, shuffle, repeat_ctx, repeat_trk, autoplay) = {
        let s = crate::lock_state!(state.shared_state);
        (
            s.last_track_uri.clone(),
            s.last_position,
            s.context_uri.clone(),
            s.shuffle,
            s.repeat_context,
            s.repeat_track,
            s.autoplay,
        )
    };

    info!(
        "[SPIRC] Resume state: last_track={:?}, pos={}ms, context={:?}, shuffle={}, repeat_ctx={}, repeat_trk={}",
        last_track, pos, context_uri, shuffle, repeat_ctx, repeat_trk
    );

    if let Some(track_uri) = last_track {
        info!("[SPIRC] -> activate()");
        let _ = spirc.activate();
        let spirc_clone = spirc.clone();
        let track_uri_clone = track_uri.clone();
        tokio::spawn(async move {
            info!("[SPIRC] Waiting 1.5s for session transfer...");
            tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
            use librespot_connect::{
                LoadContextOptions, LoadRequest, LoadRequestOptions, Options, PlayingTrack,
            };
            let mut opts = LoadRequestOptions {
                start_playing: true,
                seek_to: if pos > 0 { pos } else { 0 },
                playing_track: Some(PlayingTrack::Uri(track_uri_clone.clone())),
                ..Default::default()
            };

            let req = if let Some(ctx_uri) = context_uri {
                opts.context_options = Some(LoadContextOptions::Options(Options {
                    shuffle,
                    repeat: repeat_ctx,
                    repeat_track: repeat_trk,
                }));
                info!(
                    "[SPIRC] -> load(context={}, track={}, start_playing=true, seek_to={}ms, shuffle={}, repeat={})",
                    ctx_uri, track_uri_clone, pos, shuffle, repeat_ctx
                );
                LoadRequest::from_context_uri(ctx_uri, opts)
            } else if autoplay {
                let station_uri =
                    track_uri_clone.replace("spotify:track:", "spotify:station:track:");
                opts.context_options = Some(LoadContextOptions::Autoplay);
                info!(
                    "[SPIRC] -> load(context={}, track={}, Autoplay, start_playing=true, seek_to={}ms)",
                    station_uri, track_uri_clone, pos
                );
                LoadRequest::from_context_uri(station_uri, opts)
            } else {
                info!(
                    "[SPIRC] -> load(tracks=[{}], start_playing=true, seek_to={}ms) (no context, no autoplay)",
                    track_uri_clone, pos
                );
                LoadRequest::from_tracks(vec![track_uri_clone], opts)
            };
            let _ = spirc_clone.load(req);
            info!("[SPIRC] load() call completed");
        });
    } else {
        info!("[SPIRC] -> activate() (no last track, fallback)");
        let _ = spirc.activate();
        let spirc_clone = spirc.clone();
        tokio::spawn(async move {
            info!("[SPIRC] Waiting 3s then play()...");
            tokio::time::sleep(tokio::time::Duration::from_millis(3000)).await;
            info!("[SPIRC] -> play() (fallback, no track)");
            let _ = spirc_clone.play();
        });
    }

    (axum::http::StatusCode::OK, "OK")
}

/// Skip to the next track.
#[utoipa::path(
    post,
    path = "/api/v1/next",
    tag = "Player Control",
    responses(
        (status = 200, description = "Skipped to next track", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn next(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, &'static str) {
    info!("Skipping to next track");
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Next request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let was_active = {
        let s = crate::lock_state!(state.shared_state);
        s.was_active_device
    };

    if !was_active {
        info!("[SPIRC] -> activate() (device was NOT active, before next)");
        let _ = spirc.activate();
    }

    let spirc_clone = spirc.clone();
    tokio::spawn(async move {
        if !was_active {
            info!("[SPIRC] Waiting 1.5s for session transfer...");
            tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        }
        info!("[SPIRC] -> next()");
        let _ = spirc_clone.next();
    });

    (axum::http::StatusCode::OK, "OK")
}

/// Skip to the previous track.
#[utoipa::path(
    post,
    path = "/api/v1/previous",
    tag = "Player Control",
    responses(
        (status = 200, description = "Skipped to previous track", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn previous(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, &'static str) {
    info!("Skipping to previous track");
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Previous request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let was_active = {
        let s = crate::lock_state!(state.shared_state);
        s.was_active_device
    };

    if !was_active {
        info!("[SPIRC] -> activate() (device was NOT active, before prev)");
        let _ = spirc.activate();
    }

    let spirc_clone = spirc.clone();
    tokio::spawn(async move {
        if !was_active {
            info!("[SPIRC] Waiting 1.5s for session transfer...");
            tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        }
        info!("[SPIRC] -> prev()");
        let _ = spirc_clone.prev();
    });

    (axum::http::StatusCode::OK, "OK")
}

/// Activate this node and start playing a Spotify context URI (playlist, album, artist, or station).
#[utoipa::path(
    post,
    path = "/api/v1/play-context",
    tag = "Player Control",
    request_body = PlayContextRequest,
    responses(
        (status = 200, description = "Context playback initiated", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn play_context(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<PlayContextRequest>,
) -> (axum::http::StatusCode, &'static str) {
    info!("Playing context: {}", payload.context_uri);
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Play context request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    info!("[SPIRC] -> activate() for play_context");
    let _ = spirc.activate();
    let spirc_clone = spirc.clone();
    let context_uri = payload.context_uri.clone();
    tokio::spawn(async move {
        info!("[SPIRC] Waiting 1.5s for session transfer...");
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
        use librespot_connect::{LoadRequest, LoadRequestOptions};
        let opts = LoadRequestOptions {
            start_playing: true,
            ..Default::default()
        };
        info!(
            "[SPIRC] -> load(context={}, start_playing=true)",
            context_uri
        );
        let req = LoadRequest::from_context_uri(context_uri, opts);
        let _ = spirc_clone.load(req);
        info!("[SPIRC] load() context completed");
    });
    (axum::http::StatusCode::OK, "OK")
}

/// Seek to a specific position in milliseconds within the current track.
#[utoipa::path(
    post,
    path = "/api/v1/seek",
    tag = "Player Control",
    request_body = SeekRequest,
    responses(
        (status = 200, description = "Seek command sent", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn seek(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SeekRequest>,
) -> (axum::http::StatusCode, &'static str) {
    info!("Seeking to {}ms", payload.position_ms);
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Seek request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let _ = spirc.set_position_ms(payload.position_ms);
    (axum::http::StatusCode::OK, "OK")
}

/// Enable or disable shuffle mode.
#[utoipa::path(
    post,
    path = "/api/v1/shuffle",
    tag = "Player Control",
    request_body = ShuffleRequest,
    responses(
        (status = 200, description = "Shuffle mode updated", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn set_shuffle(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<ShuffleRequest>,
) -> (axum::http::StatusCode, &'static str) {
    info!("Setting shuffle to {}", payload.enabled);
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Shuffle request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let _ = spirc.shuffle(payload.enabled);
    (axum::http::StatusCode::OK, "OK")
}

/// Configure context repeat and single-track repeat modes.
#[utoipa::path(
    post,
    path = "/api/v1/repeat",
    tag = "Player Control",
    request_body = RepeatRequest,
    responses(
        (status = 200, description = "Repeat mode updated", body = String, example = "OK"),
        (status = 400, description = "No active Spotify session", body = String)
    )
)]
async fn set_repeat(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RepeatRequest>,
) -> (axum::http::StatusCode, &'static str) {
    info!(
        "Setting repeat (context={}, track={})",
        payload.context, payload.track
    );
    let spirc_guard = state.spirc.read().await;
    let Some(ref spirc) = *spirc_guard else {
        warn!("Repeat request failed: No active Spotify session");
        return (
            axum::http::StatusCode::BAD_REQUEST,
            "ERROR: No active session",
        );
    };

    let _ = spirc.repeat(payload.context);
    let _ = spirc.repeat_track(payload.track);
    (axum::http::StatusCode::OK, "OK")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn make_test_state() -> Arc<AppState> {
        let pb = crate::state::PlaybackState {
            volume: 45,
            was_playing: true,
            was_active_device: true,
            is_connected: true,
            connected_user: Some("testuser".to_string()),
            last_track_uri: Some("spotify:track:4uLU6hMCjMI75M1A2tKUQC".to_string()),
            context_uri: Some("spotify:playlist:37i9dQZF1DXcBWIGoYBM5M".to_string()),
            context_name: Some("Today's Top Hits".to_string()),
            last_position: 12345,
            position_updated_at_ms: 0,
            ..Default::default()
        };

        let (ws_tx, _) = broadcast::channel(16);

        Arc::new(AppState {
            node_id: "living-room".to_string(),
            device_name: "Living Room Speaker".to_string(),
            spirc: Arc::new(RwLock::new(None)),
            shared_state: Arc::new(std::sync::Mutex::new(pb)),
            metadata_cache: Arc::new(MetadataCache::new()),
            ws_broadcast: ws_tx,
            state_file_path: None,
        })
    }

    #[tokio::test]
    async fn test_get_health_v1_and_legacy() {
        let state = make_test_state();
        let app = build_router(state);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["node_id"], "living-room");
        assert_eq!(json["device_name"], "Living Room Speaker");
        assert_eq!(json["is_connected"], true);

        // Also verify legacy /health route works
        let legacy_resp = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(legacy_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_status_v1_includes_cached_track_metadata() {
        let state = make_test_state();
        let track_uri =
            librespot_core::SpotifyUri::from_uri("spotify:track:4uLU6hMCjMI75M1A2tKUQC").unwrap();
        state
            .metadata_cache
            .insert_ready_for_test(
                track_uri,
                crate::metadata_cache::CachedMeta {
                    title: "Never Gonna Give You Up".to_string(),
                    artist: "Rick Astley".to_string(),
                    album: "Whenever You Need Somebody".to_string(),
                    duration_ms: 213573,
                    cover_url: Some("https://i.scdn.co/image/cover123".to_string()),
                },
            )
            .await;

        let app = build_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body_bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["node_id"], "living-room");
        assert_eq!(json["is_playing"], true);
        assert_eq!(json["volume"], 45);
        assert_eq!(json["context_name"], "Today's Top Hits");
        assert_eq!(json["track"]["title"], "Never Gonna Give You Up");
        assert_eq!(json["track"]["artist"], "Rick Astley");
        assert_eq!(json["track"]["duration_ms"], 213573);
    }

    #[tokio::test]
    async fn test_openapi_and_asyncapi_specs_and_scalar_docs() {
        let state = make_test_state();
        let app = build_router(state);

        // 1. Verify OpenAPI 3.1 JSON contains all REST paths AND WebSocket event schemas
        let oa_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(oa_resp.status(), StatusCode::OK);
        let oa_bytes = oa_resp.into_body().collect().await.unwrap().to_bytes();
        let oa_json: serde_json::Value = serde_json::from_slice(&oa_bytes).unwrap();
        assert!(oa_json["paths"]["/api/v1/status"].is_object());
        assert!(oa_json["paths"]["/api/v1/ws"].is_object());
        assert!(oa_json["components"]["schemas"]["WebhookPayload"].is_object());
        assert!(oa_json["components"]["schemas"]["MetadataPayload"].is_object());
        assert!(oa_json["components"]["schemas"]["InitialStateEvent"].is_object());
        assert!(oa_json["components"]["schemas"]["WebSocketEventPacket"].is_object());

        // 2. Verify AsyncAPI 2.6.0 JSON describes the /api/v1/ws channel and messages
        let aa_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/asyncapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(aa_resp.status(), StatusCode::OK);
        let aa_bytes = aa_resp.into_body().collect().await.unwrap().to_bytes();
        let aa_json: serde_json::Value = serde_json::from_slice(&aa_bytes).unwrap();
        assert_eq!(aa_json["asyncapi"], "2.6.0");
        assert!(aa_json["channels"]["/api/v1/ws"]["subscribe"].is_object());
        assert!(aa_json["components"]["messages"]["PlayerEventMessage"].is_object());

        // 3. Verify Scalar UI HTML page is served at /docs
        let docs_resp = app
            .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(docs_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_post_commands_without_session_return_bad_request() {
        let state = make_test_state();
        let app = build_router(state);

        for uri in [
            "/api/v1/pause",
            "/api/v1/player/pause",
            "/pause",
            "/api/v1/next",
        ] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_set_volume_without_session_caches_volume_and_returns_ok() {
        let state = make_test_state();
        let app = build_router(state.clone());

        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/volume")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"volume": 20}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Verify volume was cached in shared_state
        {
            let s = crate::lock_state!(state.shared_state);
            assert_eq!(s.volume, 20);
        }

        // Verify GET /api/v1/status returns the cached volume
        let status_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status_resp.status(), StatusCode::OK);
        let bytes = status_resp.into_body().collect().await.unwrap().to_bytes();
        let status_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(status_json["volume"], 20);

        // Also test /player/volume and /volume routes
        for uri in ["/api/v1/player/volume", "/volume"] {
            let resp = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"volume": 75}"#))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let s = crate::lock_state!(state.shared_state);
            assert_eq!(s.volume, 75);
        }
    }

    #[tokio::test]
    async fn test_committed_specs_are_in_sync() {
        let openapi_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
        assert!(
            openapi_file.exists(),
            "openapi.json must exist in repo root"
        );
        let openapi_content =
            std::fs::read_to_string(&openapi_file).expect("Failed to read openapi.json");
        let openapi_committed: serde_json::Value =
            serde_json::from_str(&openapi_content).expect("Invalid openapi.json");

        let generated_openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
        assert_eq!(
            openapi_committed["paths"], generated_openapi["paths"],
            "openapi.json paths have drifted from Rust code! Run `cargo run -- --dump-specs`."
        );
        assert_eq!(
            openapi_committed["components"], generated_openapi["components"],
            "openapi.json components have drifted from Rust code! Run `cargo run -- --dump-specs`."
        );

        let asyncapi_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("asyncapi.json");
        assert!(
            asyncapi_file.exists(),
            "asyncapi.json must exist in repo root"
        );
        let asyncapi_content =
            std::fs::read_to_string(&asyncapi_file).expect("Failed to read asyncapi.json");
        let asyncapi_committed: serde_json::Value =
            serde_json::from_str(&asyncapi_content).expect("Invalid asyncapi.json");

        let generated_asyncapi = build_asyncapi_spec();
        assert_eq!(
            asyncapi_committed["channels"], generated_asyncapi["channels"],
            "asyncapi.json channels have drifted from Rust code! Run `cargo run -- --dump-specs`."
        );
        assert_eq!(
            asyncapi_committed["components"], generated_asyncapi["components"],
            "asyncapi.json components have drifted from Rust code! Run `cargo run -- --dump-specs`."
        );
    }
}
