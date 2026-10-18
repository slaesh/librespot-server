use librespot_core::session::Session;
use librespot_playback::player::PlayerEvent;
use serde::Serialize;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::metadata_cache::{CacheResult, MetadataCache};
use std::sync::Arc;
use utoipa::ToSchema;

/// Real-time player state event emitted via WebSocket (`/api/v1/ws`) and optional HTTP Webhook (`WEBHOOK_URL`).
#[derive(Serialize, Clone, Debug, ToSchema)]
pub struct WebhookPayload {
    /// Unique identifier of the Spotify node / room group.
    #[serde(rename = "GroupId")]
    #[schema(example = "living-room")]
    pub group_id: String,
    /// Event type (`playing`, `paused`, `stopped`, `changed`, `volume_set`, `heartbeat`, `set_queue`, `seeked`, `connected`, `disconnected`, etc.).
    #[serde(rename = "Event")]
    #[schema(example = "playing")]
    pub event: String,
    /// Raw 16-bit Spotify volume (`0..=65535`), present on `volume_set` events.
    #[serde(rename = "Volume", skip_serializing_if = "Option::is_none")]
    #[schema(example = 32768)]
    pub volume: Option<u32>,
    /// Current playback position in milliseconds.
    #[serde(rename = "PositionMs", skip_serializing_if = "Option::is_none")]
    #[schema(example = 45210)]
    pub position_ms: Option<u32>,
    /// Track duration in milliseconds (if known at event time).
    #[serde(rename = "DurationMs", skip_serializing_if = "Option::is_none")]
    #[schema(example = 213573)]
    pub duration_ms: Option<u32>,
    /// Spotify track URI (`spotify:track:...`).
    #[serde(rename = "TrackId", skip_serializing_if = "Option::is_none")]
    #[schema(example = "spotify:track:4uLU6hMCjMI75M1A2tKUQC")]
    pub track_id: Option<String>,
    /// Heartbeat interval in milliseconds (present on `heartbeat` events).
    #[serde(rename = "IntervalMs", skip_serializing_if = "Option::is_none")]
    #[schema(example = 10000)]
    pub interval_ms: Option<u32>,
    /// Whether the player is actively playing audio.
    #[serde(rename = "IsPlaying", skip_serializing_if = "Option::is_none")]
    #[schema(example = true)]
    pub is_playing: Option<bool>,
    /// Active playback context URI (`spotify:playlist:...`, `spotify:album:...`, `spotify:artist:...`).
    #[serde(rename = "ContextUri", skip_serializing_if = "Option::is_none")]
    #[schema(example = "spotify:playlist:37i9dQZF1DXcBWIGoYBM5M")]
    pub context_uri: Option<String>,
    /// Resolved human-readable name of the playlist, album, or artist context.
    #[serde(rename = "ContextName", skip_serializing_if = "Option::is_none")]
    #[schema(example = "Today's Top Hits")]
    pub context_name: Option<String>,
}

pub enum AppEvent {
    Connected {
        username: String,
        session: Session,
    },
    Disconnected,
    Spotify(PlayerEvent),
    Heartbeat {
        track_id: String,
        position_ms: u32,
        duration_ms: u32,
        interval_ms: u32,
        is_playing: bool,
    },
}

/// Rich track metadata event emitted once per track when resolved from Spotify (`Event = "metadata_update"`).
#[derive(Serialize, Clone, Debug, ToSchema)]
pub struct MetadataPayload {
    /// Unique identifier of the Spotify node / room group.
    #[serde(rename = "GroupId")]
    #[schema(example = "living-room")]
    pub group_id: String,
    /// Always `"metadata_update"`.
    #[serde(rename = "Event")]
    #[schema(example = "metadata_update")]
    pub event: String,
    /// Spotify track URI (`spotify:track:...`).
    #[serde(rename = "TrackId")]
    #[schema(example = "spotify:track:4uLU6hMCjMI75M1A2tKUQC")]
    pub track_id: String,
    /// Track title.
    #[serde(rename = "Title")]
    #[schema(example = "Never Gonna Give You Up")]
    pub title: String,
    /// Comma-separated artist names.
    #[serde(rename = "Artist")]
    #[schema(example = "Rick Astley")]
    pub artist: String,
    /// Album title.
    #[serde(rename = "Album")]
    #[schema(example = "Whenever You Need Somebody")]
    pub album: String,
    /// Track duration in milliseconds.
    #[serde(rename = "DurationMs")]
    #[schema(example = 213573)]
    pub duration_ms: u32,
    /// Full HTTPS URL to the album cover image on Spotify's CDN.
    #[serde(rename = "CoverUrl")]
    #[schema(example = "https://i.scdn.co/image/ab67616d0000b2735755e164993798e0c9ef7d7a")]
    pub cover_url: Option<String>,
}

fn dispatch_event_json<T: Serialize + Send + 'static>(
    payload: &T,
    ws_broadcast: &broadcast::Sender<String>,
    webhook_url: &Option<String>,
    client: &reqwest::Client,
) {
    if let Ok(json_str) = serde_json::to_string(payload) {
        let _ = ws_broadcast.send(json_str);
    }

    let Some(ref url) = webhook_url else {
        return;
    };

    let url_clone = url.clone();
    let client_clone = client.clone();
    let Ok(json_value) = serde_json::to_value(payload) else {
        return;
    };

    tokio::spawn(async move {
        let mut retries = 3;
        let mut wait = Duration::from_secs(1);

        loop {
            match client_clone.post(&url_clone).json(&json_value).send().await {
                Ok(resp) if resp.status().is_success() => break,
                Ok(resp) => {
                    warn!(
                        "Webhook returned error status {}: Retrying in {:?}...",
                        resp.status(),
                        wait
                    );
                    if retries > 0 {
                        tokio::time::sleep(wait).await;
                        retries -= 1;
                        wait *= 2;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    if retries > 0 {
                        warn!(
                            "Failed to send webhook ({} retries left): {}. Retrying in {:?}...",
                            retries, e, wait
                        );
                        tokio::time::sleep(wait).await;
                        retries -= 1;
                        wait *= 2;
                    } else {
                        error!("Failed to send webhook after multiple retries: {}", e);
                        break;
                    }
                }
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
pub async fn start_event_loop(
    mut session: Option<Session>,
    mut receiver: mpsc::UnboundedReceiver<AppEvent>,
    group_id: String,
    webhook_url: Option<String>,
    ws_broadcast: broadcast::Sender<String>,
    shared_state: crate::state::SharedPlaybackState,
    state_file_path: std::path::PathBuf,
    metadata_cache: Arc<MetadataCache>,
    spirc_handle: std::sync::Arc<
        tokio::sync::RwLock<Option<std::sync::Arc<librespot_connect::Spirc>>>,
    >,
) {
    let client = reqwest::Client::new();
    let mut last_end_of_track_time: Option<u64> = None;
    let mut long_pause_detected: bool = false;
    let mut last_pause_time: u64 = 0;
    let mut last_eot_track: Option<String> = None;
    let mut consecutive_eot_count: u32 = 0;

    while let Some(app_event) = receiver.recv().await {
        let (event_name, volume, pos_ms, dur_ms, t_id, i_ms, is_playing_flag) = match app_event {
            AppEvent::Connected {
                username,
                session: new_session,
            } => {
                info!("Spotify Connected as {}", username);
                session = Some(new_session.clone());

                let (has_track, is_playing, was_active, last_track, pos) = {
                    let mut state = crate::lock_state!(shared_state);
                    state.is_connected = true;
                    state.connected_user = Some(username);
                    (
                        state.last_track_uri.clone(),
                        state.was_playing,
                        state.was_active_device,
                        state.last_track_uri.clone(),
                        state.last_position,
                    )
                };

                if was_active {
                    if let Some(ref uri) = has_track {
                        if let Ok(track_id) = librespot_core::SpotifyUri::from_uri(uri) {
                            info!("Restoring metadata for recovered session: {}", uri);
                            tokio::spawn(fetch_and_send_metadata(
                                new_session.clone(),
                                track_id,
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                true,
                            ));
                        }
                    }
                }

                if is_playing {
                    (
                        "playing",
                        None,
                        Some(pos),
                        None,
                        last_track,
                        None,
                        Some(true),
                    )
                } else {
                    ("connected", None, None, None, None, None, None)
                }
            }
            AppEvent::Disconnected => {
                warn!("Spotify Disconnected");
                session = None;
                {
                    let mut state = crate::lock_state!(shared_state);
                    state.is_connected = false;
                    state.connected_user = None;
                }
                ("disconnected", None, None, None, None, None, None)
            }
            AppEvent::Heartbeat {
                track_id,
                position_ms,
                duration_ms,
                interval_ms,
                is_playing,
            } => {
                if session.is_none() {
                    continue;
                }
                (
                    "heartbeat",
                    None,
                    Some(position_ms),
                    Some(duration_ms),
                    Some(track_id),
                    Some(interval_ms),
                    Some(is_playing),
                )
            }
            AppEvent::Spotify(event) => {
                match &event {
                    PlayerEvent::VolumeChanged { volume } => {
                        let percentage = (*volume as f64 / 65535.0 * 100.0).round() as u32;
                        info!(
                            "Player event: VolumeSet (Volume: {} [{}%])",
                            volume, percentage
                        );

                        let mut state = crate::lock_state!(shared_state);
                        state.volume = percentage;
                        state.timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state.save(&state_file_path);

                        (
                            "volume_set",
                            Some(*volume as u32),
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    PlayerEvent::TrackChanged { audio_item, .. } => {
                        let track_id = &audio_item.track_id;
                        info!("Player event: Changed (Track: {:?})", track_id);
                        let mut state = crate::lock_state!(shared_state);
                        state.last_track_uri = Some(track_id.to_uri());
                        state.last_position = 0;
                        state.position_updated_at_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        state.timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state.save(&state_file_path);

                        if let Some(ref s) = session {
                            tokio::spawn(fetch_and_send_metadata(
                                s.clone(),
                                track_id.clone(),
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                true,
                            ));
                        }

                        long_pause_detected = false;
                        last_pause_time = 0;
                        consecutive_eot_count = 0;
                        last_eot_track = None;

                        (
                            "changed",
                            None,
                            Some(0),
                            None,
                            Some(track_id.to_uri()),
                            None,
                            None,
                        )
                    }
                    PlayerEvent::Stopped { .. } => {
                        info!("Player event: Stopped");
                        {
                            let mut state = crate::lock_state!(shared_state);
                            state.was_playing = false;
                            state.was_active_device = false;
                            state.timestamp = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            state.save(&state_file_path);
                        }

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        if let Some(eot) = last_end_of_track_time {
                            if now.saturating_sub(eot) < 3000 {
                                warn!("Detected Stop immediately after EndOfTrack. Auto-engaging Autoplay queue to keep playback alive!");
                                if let Some(ref spirc) = *spirc_handle.read().await {
                                    let spirc_clone = spirc.clone();
                                    tokio::spawn(async move {
                                        info!("[SPIRC] Auto-recovery: -> next() and play()");
                                        let _ = spirc_clone.next();
                                        tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                            .await;
                                        let _ = spirc_clone.play();
                                    });
                                }
                                long_pause_detected = false;
                            }
                        }

                        ("stopped", None, None, None, None, None, Some(false))
                    }
                    PlayerEvent::Playing {
                        track_id,
                        position_ms,
                        ..
                    } => {
                        info!(
                            "Player event: Playing (Track: {:?}, Position: {}ms)",
                            track_id, position_ms
                        );
                        let mut state = crate::lock_state!(shared_state);
                        state.last_track_uri = Some(track_id.to_uri());
                        state.last_position = *position_ms;
                        state.position_updated_at_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        state.was_playing = true;
                        state.was_active_device = true;
                        state.timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state.save(&state_file_path);

                        if let Some(ref s) = session {
                            tokio::spawn(fetch_and_send_metadata(
                                s.clone(),
                                track_id.clone(),
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                true,
                            ));
                        }

                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        if last_pause_time > 0
                            && now.saturating_sub(last_pause_time) > 5 * 60 * 1000
                        {
                            long_pause_detected = true;
                            info!("Detected a pause longer than 5 minutes. Armed URL expiry workaround.");
                        }
                        let _ = long_pause_detected;
                        last_pause_time = 0;
                        consecutive_eot_count = 0;
                        last_eot_track = None;

                        (
                            "playing",
                            None,
                            Some(*position_ms),
                            None,
                            Some(track_id.to_uri()),
                            None,
                            Some(true),
                        )
                    }
                    PlayerEvent::Paused {
                        track_id,
                        position_ms,
                        ..
                    } => {
                        info!(
                            "Player event: Paused (Track: {:?}, Position: {}ms)",
                            track_id, position_ms
                        );
                        let mut state = crate::lock_state!(shared_state);
                        state.last_track_uri = Some(track_id.to_uri());
                        state.last_position = *position_ms;
                        state.position_updated_at_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        state.was_playing = false;
                        state.was_active_device = true;
                        state.timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state.save(&state_file_path);

                        if let Some(ref s) = session {
                            tokio::spawn(fetch_and_send_metadata(
                                s.clone(),
                                track_id.clone(),
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                true,
                            ));
                        }

                        last_pause_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);

                        (
                            "paused",
                            None,
                            Some(*position_ms),
                            None,
                            Some(track_id.to_uri()),
                            None,
                            Some(false),
                        )
                    }
                    PlayerEvent::Loading { track_id, .. } => {
                        info!("Player event: Loading (Track: {:?})", track_id);
                        let mut state = crate::lock_state!(shared_state);
                        state.last_track_uri = Some(track_id.to_uri());
                        state.timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        state.save(&state_file_path);

                        if let Some(ref s) = session {
                            tokio::spawn(fetch_and_send_metadata(
                                s.clone(),
                                track_id.clone(),
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                true,
                            ));
                        }

                        (
                            "loading",
                            None,
                            None,
                            None,
                            Some(track_id.to_uri()),
                            None,
                            None,
                        )
                    }
                    PlayerEvent::Preloading { track_id, .. } => {
                        info!("Player event: Preloading (Track: {:?})", track_id);

                        if let Some(ref s) = session {
                            tokio::spawn(fetch_and_send_metadata(
                                s.clone(),
                                track_id.clone(),
                                group_id.clone(),
                                webhook_url.clone(),
                                ws_broadcast.clone(),
                                metadata_cache.clone(),
                                false, // Prefetch only into cache, do NOT send webhook for preloaded next track
                            ));
                        }

                        (
                            "preloading",
                            None,
                            None,
                            None,
                            Some(track_id.to_uri()),
                            None,
                            None,
                        )
                    }
                    PlayerEvent::EndOfTrack { track_id, .. } => {
                        info!("Player event: EndOfTrack (Track: {:?})", track_id);
                        let track_uri_str = track_id.to_uri();
                        if last_eot_track.as_deref() == Some(&track_uri_str) {
                            consecutive_eot_count += 1;
                        } else {
                            last_eot_track = Some(track_uri_str.clone());
                            consecutive_eot_count = 1;
                        }

                        if consecutive_eot_count >= 3 {
                            warn!("Track {:?} stuck in EndOfTrack loop ({}x). Auto-skipping to next track in 1.5s!", track_uri_str, consecutive_eot_count);
                            if let Some(ref spirc) = *spirc_handle.read().await {
                                let spirc_clone = spirc.clone();
                                tokio::spawn(async move {
                                    info!("[SPIRC] Stuck-loop recovery: -> next()");
                                    let _ = spirc_clone.next();
                                });
                            }
                            consecutive_eot_count = 0;
                        }

                        last_end_of_track_time = Some(
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0),
                        );
                        (
                            "end_of_track",
                            None,
                            None,
                            None,
                            Some(track_uri_str),
                            None,
                            None,
                        )
                    }
                    PlayerEvent::TimeToPreloadNextTrack { .. } => {
                        info!("Player event: TimeToPreloadNextTrack");
                        ("preloading", None, None, None, None, None, None)
                    }
                    PlayerEvent::ShuffleChanged { shuffle } => {
                        info!("Player event: ShuffleChanged (shuffle: {})", shuffle);
                        let mut state = crate::lock_state!(shared_state);
                        state.shuffle = *shuffle;
                        state.save(&state_file_path);
                        ("shuffle_changed", None, None, None, None, None, None)
                    }
                    PlayerEvent::RepeatChanged { context, track } => {
                        info!(
                            "Player event: RepeatChanged (context: {}, track: {})",
                            context, track
                        );
                        let mut state = crate::lock_state!(shared_state);
                        state.repeat_context = *context;
                        state.repeat_track = *track;
                        state.save(&state_file_path);
                        ("repeat_changed", None, None, None, None, None, None)
                    }
                    PlayerEvent::Unavailable { track_id, .. } => {
                        info!("Player event: Unavailable (Track: {:?})", track_id);
                        (
                            "unavailable",
                            None,
                            None,
                            None,
                            Some(track_id.to_uri()),
                            None,
                            None,
                        )
                    }
                    PlayerEvent::SetQueue { context_uri, .. } => {
                        info!("Player event: SetQueue (Context: {})", context_uri);
                        {
                            let mut state = crate::lock_state!(shared_state);
                            if !context_uri.is_empty() {
                                state.context_uri = Some(context_uri.clone());
                            } else {
                                state.context_uri = None;
                                state.context_name = None;
                            }
                            state.save(&state_file_path);
                        }

                        let cloned_session = session.clone();
                        let group_id_clone = group_id.clone();
                        let webhook_url_clone = webhook_url.clone();
                        let ws_broadcast_clone = ws_broadcast.clone();
                        let shared_state_clone = shared_state.clone();
                        let state_file_path_clone = state_file_path.clone();
                        let ctx_clone = context_uri.clone();
                        let client_clone = client.clone();

                        tokio::spawn(async move {
                            let mut context_name = None;

                            if let Some(session) = cloned_session {
                                if !ctx_clone.is_empty() {
                                    use librespot_core::SpotifyUri;
                                    use librespot_metadata::{Album, Artist, Metadata, Playlist};

                                    if let Ok(uri) = SpotifyUri::from_uri(&ctx_clone) {
                                        match uri {
                                            SpotifyUri::Playlist { .. } => {
                                                if let Ok(playlist) =
                                                    Playlist::get(&session, &uri).await
                                                {
                                                    context_name =
                                                        Some(playlist.name().to_string());
                                                }
                                            }
                                            SpotifyUri::Album { .. } => {
                                                if let Ok(album) = Album::get(&session, &uri).await
                                                {
                                                    context_name = Some(album.name.clone());
                                                }
                                            }
                                            SpotifyUri::Artist { .. } => {
                                                if let Ok(artist) =
                                                    Artist::get(&session, &uri).await
                                                {
                                                    context_name = Some(artist.name.clone());
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            if let Some(ref resolved_name) = context_name {
                                let mut state = crate::lock_state!(shared_state_clone);
                                state.context_name = Some(resolved_name.clone());
                                state.save(&state_file_path_clone);
                            }

                            let payload = WebhookPayload {
                                group_id: group_id_clone,
                                event: "set_queue".to_string(),
                                volume: None,
                                position_ms: None,
                                duration_ms: None,
                                track_id: None,
                                interval_ms: None,
                                is_playing: None,
                                context_uri: Some(ctx_clone),
                                context_name,
                            };

                            info!(
                                "Dispatching event: set_queue (Context: {:?}, Name: {:?})",
                                payload.context_uri, payload.context_name
                            );
                            dispatch_event_json(
                                &payload,
                                &ws_broadcast_clone,
                                &webhook_url_clone,
                                &client_clone,
                            );
                        });

                        continue;
                    }
                    PlayerEvent::SessionConnected { .. } => {
                        info!("Player event: SessionConnected");
                        ("session_connected", None, None, None, None, None, None)
                    }
                    PlayerEvent::SessionDisconnected { .. } => {
                        info!("Player event: SessionDisconnected");
                        ("session_disconnected", None, None, None, None, None, None)
                    }
                    PlayerEvent::SessionClientChanged { client_id, .. } => {
                        info!("Player event: SessionClientChanged (client: {})", client_id);
                        ("session_client_changed", None, None, None, None, None, None)
                    }
                    PlayerEvent::AutoPlayChanged { auto_play } => {
                        info!("Player event: AutoPlayChanged (auto_play: {})", auto_play);
                        let mut state = crate::lock_state!(shared_state);
                        state.autoplay = *auto_play;
                        state.save(&state_file_path);
                        ("autoplay_changed", None, None, None, None, None, None)
                    }
                    PlayerEvent::FilterExplicitContentChanged { filter } => {
                        info!(
                            "Player event: FilterExplicitContentChanged (filter: {})",
                            filter
                        );
                        (
                            "filter_explicit_changed",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    PlayerEvent::PlayRequestIdChanged { play_request_id } => {
                        info!(
                            "Player event: PlayRequestIdChanged (id: {})",
                            play_request_id
                        );
                        (
                            "play_request_id_changed",
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    PlayerEvent::Seeked { position_ms, .. } => {
                        info!("Player event: Seeked (position: {}ms)", position_ms);
                        {
                            let mut state = crate::lock_state!(shared_state);
                            state.last_position = *position_ms;
                            state.position_updated_at_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                            state.save(&state_file_path);
                        }
                        ("seeked", None, Some(*position_ms), None, None, None, None)
                    }
                    PlayerEvent::PositionCorrection { position_ms, .. } => {
                        info!(
                            "Player event: PositionCorrection (position: {}ms)",
                            position_ms
                        );
                        {
                            let mut state = crate::lock_state!(shared_state);
                            state.last_position = *position_ms;
                            state.position_updated_at_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                        }
                        (
                            "position_correction",
                            None,
                            Some(*position_ms),
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    PlayerEvent::PositionChanged { position_ms, .. } => {
                        {
                            let mut state = crate::lock_state!(shared_state);
                            state.last_position = *position_ms;
                            state.position_updated_at_ms = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_millis() as u64)
                                .unwrap_or(0);
                        }
                        (
                            "position_changed",
                            None,
                            Some(*position_ms),
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                }
            }
        };

        let payload = WebhookPayload {
            group_id: group_id.clone(),
            event: event_name.to_string(),
            volume,
            position_ms: pos_ms,
            duration_ms: dur_ms,
            track_id: t_id,
            interval_ms: i_ms,
            is_playing: is_playing_flag,
            context_uri: None,
            context_name: None,
        };

        if payload.event == "heartbeat" {
            tracing::debug!(
                "Dispatching event: {} (Vol: {:?}, Pos: {:?})",
                payload.event,
                payload.volume,
                payload.position_ms
            );
        } else {
            info!(
                "Dispatching event: {} (Vol: {:?}, Pos: {:?})",
                payload.event, payload.volume, payload.position_ms
            );
        }

        dispatch_event_json(&payload, &ws_broadcast, &webhook_url, &client);
    }
}

async fn fetch_and_send_metadata(
    session: Session,
    track_id: librespot_core::SpotifyUri,
    group_id: String,
    webhook_url: Option<String>,
    ws_broadcast: broadcast::Sender<String>,
    cache: Arc<MetadataCache>,
    send_webhook: bool,
) {
    let result = match cache.get_or_fetch(&session, track_id.clone()).await {
        Some(res) => res,
        None => return,
    };

    let meta = match result {
        CacheResult::Fetched(m) => m,
        CacheResult::Cached(m) => m,
    };

    if !send_webhook {
        info!(
            "Prefetch-only: cached metadata for {:?}, not sending webhook",
            track_id
        );
        return;
    }

    // Atomically check if we should be the one to send the webhook for this track
    if !cache.try_mark_webhook_sent(&track_id).await {
        info!(
            "Skipping redundant metadata webhook for {:?} (already sent)",
            track_id
        );
        return;
    }

    let payload = MetadataPayload {
        group_id,
        event: "metadata_update".to_string(),
        track_id: track_id.to_uri(),
        title: meta.title,
        artist: meta.artist,
        album: meta.album,
        duration_ms: meta.duration_ms,
        cover_url: meta.cover_url,
    };

    info!(
        "Dispatching metadata update for {}: {} - {}",
        payload.group_id, payload.title, payload.artist
    );
    let client = reqwest::Client::new();
    dispatch_event_json(&payload, &ws_broadcast, &webhook_url, &client);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dispatch_event_broadcasts_to_websocket_without_webhook() {
        let (ws_tx, mut ws_rx) = broadcast::channel(16);
        let client = reqwest::Client::new();
        let payload = WebhookPayload {
            group_id: "node-1".to_string(),
            event: "playing".to_string(),
            volume: Some(32768),
            position_ms: Some(1200),
            duration_ms: Some(180000),
            track_id: Some("spotify:track:123".to_string()),
            interval_ms: None,
            is_playing: Some(true),
            context_uri: None,
            context_name: None,
        };

        dispatch_event_json(&payload, &ws_tx, &None, &client);

        let received = ws_rx.recv().await.expect("Should receive broadcast JSON");
        let parsed: serde_json::Value = serde_json::from_str(&received).unwrap();
        assert_eq!(parsed["GroupId"], "node-1");
        assert_eq!(parsed["Event"], "playing");
        assert_eq!(parsed["PositionMs"], 1200);
        assert_eq!(parsed["IsPlaying"], true);
    }

    #[tokio::test]
    async fn test_event_loop_ignores_set_queue_synchronous_fallback() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (ws_tx, mut ws_rx) = broadcast::channel(16);
        let shared_state = Arc::new(std::sync::Mutex::new(crate::state::PlaybackState::default()));
        let dir = tempfile::tempdir().unwrap();
        let state_file = dir.path().join("state.json");
        let metadata_cache = Arc::new(MetadataCache::new());
        let spirc_handle = Arc::new(tokio::sync::RwLock::new(None));

        tokio::spawn(start_event_loop(
            None,
            rx,
            "node-test".to_string(),
            None,
            ws_tx,
            shared_state.clone(),
            state_file,
            metadata_cache,
            spirc_handle,
        ));

        // Send a SetQueue event followed by Disconnected
        let _ = tx.send(AppEvent::Spotify(PlayerEvent::SetQueue {
            context_uri: "spotify:playlist:test123".to_string(),
            current_track: None,
            next_tracks: vec![],
            prev_tracks: vec![],
        }));
        let _ = tx.send(AppEvent::Disconnected);

        // We should only see "disconnected" and "set_queue", NEVER "ignore"
        let msg1 = ws_rx.recv().await.unwrap();
        let msg2 = ws_rx.recv().await.unwrap();

        let v1: serde_json::Value = serde_json::from_str(&msg1).unwrap();
        let v2: serde_json::Value = serde_json::from_str(&msg2).unwrap();
        let events = [
            v1["Event"].as_str().unwrap().to_string(),
            v2["Event"].as_str().unwrap().to_string(),
        ];

        assert!(!events.contains(&"ignore".to_string()));
        assert!(events.contains(&"set_queue".to_string()));
        assert!(events.contains(&"disconnected".to_string()));
    }
}
