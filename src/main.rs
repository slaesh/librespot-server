use std::env;
use librespot_core::authentication::Credentials;
use librespot_core::config::{SessionConfig, DeviceType};
use librespot_core::session::Session;
use librespot_core::cache::Cache;
use librespot_playback::config::PlayerConfig;
use librespot_playback::player::Player;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;
use anyhow::Context;
use std::sync::Arc;
use futures_util::StreamExt;
use std::path::PathBuf;

mod sink;
mod api;
mod events;
mod shared_mixer;
mod state;
mod metadata_cache;

#[macro_export]
macro_rules! lock_state {
    ($lock:expr) => {
        match $lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::error!("Mutex poisoned! Recovering state...");
                poisoned.into_inner()
            }
        }
    };
}

use crate::shared_mixer::SharedMixer;
use librespot_connect::Spirc;
use librespot_connect::ConnectConfig;
use librespot_connect::{LoadRequest, LoadRequestOptions, LoadContextOptions, Options, PlayingTrack};

pub fn app_version() -> &'static str {
    const VERSION: Option<&str> = option_env!("APP_VERSION");
    match VERSION {
        Some(v) if !v.is_empty() => v,
        _ => env!("CARGO_PKG_VERSION"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("librespot-server v{} (librespot v{})", app_version(), librespot_core::version::SEMVER);
        return Ok(());
    }

    if args.iter().any(|a| a == "--dump-specs") {
        let openapi = <crate::api::ApiDoc as utoipa::OpenApi>::openapi();
        let openapi_json = serde_json::to_string_pretty(&openapi)?;
        std::fs::write("openapi.json", openapi_json)?;

        let asyncapi = crate::api::build_asyncapi_spec();
        let asyncapi_json = serde_json::to_string_pretty(&asyncapi)?;
        std::fs::write("asyncapi.json", asyncapi_json)?;

        println!("Successfully wrote openapi.json and asyncapi.json");
        return Ok(());
    }

    if args.iter().any(|a| a == "--healthcheck") {
        let api_port = env::var("API_PORT")
            .or_else(|_| env::var("HTTP_PORT"))
            .unwrap_or_else(|_| "25000".to_string())
            .parse::<u16>()
            .unwrap_or(25000);

        let url = format!("http://127.0.0.1:{}/api/v1/health", api_port);
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()?;

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let text = resp.text().await.unwrap_or_default();
                if text.contains("\"status\":\"ok\"") || text.contains("\"status\": \"ok\"") {
                    std::process::exit(0);
                } else {
                    eprintln!("Healthcheck response body unhealthy: {}", text);
                    std::process::exit(1);
                }
            }
            Ok(resp) => {
                eprintln!("Healthcheck HTTP status error: {}", resp.status());
                std::process::exit(1);
            }
            Err(e) => {
                eprintln!("Healthcheck request error: {}", e);
                std::process::exit(1);
            }
        }
    }

    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    if let Err(e) = tracing::subscriber::set_global_default(subscriber) {
        eprintln!("Failed to set tracing subscriber: {}", e);
    }

    let version = app_version();
    info!("Starting librespot-server v{} (librespot v{})...", version, librespot_core::version::SEMVER);

    // Load configuration from environment (supporting both generic Open-Source and Orchestrator env vars)
    let device_name = env::var("DEVICE_NAME").unwrap_or_else(|_| "Spotify Node".to_string());
    let pipe_path = env::var("PIPE_PATH")
        .or_else(|_| env::var("FIFO_PATH"))
        .unwrap_or_else(|_| "/tmp/snapfifo".to_string());
    let api_port = env::var("API_PORT")
        .or_else(|_| env::var("HTTP_PORT"))
        .unwrap_or_else(|_| "25000".to_string())
        .parse::<u16>()?;
    let webhook_url = env::var("WEBHOOK_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let group_id = env::var("NODE_ID")
        .or_else(|_| env::var("GROUP_ID"))
        .unwrap_or_else(|_| "default".to_string());
    let cache_root = env::var("CACHE_PATH")
        .or_else(|_| env::var("CACHE_DIR"))
        .unwrap_or_else(|_| "/var/cache/librespot".to_string());
    let cache_path = PathBuf::from(cache_root).join(&group_id);
    std::fs::create_dir_all(&cache_path).context("Failed to create cache directory")?;
    
    let username = env::var("SPOTIFY_USERNAME").ok();
    let password = env::var("SPOTIFY_PASSWORD").ok();
    
    // Initial volume (0-100 to 0-65535)
    let initial_vol_percent = env::var("INITIAL_VOLUME").unwrap_or_else(|_| "50".to_string()).parse::<u32>().unwrap_or(50);

    use sha1::{Sha1, Digest};
    let device_id = hex::encode(Sha1::digest(group_id.as_bytes()));
    info!(
        "Device ID: {} (Node/Group: {}, Webhook: {:?})",
        device_id,
        group_id,
        webhook_url.as_deref().unwrap_or("disabled (WebSocket only)")
    );

    let mut session_config = SessionConfig::default();
    session_config.device_id = device_id.clone();
    
    // Initialize Cache
    let cache = Cache::new(Some(cache_path.clone()), None, None, None)
        .context("Failed to initialize cache")?;
    
    let state_file_path = cache_path.join("playback_state.json");
    let mut initial_state = state::PlaybackState::default();
    initial_state.volume = initial_vol_percent;
    
    const RECOVERY_THRESHOLD_SECS: u64 = 3600; // 60 minutes
    
    if let Some(loaded_state) = state::PlaybackState::load(&state_file_path) {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let age = now.saturating_sub(loaded_state.timestamp);
        
        // Always restore volume from cache if it exists
        initial_state.volume = loaded_state.volume;

        if age < RECOVERY_THRESHOLD_SECS {
            info!("Found valid persisted state ({}s old, threshold: {}s). Restoring for Cold Boot recovery.", age, RECOVERY_THRESHOLD_SECS);
            initial_state = loaded_state;
        } else {
            info!("Found stale persisted state ({}s old, threshold: {}s). Restoring volume only.", age, RECOVERY_THRESHOLD_SECS);
        }
    }

    let mut current_creds = if let Some(creds) = cache.credentials() {
        info!("Restoring session credentials from cache...");
        Some(creds)
    } else {
        None
    };

    let (creds_tx, mut creds_rx) = tokio::sync::mpsc::unbounded_channel();

    // Spawn persistent Discovery task
    let disc_id = device_id.clone();
    let disc_name = device_name.clone();
    let client_id = session_config.client_id.clone();
    tokio::spawn(async move {
        loop {
            info!("Starting Discovery service for '{}'...", disc_name);
            match librespot_discovery::Discovery::builder(disc_id.clone(), client_id.clone())
                .name(disc_name.clone())
                .launch() 
            {
                Ok(mut discovery) => {
                    while let Some(creds) = discovery.next().await {
                        info!("Discovery received credentials for user: {:?}", creds.username);
                        let _ = creds_tx.send(creds);
                    }
                }
                Err(e) => {
                    error!("Discovery failed to start: {}. Retrying in 10s...", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                }
            }
        }
    });

    let shared_state = Arc::new(std::sync::Mutex::new(initial_state));
    let metadata_cache = Arc::new(metadata_cache::MetadataCache::new());
    let (ws_broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(64);

    // Initialize AppEvent channel and persistent loop
    let (app_event_tx, app_event_rx) = tokio::sync::mpsc::unbounded_channel();
    let group_id_clone = group_id.clone();
    let webhook_url_clone = webhook_url.clone();
    let ws_broadcast_for_events = ws_broadcast_tx.clone();
    let shared_state_clone = shared_state.clone();
    let state_file_path_clone = state_file_path.clone();
    let metadata_cache_clone = metadata_cache.clone();

    let api_spirc_handle = Arc::new(tokio::sync::RwLock::new(None));
    let spirc_handle_for_events = api_spirc_handle.clone();

    tokio::spawn(async move {
        events::start_event_loop(
            None,
            app_event_rx,
            group_id_clone,
            webhook_url_clone,
            ws_broadcast_for_events,
            shared_state_clone,
            state_file_path_clone,
            metadata_cache_clone,
            spirc_handle_for_events,
        ).await;
    });

    // Persistent Heartbeat Task
    let heartbeat_tx = app_event_tx.clone();
    let heartbeat_state = shared_state.clone();
    let heartbeat_file_path = state_file_path.clone();
    let mut heartbeat_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    tokio::spawn(async move {
        let mut last_disk_save = std::time::Instant::now();
        loop {
            heartbeat_interval.tick().await;
            let (track_id, pos, was_playing, age) = {
                let mut state = lock_state!(heartbeat_state);
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
                
                let mut pos = state.last_position;
                if state.was_playing && state.position_updated_at_ms > 0 {
                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                    let elapsed = now_ms.saturating_sub(state.position_updated_at_ms);
                    pos = pos.saturating_add(elapsed as u32);
                    
                    // Advance internal state so that it persists the current position on disk saves
                    state.last_position = pos;
                    state.position_updated_at_ms = now_ms;
                }

                // Every 10 seconds, refresh the "Last Seen" timestamp and save position on disk
                if last_disk_save.elapsed().as_secs() >= 10 {
                    state.timestamp = now;
                    state.save(&heartbeat_file_path);
                    last_disk_save = std::time::Instant::now();
                }

                (state.last_track_uri.clone().unwrap_or_default(), pos, state.was_playing, now.saturating_sub(state.timestamp))
            };
            
            // Send heartbeat webhook continuously.. (as long as we're alive)
            // We allow a 30s grace period (matching our disk save) to keep the pulse alive
            if age <= 30 {
                let _ = heartbeat_tx.send(events::AppEvent::Heartbeat {
                    track_id,
                    position_ms: pos,
                    duration_ms: 0, 
                    interval_ms: 10000,
                    is_playing: was_playing,
                });
            }
        }
    });

    // Persistent API server
    let api_spirc_clone = api_spirc_handle.clone();
    let initial_vol_u16_dummy = {
        let state = lock_state!(shared_state);
        ((state.volume as f64 / 100.0) * 65535.0) as u16
    };
    let shared_mixer_dummy = SharedMixer::new(initial_vol_u16_dummy);
    let api_port_val = api_port;
    let shared_state_api = shared_state.clone();
    let metadata_cache_api = metadata_cache.clone();
    let ws_broadcast_api = ws_broadcast_tx.clone();
    let node_id_api = group_id.clone();
    let device_name_api = device_name.clone();

    tokio::spawn(async move {
        if let Err(e) = api::start_api(
            node_id_api,
            device_name_api,
            api_spirc_clone,
            shared_mixer_dummy,
            shared_state_api,
            metadata_cache_api,
            ws_broadcast_api,
            api_port_val,
        ).await {
            error!("API server failed: {}", e);
        }
    });

    if current_creds.is_none() && (username.is_some() && password.is_some()) {
        info!("Using credentials from environment variables.");
        current_creds = Some(Credentials::with_password(username.clone().unwrap(), password.clone().unwrap()));
    }

    loop {
        if current_creds.is_none() {
            // Check if we should recover from cache
            let state = lock_state!(shared_state);
            let age = {
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
                now.saturating_sub(state.timestamp)
            };
            
            if let Some(creds) = cache.credentials() {
                if state.was_playing {
                    info!("Session lost while playing (State age: {}s). Attempting active recovery...", age);
                } else {
                    info!("Session lost while paused (State age: {}s). Reconnecting for warm standby...", age);
                }
                current_creds = Some(creds);
            }
        }

        if current_creds.is_none() {
            info!("No credentials available. Waiting for Spotify Discovery or environment variables...");
            current_creds = creds_rx.recv().await;
        }

        let creds = current_creds.take().unwrap();
        
        let session = Session::new(session_config.clone(), Some(cache.clone()));
        
        // In 0.8.0, session.connect is called internally by Spirc::new.
        // We will just pass the credentials down to Spirc::new later.

        // Initialize Shared Mixer
        let initial_vol_u16 = {
            let state = lock_state!(shared_state);
            ((state.volume as f64 / 100.0) * 65535.0) as u16
        };
        let shared_mixer = SharedMixer::new(initial_vol_u16);

        // Initialize custom sink
        let sink_path = pipe_path.clone();
        
        // Create player
        let player_config = PlayerConfig::default();
        let player = Player::new(
            player_config,
            session.clone(),
            Box::new(shared_mixer.clone()),
            move || {
                Box::new(sink::FifoSink::new(sink_path.clone()))
            },
        );
        let mut event_receiver = player.get_player_event_channel();

        // Initialize Connect (Spirc)
        let spirc_config = ConnectConfig {
            name: device_name.clone(),
            device_type: DeviceType::Speaker,
            initial_volume: initial_vol_u16,
            disable_volume: false,
            volume_steps: 64,
            emit_set_queue_events: true,
            is_group: false,
        };

        let (spirc, spirc_task) = match Spirc::new(
            spirc_config,
            session.clone(),
            creds.clone(),
            player.clone(),
            Arc::new(shared_mixer.clone()),
        ).await {
            Ok(res) => res,
            Err(e) => {
                error!("Failed to initialize Spirc: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                continue;
            }
        };

        info!("Connected to Spotify as {}", session.username());
        let _ = app_event_tx.send(events::AppEvent::Connected { 
            username: session.username().to_string(),
            session: session.clone() 
        });
        
        tokio::pin!(spirc_task);
        let spirc_arc = Arc::new(spirc);
        
        // Update API handle
        {
            let mut guard = api_spirc_handle.write().await;
            *guard = Some(spirc_arc.clone());
        }

        // Re-assert volume after 2 seconds to fight Spotify's 25% default on new sessions
        let spirc_clone = spirc_arc.clone();
        let vol_to_reassert = initial_vol_u16;
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let _ = spirc_clone.set_volume(vol_to_reassert);
        });

        // Recovery Logic: Restore session state to Spirc to make the device active
        {
            let state = lock_state!(shared_state);
            
            if let Some(ref uri) = state.last_track_uri {
                if let Ok(_track) = librespot_core::SpotifyUri::from_uri(uri) {
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
                    let age = now.saturating_sub(state.timestamp);
                    
                    if state.was_active_device {
                        info!("Recovery: Claiming active focus (State age: {}s).", age);
                        let _ = spirc_arc.activate();
                        
                        let spirc_arc_clone = spirc_arc.clone();
                        let track_uri_clone = uri.clone();
                        let context_uri = state.context_uri.clone();
                        let pos = state.last_position;
                        let was_playing = state.was_playing;
                        let shuffle = state.shuffle;
                        let repeat_context = state.repeat_context;
                        let repeat_track = state.repeat_track;
                        let autoplay = state.autoplay;

                        tokio::spawn(async move {
                            tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
                            let mut opts = LoadRequestOptions::default();
                            opts.start_playing = was_playing;
                            if pos > 0 {
                                opts.seek_to = pos;
                            }
                            opts.playing_track = Some(PlayingTrack::Uri(track_uri_clone.clone()));
                            let req = if let Some(ctx_uri) = context_uri {
                                opts.context_options = Some(LoadContextOptions::Options(Options {
                                    shuffle,
                                    repeat: repeat_context,
                                    repeat_track,
                                }));
                                info!("Recovery: Loading context={} track={} (was_playing={}, pos={}ms)", ctx_uri, track_uri_clone, was_playing, pos);
                                LoadRequest::from_context_uri(ctx_uri, opts)
                            } else if autoplay {
                                // Spotify cannot resolve Autoplay for a standalone track queue.
                                // We must convert the track URI into a radio station context URI!
                                let station_uri = track_uri_clone.replace("spotify:track:", "spotify:station:track:");
                                opts.context_options = Some(LoadContextOptions::Autoplay);
                                info!("Recovery: Loading track={} as Station {} with Autoplay (was_playing={}, pos={}ms)", track_uri_clone, station_uri, was_playing, pos);
                                LoadRequest::from_context_uri(station_uri, opts)
                            } else {
                                info!("Recovery: Loading track={} (was_playing={}, pos={}ms, no autoplay)", track_uri_clone, was_playing, pos);
                                LoadRequest::from_tracks(vec![track_uri_clone], opts)
                            };
                            let _ = spirc_arc_clone.load(req);
                        });
                    } else {
                        info!("Recovery: Silent Warm Standby. Not stealing focus (State age: {}s).", age);
                    }
                }
            }
        }

        // Proxy Spotify events to AppEvent channel
        let proxy_tx = app_event_tx.clone();
        let mut proxy_handle = tokio::spawn(async move {
            while let Some(event) = event_receiver.recv().await {
                let _ = proxy_tx.send(events::AppEvent::Spotify(event));
            }
        });

        info!("Spotify Node is ready and running.");

        tokio::select! {
            _ = &mut spirc_task => {
                info!("Spirc task finished");
            }
            _ = &mut proxy_handle => {
                info!("Proxy handle finished");
            }
            new_creds = creds_rx.recv() => {
                if let Some(nc) = new_creds {
                    info!("Received new credentials via Discovery. Switching user...");
                    current_creds = Some(nc);
                }
            }
        }

        // Reset API handle
        {
            let mut guard = api_spirc_handle.write().await;
            *guard = None;
        }

        // Send Disconnected event
        let _ = app_event_tx.send(events::AppEvent::Disconnected);

        // Abort background tasks
        proxy_handle.abort();

        info!("Session closed or switching. Restarting loop in 2s...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}
