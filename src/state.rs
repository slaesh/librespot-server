use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct PlaybackState {
    pub last_track_uri: Option<String>,
    #[serde(default)]
    pub context_uri: Option<String>,
    #[serde(default)]
    pub context_name: Option<String>,
    #[serde(default)]
    pub track_index: u32,
    #[serde(default)]
    pub track_gids: Vec<Vec<u8>>,
    pub last_position: u32,
    #[serde(default)]
    pub position_updated_at_ms: u64,
    pub was_playing: bool,
    #[serde(default)]
    pub was_active_device: bool,
    #[serde(default)]
    pub volume: u32,
    #[serde(default)]
    pub shuffle: bool,
    #[serde(default)]
    pub repeat_context: bool,
    #[serde(default)]
    pub repeat_track: bool,
    #[serde(default = "default_true")]
    pub autoplay: bool,
    pub timestamp: u64, // Unix timestamp in seconds

    #[serde(skip)]
    pub is_connected: bool,
    #[serde(skip)]
    pub connected_user: Option<String>,
}

fn default_true() -> bool {
    true
}

pub type SharedPlaybackState = Arc<Mutex<PlaybackState>>;

impl PlaybackState {
    pub fn save(&self, path: &std::path::Path) {
        if let Ok(json) = serde_json::to_string(self) {
            let _ = std::fs::write(path, json);
        }
    }

    pub fn load(path: &std::path::Path) -> Option<Self> {
        if let Ok(json) = std::fs::read_to_string(path) {
            if let Ok(state) = serde_json::from_str::<Self>(&json) {
                return Some(state);
            }
        }
        None
    }

    /// Computes the live estimated position in milliseconds based on `last_position`
    /// and elapsed wall-clock time since `position_updated_at_ms` when playing.
    pub fn current_position_ms(&self, now_ms: u64) -> u32 {
        if !self.was_playing || self.position_updated_at_ms == 0 {
            return self.last_position;
        }
        let elapsed = now_ms.saturating_sub(self.position_updated_at_ms);
        self.last_position.saturating_add(elapsed as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("playback_state.json");

        let state = PlaybackState {
            last_track_uri: Some("spotify:track:12345".to_string()),
            context_uri: Some("spotify:playlist:abcde".to_string()),
            context_name: Some("My Playlist".to_string()),
            track_index: 2,
            track_gids: vec![vec![1, 2, 3]],
            last_position: 42000,
            position_updated_at_ms: 1700000000000,
            was_playing: true,
            was_active_device: true,
            volume: 65,
            shuffle: true,
            repeat_context: true,
            repeat_track: false,
            autoplay: true,
            timestamp: 1700000000,
            is_connected: true,
            connected_user: Some("alice".to_string()),
        };

        state.save(&file_path);
        let loaded = PlaybackState::load(&file_path).expect("Should load state");

        assert_eq!(loaded.last_track_uri, state.last_track_uri);
        assert_eq!(loaded.context_uri, state.context_uri);
        assert_eq!(loaded.context_name, state.context_name);
        assert_eq!(loaded.last_position, 42000);
        assert_eq!(loaded.volume, 65);
        assert!(loaded.shuffle);
        assert!(loaded.repeat_context);
        assert!(!loaded.repeat_track);
        // Transient fields are skipped on serialization
        assert!(!loaded.is_connected);
        assert_eq!(loaded.connected_user, None);
    }

    #[test]
    fn test_current_position_ms_calculation() {
        let mut state = PlaybackState::default();
        state.last_position = 10_000;
        state.position_updated_at_ms = 1_000_000;
        state.was_playing = false;

        // When paused, position does not advance
        assert_eq!(state.current_position_ms(1_005_000), 10_000);

        // When playing, position advances by elapsed ms
        state.was_playing = true;
        assert_eq!(state.current_position_ms(1_005_500), 15_500);
    }
}
