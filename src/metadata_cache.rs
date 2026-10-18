use librespot_core::session::Session;
use librespot_core::SpotifyUri;
use librespot_metadata::{Metadata, Track};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct CachedMeta {
    pub title: String,
    pub artist: String,
    pub album: String,
    pub duration_ms: u32,
    pub cover_url: Option<String>,
}

#[derive(Clone)]
enum CacheEntry {
    Pending(watch::Receiver<Option<CachedMeta>>),
    Ready(CachedMeta, bool), // metadata, webhook_sent
}

pub enum CacheResult {
    Fetched(CachedMeta),
    Cached(CachedMeta),
}

pub struct MetadataCache {
    entries: Mutex<HashMap<SpotifyUri, CacheEntry>>,
}

impl MetadataCache {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_cached(&self, track_id: &SpotifyUri) -> Option<CachedMeta> {
        let entries = self.entries.lock().await;
        if let Some(CacheEntry::Ready(meta, _)) = entries.get(track_id) {
            return Some(meta.clone());
        }
        None
    }

    pub async fn get_cached_by_uri(&self, uri_str: &str) -> Option<CachedMeta> {
        let track_id = SpotifyUri::from_uri(uri_str).ok()?;
        self.get_cached(&track_id).await
    }

    #[cfg(test)]
    pub async fn insert_ready_for_test(&self, track_id: SpotifyUri, meta: CachedMeta) {
        let mut entries = self.entries.lock().await;
        entries.insert(track_id, CacheEntry::Ready(meta, false));
    }

    pub async fn get_or_fetch(
        &self,
        session: &Session,
        track_id: SpotifyUri,
    ) -> Option<CacheResult> {
        let mut entries = self.entries.lock().await;

        if let Some(entry) = entries.get(&track_id) {
            match entry {
                CacheEntry::Ready(meta, _) => {
                    tracing::debug!("Metadata cache hit for {:?}", track_id);
                    return Some(CacheResult::Cached(meta.clone()));
                }
                CacheEntry::Pending(rx) => {
                    info!(
                        "Metadata fetch already pending for {:?}, waiting...",
                        track_id
                    );
                    let mut rx = rx.clone();
                    drop(entries);
                    if let Err(_) = rx.changed().await {
                        return None;
                    }
                    return rx.borrow().clone().map(CacheResult::Cached);
                }
            }
        }

        // Missing: Start fetching
        let (tx, rx) = watch::channel(None);
        entries.insert(track_id.clone(), CacheEntry::Pending(rx));
        drop(entries);

        tracing::debug!("Metadata cache miss for {:?}, starting fetch", track_id);

        // Actual fetch logic adapted from events.rs
        let mut retries = 3;
        let mut track_result = None;

        while retries > 0 {
            match tokio::time::timeout(Duration::from_secs(10), Track::get(session, &track_id))
                .await
            {
                Ok(Ok(track)) => {
                    track_result = Some(track);
                    break;
                }
                Ok(Err(e)) => {
                    error!(
                        "Error fetching metadata for {:?} ({} retries left): {:?}",
                        track_id,
                        retries - 1,
                        e
                    );
                }
                Err(_) => {
                    error!(
                        "Timed out fetching metadata for {:?} ({} retries left)",
                        track_id,
                        retries - 1
                    );
                }
            }
            retries -= 1;
            if retries > 0 {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }

        let track = match track_result {
            Some(t) => t,
            None => {
                let mut entries = self.entries.lock().await;
                entries.remove(&track_id); // Clean up pending
                return None;
            }
        };

        let artist_names: Vec<String> = track.artists.iter().map(|a| a.name.clone()).collect();
        let artist_str = artist_names.join(", ");

        let album_name = track.album.name.clone();
        let cover_url = track.album.covers.iter().next().map(|id| {
            let hex = id
                .id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            format!("https://i.scdn.co/image/{}", hex)
        });

        let meta = CachedMeta {
            title: track.name,
            artist: artist_str,
            album: album_name,
            duration_ms: track.duration as u32,
            cover_url,
        };

        let mut entries = self.entries.lock().await;
        entries.insert(track_id, CacheEntry::Ready(meta.clone(), false));
        let _ = tx.send(Some(meta.clone()));

        Some(CacheResult::Fetched(meta))
    }

    pub async fn try_mark_webhook_sent(&self, track_id: &SpotifyUri) -> bool {
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(track_id) {
            if let CacheEntry::Ready(_, ref mut sent) = entry {
                if !*sent {
                    *sent = true;
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_lookup_and_webhook_deduplication() {
        let cache = MetadataCache::new();
        let uri_str = "spotify:track:4uLU6hMCjMI75M1A2tKUQC";
        let uri = SpotifyUri::from_uri(uri_str).unwrap();
        let meta = CachedMeta {
            title: "Never Gonna Give You Up".to_string(),
            artist: "Rick Astley".to_string(),
            album: "Whenever You Need Somebody".to_string(),
            duration_ms: 213573,
            cover_url: Some("https://i.scdn.co/image/abcd".to_string()),
        };

        assert_eq!(cache.get_cached_by_uri(uri_str).await, None);
        cache.insert_ready_for_test(uri.clone(), meta.clone()).await;

        assert_eq!(cache.get_cached_by_uri(uri_str).await, Some(meta));
        assert!(cache.try_mark_webhook_sent(&uri).await);
        // Second call should return false to prevent duplicate metadata webhooks
        assert!(!cache.try_mark_webhook_sent(&uri).await);
    }
}
