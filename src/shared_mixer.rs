use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use librespot_playback::mixer::{Mixer, VolumeGetter, MixerConfig};

pub struct SharedMixer {
    volume: Arc<AtomicU16>,
}

impl SharedMixer {
    pub fn new(initial_volume: u16) -> Self {
        Self {
            volume: Arc::new(AtomicU16::new(initial_volume)),
        }
    }
}

impl Mixer for SharedMixer {
    fn open(_config: MixerConfig) -> Result<Self, librespot_core::Error> where Self: Sized {
        Ok(Self::new(32768))
    }

    fn volume(&self) -> u16 {
        self.volume.load(Ordering::Relaxed)
    }

    fn set_volume(&self, volume: u16) {
        self.volume.store(volume, Ordering::Relaxed);
    }

    fn get_soft_volume(&self) -> Box<dyn VolumeGetter + Send> {
        Box::new(self.clone())
    }
}

impl VolumeGetter for SharedMixer {
    fn attenuation_factor(&self) -> f64 {
        // Return 1.0 to ensure full volume in the audio stream.
        // We let Snapserver handle the actual volume attenuation to avoid double-attenuation.
        1.0
    }
}

// Implement Send + Sync for VolumeGetter requirement
unsafe impl Send for SharedMixer {}
unsafe impl Sync for SharedMixer {}

impl Clone for SharedMixer {
    fn clone(&self) -> Self {
        Self {
            volume: Arc::clone(&self.volume),
        }
    }
}
