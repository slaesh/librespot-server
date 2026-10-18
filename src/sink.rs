use std::fs::File;
use std::io::Write;
use librespot_playback::audio_backend::{Sink, SinkResult};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;

pub struct FifoSink {
    file: Option<File>,
    path: String,
}

impl FifoSink {
    pub fn new(path: String) -> Self {
        // We will open it lazily on first write, or we can try opening it now
        // Let's just try opening it now so we don't delay the first playback unnecessarily
        let file = match std::fs::OpenOptions::new().write(true).open(&path) {
            Ok(f) => Some(f),
            Err(e) => {
                tracing::warn!("Could not open audio pipe {} on startup: {}. Will retry on playback.", path, e);
                None
            }
        };
        Self { file, path }
    }

    fn write_with_retry(&mut self, data: &[u8]) -> SinkResult<()> {
        loop {
            if self.file.is_none() {
                match std::fs::OpenOptions::new().write(true).open(&self.path) {
                    Ok(f) => {
                        tracing::info!("Audio pipe {} opened successfully.", self.path);
                        self.file = Some(f);
                    }
                    Err(e) => {
                        tracing::warn!("Could not open audio pipe {}: {}. Retrying in 500ms...", self.path, e);
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        continue;
                    }
                }
            }

            if let Some(f) = &mut self.file {
                match f.write_all(data) {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        tracing::warn!("Failed to write to audio pipe (broken pipe?): {}. Reopening...", e);
                        self.file = None;
                        // Loop around to reopen
                    }
                }
            }
        }
    }
}

impl Sink for FifoSink {
    fn start(&mut self) -> SinkResult<()> {
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, converter: &mut Converter) -> SinkResult<()> {
        match packet {
            AudioPacket::Samples(samples) => {
                let samples_s16 = converter.f64_to_s16(&samples);
                let mut buffer = Vec::with_capacity(samples_s16.len() * 2);
                for &sample in &samples_s16 {
                    buffer.extend_from_slice(&sample.to_le_bytes());
                }
                self.write_with_retry(&buffer)?;
            }
            AudioPacket::Raw(data) => {
                self.write_with_retry(&data)?;
            }
        }
        Ok(())
    }
}
