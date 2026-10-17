//! Durable, versioned transfer checkpoints. Callers must validate the remote
//! resource before trusting any recorded completed segment.

use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentCheckpoint {
    pub start: u64,
    pub end: u64,
    pub completed: bool,
}

impl SegmentCheckpoint {
    pub fn length(&self) -> Option<u64> {
        self.end.checked_sub(self.start)?.checked_add(1)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DownloadCheckpoint {
    pub version: u32,
    pub source_url: String,
    pub final_url: String,
    pub total_bytes: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub segments: Vec<SegmentCheckpoint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    UnsupportedVersion,
    InvalidStructure,
    RemoteChanged,
    InvalidSegment(usize),
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CheckpointError {}

impl DownloadCheckpoint {
    pub const VERSION: u32 = 1;

    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.version != Self::VERSION {
            return Err(CheckpointError::UnsupportedVersion);
        }
        if self.source_url.is_empty() || self.final_url.is_empty()
            || self.total_bytes == 0 || self.segments.is_empty() || self.segments.len() > 16
        {
            return Err(CheckpointError::InvalidStructure);
        }
        let mut offset = 0u64;
        for (index, segment) in self.segments.iter().enumerate() {
            let length = segment.length().ok_or(CheckpointError::InvalidSegment(index))?;
            if segment.start != offset || length == 0 {
                return Err(CheckpointError::InvalidSegment(index));
            }
            offset = segment.end.checked_add(1).ok_or(CheckpointError::InvalidSegment(index))?;
        }
        if offset != self.total_bytes {
            return Err(CheckpointError::InvalidStructure);
        }
        Ok(())
    }

    /// Requires the URL, content length and *available* strong validator to
    /// remain unchanged. Unvalidated checkpoint metadata must never imply
    /// completed data is safe to reuse.
    pub fn validate_remote(
        &self, final_url: &str, total_bytes: u64,
        etag: Option<&str>, last_modified: Option<&str>,
    ) -> Result<(), CheckpointError> {
        self.validate()?;
        if self.final_url != final_url || self.total_bytes != total_bytes {
            return Err(CheckpointError::RemoteChanged);
        }
        let consistent = match (&self.etag, &self.last_modified) {
            (Some(expected), _) if !expected.starts_with("W/") =>
                etag == Some(expected.as_str()),
            (_, Some(expected)) => last_modified == Some(expected.as_str()),
            _ => false,
        };
        if !consistent { return Err(CheckpointError::RemoteChanged); }
        Ok(())
    }

    pub fn mark_completed(&mut self, index: usize, actual_bytes: u64) -> Result<(), CheckpointError> {
        let segment = self.segments.get_mut(index).ok_or(CheckpointError::InvalidSegment(index))?;
        if segment.length() != Some(actual_bytes) {
            return Err(CheckpointError::InvalidSegment(index));
        }
        segment.completed = true;
        Ok(())
    }

    /// On recovery, recheck recorded segment lengths before reusing any bytes.
    pub fn reconcile_files(&mut self, directory: &Path) -> io::Result<()> {
        for (index, segment) in self.segments.iter_mut().enumerate() {
            if segment.completed {
                let path = directory.join(format!("{index:04}.part"));
                let matches = match fs::metadata(path) {
                    Ok(metadata) => metadata.is_file() && Some(metadata.len()) == segment.length(),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                    Err(error) => return Err(error),
                };
                if !matches { segment.completed = false; }
            }
        }
        Ok(())
    }
}

/// Writes through a uniquely-created sibling file and renames after syncing.
/// A failed write leaves the previously committed checkpoint intact.
pub fn save(path: &Path, checkpoint: &DownloadCheckpoint) -> io::Result<()> {
    checkpoint.validate().map_err(io::Error::other)?;
    let temporary = path.with_extension(format!("checkpoint-{}.tmp", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&temporary)?;
        serde_json::to_writer(&mut file, checkpoint).map_err(io::Error::other)?;
        file.flush()?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() { let _ = fs::remove_file(&temporary); }
    result
}

pub fn load(path: &Path) -> io::Result<DownloadCheckpoint> {
    let file = fs::File::open(path)?;
    let checkpoint: DownloadCheckpoint = serde_json::from_reader(file).map_err(io::Error::other)?;
    checkpoint.validate().map_err(io::Error::other)?;
    Ok(checkpoint)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> DownloadCheckpoint {
        DownloadCheckpoint {
            version: 1,
            source_url: "https://example.com/file".into(),
            final_url: "https://example.com/file".into(),
            total_bytes: 10,
            etag: Some("\"strong\"".into()),
            last_modified: None,
            segments: vec![
                SegmentCheckpoint { start: 0, end: 4, completed: false },
                SegmentCheckpoint { start: 5, end: 9, completed: false },
            ],
        }
    }

    #[test]
    fn segment_checkpoint_lifecycle() {
        let mut data = sample();
        data.validate().unwrap();
        assert!(data.mark_completed(0, 4).is_err());
        data.mark_completed(0, 5).unwrap();
        assert!(data.segments[0].completed);
        assert!(data.validate_remote("https://example.com/file", 10, Some("\"strong\""), None).is_ok());
        assert!(data.validate_remote("https://example.com/file", 10, Some("\"new\""), None).is_err());
        assert!(data.validate_remote("https://example.com/file", 11, Some("\"strong\""), None).is_err());
    }

    #[test]
    fn invalid_ranges_rejected() {
        let mut data = sample();
        data.segments[1].start = 6;
        assert!(data.validate().is_err());
    }

    #[test]
    fn weak_etag_cannot_authorize_resume_alone() {
        let mut data = sample();
        data.etag = Some("W/\"weak\"".into());
        assert!(data.validate_remote("https://example.com/file", 10, Some("W/\"weak\""), None).is_err());
    }

    #[test]
    fn json_round_trip() {
        let data = sample();
        let serialized = serde_json::to_string(&data).unwrap();
        let decoded: DownloadCheckpoint = serde_json::from_str(&serialized).unwrap();
        assert_eq!(data, decoded);
    }
}
