//! Core download identity and lifecycle, independent from UI and networking.

use std::error::Error;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DownloadId(u64);

impl DownloadId {
    pub fn new(value: u64) -> Result<Self, DownloadError> {
        if value == 0 {
            return Err(DownloadError::InvalidId);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadState {
    Queued,
    Downloading,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl DownloadState {
    pub const fn can_transition_to(self, next: Self) -> bool {
        use DownloadState::*;
        matches!(
            (self, next),
            (Queued, Downloading | Paused | Cancelled)
                | (Downloading, Paused | Completed | Failed | Cancelled)
                | (Paused, Queued | Downloading | Cancelled)
                | (Failed, Queued | Cancelled)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadError {
    InvalidId,
    InvalidTransition {
        from: DownloadState,
        to: DownloadState,
    },
    ProgressExceedsTotal,
    ProgressRegressed,
}

impl fmt::Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => write!(f, "download ID must be nonzero"),
            Self::InvalidTransition { from, to } => {
                write!(f, "cannot transition from {from:?} to {to:?}")
            }
            Self::ProgressExceedsTotal => write!(f, "received bytes exceed known total"),
            Self::ProgressRegressed => write!(f, "received bytes cannot decrease"),
        }
    }
}

impl Error for DownloadError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Download {
    id: DownloadId,
    state: DownloadState,
    received_bytes: u64,
    total_bytes: Option<u64>,
}

impl Download {
    pub const fn new(id: DownloadId) -> Self {
        Self {
            id,
            state: DownloadState::Queued,
            received_bytes: 0,
            total_bytes: None,
        }
    }

    pub const fn id(&self) -> DownloadId {
        self.id
    }

    pub const fn state(&self) -> DownloadState {
        self.state
    }

    pub const fn received_bytes(&self) -> u64 {
        self.received_bytes
    }

    pub const fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    pub fn transition_to(&mut self, next: DownloadState) -> Result<(), DownloadError> {
        if !self.state.can_transition_to(next) {
            return Err(DownloadError::InvalidTransition {
                from: self.state,
                to: next,
            });
        }
        if next == DownloadState::Completed
            && self.total_bytes.is_some_and(|total| self.received_bytes != total)
        {
            return Err(DownloadError::ProgressExceedsTotal);
        }
        self.state = next;
        Ok(())
    }

    pub fn update_progress(
        &mut self,
        received_bytes: u64,
        total_bytes: Option<u64>,
    ) -> Result<(), DownloadError> {
        if self.state != DownloadState::Downloading {
            return Err(DownloadError::InvalidTransition {
                from: self.state,
                to: DownloadState::Downloading,
            });
        }
        if received_bytes < self.received_bytes {
            return Err(DownloadError::ProgressRegressed);
        }
        if total_bytes.is_some_and(|total| received_bytes > total) {
            return Err(DownloadError::ProgressExceedsTotal);
        }
        self.received_bytes = received_bytes;
        self.total_bytes = total_bytes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_zero_id() {
        assert_eq!(DownloadId::new(0), Err(DownloadError::InvalidId));
    }

    #[test]
    fn lifecycle_is_strict() {
        let mut item = Download::new(DownloadId::new(1).unwrap());
        assert!(item.transition_to(DownloadState::Completed).is_err());
        item.transition_to(DownloadState::Downloading).unwrap();
        item.update_progress(10, Some(10)).unwrap();
        item.transition_to(DownloadState::Completed).unwrap();
        assert!(item.transition_to(DownloadState::Downloading).is_err());
    }

    #[test]
    fn progress_must_be_monotonic_and_bounded() {
        let mut item = Download::new(DownloadId::new(2).unwrap());
        item.transition_to(DownloadState::Downloading).unwrap();
        item.update_progress(5, Some(10)).unwrap();
        assert_eq!(item.update_progress(4, Some(10)), Err(DownloadError::ProgressRegressed));
        assert_eq!(item.update_progress(11, Some(10)), Err(DownloadError::ProgressExceedsTotal));
        assert_eq!(item.received_bytes(), 5);
    }

    #[test]
    fn failed_download_may_retry() {
        let mut item = Download::new(DownloadId::new(3).unwrap());
        item.transition_to(DownloadState::Downloading).unwrap();
        item.transition_to(DownloadState::Failed).unwrap();
        item.transition_to(DownloadState::Queued).unwrap();
        assert_eq!(item.state(), DownloadState::Queued);
    }
}
