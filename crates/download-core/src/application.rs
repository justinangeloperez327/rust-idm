//! Synchronous domain application service. The async engine will consume these events.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::{Download, DownloadError, DownloadId, DownloadState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadRequest {
    pub url: String,
    pub destination: PathBuf,
}

impl DownloadRequest {
    pub fn new(url: impl Into<String>, destination: PathBuf) -> Result<Self, ServiceError> {
        let url = url.into();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(ServiceError::InvalidUrl);
        }
        if destination.as_os_str().is_empty() {
            return Err(ServiceError::InvalidDestination);
        }
        Ok(Self { url, destination })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadCommand {
    Enqueue(DownloadRequest),
    Start(DownloadId),
    Pause(DownloadId),
    Retry(DownloadId),
    Cancel(DownloadId),
    ReportProgress {
        id: DownloadId,
        received_bytes: u64,
        total_bytes: Option<u64>,
    },
    Finish(DownloadId),
    Fail(DownloadId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadEvent {
    Queued { id: DownloadId },
    StateChanged {
        id: DownloadId,
        from: DownloadState,
        to: DownloadState,
    },
    ProgressChanged {
        id: DownloadId,
        received_bytes: u64,
        total_bytes: Option<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceError {
    InvalidUrl,
    InvalidDestination,
    IdExhausted,
    MissingDownload(DownloadId),
    Domain(DownloadError),
}

impl From<DownloadError> for ServiceError {
    fn from(error: DownloadError) -> Self {
        Self::Domain(error)
    }
}

#[derive(Debug, Clone)]
pub struct DownloadEntry {
    pub request: DownloadRequest,
    pub download: Download,
}

#[derive(Debug, Default)]
pub struct DownloadService {
    next_id: u64,
    entries: HashMap<DownloadId, DownloadEntry>,
}

impl DownloadService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, id: DownloadId) -> Option<&DownloadEntry> {
        self.entries.get(&id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn execute(&mut self, command: DownloadCommand) -> Result<DownloadEvent, ServiceError> {
        match command {
            DownloadCommand::Enqueue(request) => {
                let next = self.next_id.checked_add(1).ok_or(ServiceError::IdExhausted)?;
                let id = DownloadId::new(next)?;
                self.entries.insert(
                    id,
                    DownloadEntry {
                        request,
                        download: Download::new(id),
                    },
                );
                self.next_id = next;
                Ok(DownloadEvent::Queued { id })
            }
            DownloadCommand::Start(id) => self.change_state(id, DownloadState::Downloading),
            DownloadCommand::Pause(id) => self.change_state(id, DownloadState::Paused),
            DownloadCommand::Retry(id) => self.change_state(id, DownloadState::Queued),
            DownloadCommand::Cancel(id) => self.change_state(id, DownloadState::Cancelled),
            DownloadCommand::Finish(id) => self.change_state(id, DownloadState::Completed),
            DownloadCommand::Fail(id) => self.change_state(id, DownloadState::Failed),
            DownloadCommand::ReportProgress {
                id,
                received_bytes,
                total_bytes,
            } => {
                let entry = self.entries.get_mut(&id).ok_or(ServiceError::MissingDownload(id))?;
                entry.download.update_progress(received_bytes, total_bytes)?;
                Ok(DownloadEvent::ProgressChanged {
                    id,
                    received_bytes,
                    total_bytes,
                })
            }
        }
    }

    fn change_state(
        &mut self,
        id: DownloadId,
        to: DownloadState,
    ) -> Result<DownloadEvent, ServiceError> {
        let entry = self.entries.get_mut(&id).ok_or(ServiceError::MissingDownload(id))?;
        let from = entry.download.state();
        entry.download.transition_to(to)?;
        Ok(DownloadEvent::StateChanged { id, from, to })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> DownloadRequest {
        DownloadRequest::new(
            "https://example.org/archive.zip",
            PathBuf::from("archive.zip"),
        )
        .unwrap()
    }

    #[test]
    fn enqueue_allocates_unique_ids() {
        let mut service = DownloadService::new();
        let first = service.execute(DownloadCommand::Enqueue(request())).unwrap();
        let second = service.execute(DownloadCommand::Enqueue(request())).unwrap();
        assert_ne!(first, second);
        assert_eq!(service.len(), 2);
    }

    #[test]
    fn invalid_request_is_rejected() {
        assert_eq!(
            DownloadRequest::new("file:///private.txt", PathBuf::from("out")),
            Err(ServiceError::InvalidUrl)
        );
        assert_eq!(
            DownloadRequest::new("https://example.org", PathBuf::new()),
            Err(ServiceError::InvalidDestination)
        );
    }

    #[test]
    fn commands_emit_events_and_preserve_state() {
        let mut service = DownloadService::new();
        let DownloadEvent::Queued { id } = service.execute(DownloadCommand::Enqueue(request())).unwrap() else {
            panic!("expected queued event");
        };
        assert_eq!(
            service.execute(DownloadCommand::Start(id)).unwrap(),
            DownloadEvent::StateChanged {
                id,
                from: DownloadState::Queued,
                to: DownloadState::Downloading,
            }
        );
        service.execute(DownloadCommand::ReportProgress {
            id,
            received_bytes: 20,
            total_bytes: Some(20),
        }).unwrap();
        service.execute(DownloadCommand::Finish(id)).unwrap();
        assert_eq!(service.get(id).unwrap().download.state(), DownloadState::Completed);
        assert!(service.execute(DownloadCommand::Start(id)).is_err());
    }

    #[test]
    fn missing_download_is_reported() {
        let mut service = DownloadService::new();
        let id = DownloadId::new(42).unwrap();
        assert_eq!(
            service.execute(DownloadCommand::Pause(id)),
            Err(ServiceError::MissingDownload(id))
        );
    }

    #[test]
    fn failed_command_does_not_mutate_state() {
        let mut service = DownloadService::new();
        let DownloadEvent::Queued { id } = service.execute(DownloadCommand::Enqueue(request())).unwrap() else {
            panic!("expected queued event");
        };
        assert!(service.execute(DownloadCommand::Finish(id)).is_err());
        assert_eq!(service.get(id).unwrap().download.state(), DownloadState::Queued);
    }
}
