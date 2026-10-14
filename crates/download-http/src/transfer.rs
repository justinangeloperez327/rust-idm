//! Single-stream transfers, bounded by Reqwest's streamed chunks.

use reqwest::{Client, Url};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum TransferError {
    InvalidUrl,
    Network(reqwest::Error),
    Io(std::io::Error),
    Cancelled,
    DestinationExists,
    LengthMismatch { expected: u64, actual: u64 },
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TransferError {}

impl From<reqwest::Error> for TransferError {
    fn from(error: reqwest::Error) -> Self {
        Self::Network(error)
    }
}

impl From<std::io::Error> for TransferError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferEvent {
    Started { total: Option<u64> },
    Progress { received: u64, total: Option<u64> },
    Completed { bytes: u64 },
}

#[derive(Debug, Clone)]
pub struct TransferOutcome {
    pub bytes: u64,
    pub destination: PathBuf,
}

pub struct TransferEngine {
    client: Client,
}

impl TransferEngine {
    pub fn new() -> Result<Self, TransferError> {
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 10 {
                    attempt.error("too many redirects")
                } else if matches!(attempt.url().scheme(), "http" | "https") {
                    attempt.follow()
                } else {
                    attempt.stop()
                }
            }))
            .build()?;
        Ok(Self { client })
    }

    pub async fn download(
        &self,
        url: &str,
        destination: &Path,
        cancel: &CancellationToken,
        events: Option<&mpsc::UnboundedSender<TransferEvent>>,
    ) -> Result<TransferOutcome, TransferError> {
        let parsed = Url::parse(url).map_err(|_| TransferError::InvalidUrl)?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(TransferError::InvalidUrl);
        }
        if destination.as_os_str().is_empty() {
            return Err(TransferError::InvalidUrl);
        }
        if fs::try_exists(destination).await? {
            return Err(TransferError::DestinationExists);
        }
        let response = tokio::select! {
            _ = cancel.cancelled() => return Err(TransferError::Cancelled),
            result = self.client.get(parsed).send() => result?.error_for_status()?,
        };
        let total = response.content_length();
        if let Some(tx) = events {
            let _ = tx.send(TransferEvent::Started { total });
        }

        // create_new prevents multiple workers from sharing the same partial file.
        let partial = destination.with_extension(
            format!("{}.rust-idm-part", destination.extension().and_then(|x| x.to_str()).unwrap_or("download")),
        );
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial)
            .await?;
        let result = self.stream(response, &mut output, total, cancel, events).await;
        drop(output);
        match result {
            Ok(bytes) => {
                if fs::try_exists(destination).await? {
                    fs::remove_file(&partial).await?;
                    return Err(TransferError::DestinationExists);
                }
                // Note: some platforms can replace a destination created in the
                // narrow interval between the existence check and this rename.
                if let Err(error) = fs::rename(&partial, destination).await {
                    let _ = fs::remove_file(&partial).await;
                    return Err(TransferError::Io(error));
                }
                if let Some(tx) = events {
                    let _ = tx.send(TransferEvent::Completed { bytes });
                }
                Ok(TransferOutcome { bytes, destination: destination.to_path_buf() })
            }
            Err(error) => {
                let _ = fs::remove_file(&partial).await;
                Err(error)
            }
        }
    }

    async fn stream(
        &self,
        mut response: reqwest::Response,
        file: &mut tokio::fs::File,
        total: Option<u64>,
        cancel: &CancellationToken,
        events: Option<&mpsc::UnboundedSender<TransferEvent>>,
    ) -> Result<u64, TransferError> {
        let mut received = 0u64;
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => return Err(TransferError::Cancelled),
                chunk = response.chunk() => chunk?,
            };
            let Some(chunk) = next else { break };
            received = received.checked_add(chunk.len() as u64).ok_or_else(|| {
                TransferError::Io(std::io::Error::other("download size overflow"))
            })?;
            if total.is_some_and(|expected| received > expected) {
                return Err(TransferError::LengthMismatch { expected: total.unwrap_or(0), actual: received });
            }
            tokio::select! {
                _ = cancel.cancelled() => return Err(TransferError::Cancelled),
                result = file.write_all(&chunk) => result?,
            };
            if let Some(tx) = events {
                let _ = tx.send(TransferEvent::Progress { received, total });
            }
        }
        if total.is_some_and(|expected| expected != received) {
            return Err(TransferError::LengthMismatch { expected: total.unwrap_or(0), actual: received });
        }
        tokio::select! {
            _ = cancel.cancelled() => return Err(TransferError::Cancelled),
            result = file.sync_all() => result?,
        };
        Ok(received)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_file_scheme_without_network_io() {
        let engine = TransferEngine::new().unwrap();
        let token = CancellationToken::new();
        let result = engine.download("file:///test", Path::new("test.bin"), &token, None).await;
        assert!(matches!(result, Err(TransferError::InvalidUrl)));
    }

    #[tokio::test]
    async fn cancelled_before_request_never_creates_file() {
        let engine = TransferEngine::new().unwrap();
        let token = CancellationToken::new();
        token.cancel();
        let result = engine.download("http://127.0.0.1:9/test", Path::new("cancelled-test.bin"), &token, None).await;
        assert!(matches!(result, Err(TransferError::Cancelled)));
    }
}
