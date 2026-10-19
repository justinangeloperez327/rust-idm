//! Restart-aware download coordinator with a sidecar strong-ETag identity.
//! Partial content is reusable only when the server agrees to a strict range request.
use crate::resume::{ResumeEngine, ResumeError, ResumeIdentity};
use crate::transfer::{TransferEvent, TransferOutcome};
use reqwest::header::{ACCEPT_ENCODING, CONTENT_ENCODING, ETAG};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum ManagedError {
    Http(reqwest::Error),
    Io(std::io::Error),
    Resume(ResumeError),
    InvalidResponse,
    Cancelled,
    DestinationExists,
}
impl std::fmt::Display for ManagedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{self:?}") }
}
impl std::error::Error for ManagedError {}
impl From<reqwest::Error> for ManagedError { fn from(e: reqwest::Error) -> Self { Self::Http(e) } }
impl From<std::io::Error> for ManagedError { fn from(e: std::io::Error) -> Self { Self::Io(e) } }
impl From<ResumeError> for ManagedError { fn from(e: ResumeError) -> Self { Self::Resume(e) } }

#[derive(Serialize, Deserialize)]
struct Sidecar {
    requested_url: String,
    final_url: String,
    total_bytes: u64,
    strong_etag: String,
}

fn paths(destination: &Path) -> (PathBuf, PathBuf) {
    (destination.with_extension("rust-idm-resume-part"),
     destination.with_extension("rust-idm-resume-json"))
}

pub struct ManagedDownloader { client: Client }
impl ManagedDownloader {
    pub fn new() -> Result<Self, ManagedError> {
        Ok(Self { client: Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(10))
            .build()? })
    }

    pub async fn download(
        &self, url: &str, destination: &Path, cancel: &CancellationToken,
        events: Option<&mpsc::UnboundedSender<TransferEvent>>,
    ) -> Result<TransferOutcome, ManagedError> {
        let parsed = Url::parse(url).map_err(|_| ManagedError::InvalidResponse)?;
        if !matches!(parsed.scheme(), "http" | "https") { return Err(ManagedError::InvalidResponse); }
        if fs::try_exists(destination).await? { return Err(ManagedError::DestinationExists); }
        let (partial, sidecar) = paths(destination);
        let have_part = fs::try_exists(&partial).await?;
        let have_sidecar = fs::try_exists(&sidecar).await?;
        if have_part != have_sidecar { return Err(ManagedError::InvalidResponse); }
        if have_part {
            let bytes = fs::read(&sidecar).await?;
            let saved: Sidecar = serde_json::from_slice(&bytes).map_err(|_| ManagedError::InvalidResponse)?;
            if saved.requested_url != url { return Err(ManagedError::InvalidResponse); }
            let identity = ResumeIdentity {
                final_url: saved.final_url, total_bytes: saved.total_bytes,
                strong_etag: saved.strong_etag,
            };
            identity.validate()?;
            let offset = fs::metadata(&partial).await?.len();
            if let Some(tx) = events {
                let _ = tx.send(TransferEvent::Started { total: Some(identity.total_bytes) });
                let _ = tx.send(TransferEvent::Progress { received: offset, total: Some(identity.total_bytes) });
            }
            let bytes = ResumeEngine::new()?.resume(&identity, &partial, destination, cancel).await?;
            fs::remove_file(&sidecar).await?;
            if let Some(tx) = events { let _ = tx.send(TransferEvent::Completed { bytes }); }
            return Ok(TransferOutcome { bytes, destination: destination.to_path_buf() });
        }

        if cancel.is_cancelled() { return Err(ManagedError::Cancelled); }
        let mut response = tokio::select! {
            _ = cancel.cancelled() => return Err(ManagedError::Cancelled),
            response = self.client.get(parsed).header(ACCEPT_ENCODING, "identity").send() =>
                response?.error_for_status()?,
        };
        if response.headers().get(CONTENT_ENCODING).is_some_and(
            |v| !v.as_bytes().eq_ignore_ascii_case(b"identity")) {
            return Err(ManagedError::InvalidResponse);
        }
        let total = response.content_length().filter(|n| *n > 0);
        let strong_etag = response.headers().get(ETAG).and_then(|v| v.to_str().ok())
            .filter(|s| s.starts_with('"') && s.ends_with('"') && !s.contains('\r') && !s.contains('\n'))
            .map(str::to_owned);
        if total.is_none() || strong_etag.is_none() {
            return Err(ManagedError::InvalidResponse);
        }
        let total = total.unwrap_or(0);
        let saved = Sidecar {
            requested_url: url.to_owned(), final_url: response.url().to_string(),
            total_bytes: total, strong_etag: strong_etag.unwrap_or_default(),
        };
        let bytes = serde_json::to_vec(&saved).map_err(|_| ManagedError::InvalidResponse)?;
        // A pre-existing sidecar must never be overwritten.
        let mut metadata = OpenOptions::new().write(true).create_new(true).open(&sidecar).await?;
        metadata.write_all(&bytes).await?;
        metadata.sync_all().await?;
        drop(metadata);
        let mut output = match OpenOptions::new().write(true).create_new(true).open(&partial).await {
            Ok(file) => file,
            Err(error) => { let _ = fs::remove_file(&sidecar).await; return Err(error.into()); }
        };
        if let Some(tx) = events { let _ = tx.send(TransferEvent::Started { total: Some(total) }); }
        let mut received = 0u64;
        loop {
            let chunk = tokio::select! {
                _ = cancel.cancelled() => {
                    output.sync_all().await?;
                    return Err(ManagedError::Cancelled);
                },
                result = response.chunk() => result?,
            };
            let Some(chunk) = chunk else { break };
            received = received.checked_add(chunk.len() as u64).ok_or(ManagedError::InvalidResponse)?;
            if received > total { return Err(ManagedError::InvalidResponse); }
            output.write_all(&chunk).await?;
            if let Some(tx) = events { let _ = tx.send(TransferEvent::Progress { received, total: Some(total) }); }
        }
        output.sync_all().await?;
        drop(output);
        if received != total { return Err(ManagedError::InvalidResponse); }
        if cancel.is_cancelled() { return Err(ManagedError::Cancelled); }
        match fs::hard_link(&partial, destination).await {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Err(ManagedError::DestinationExists),
            Err(e) => return Err(e.into()),
        }
        fs::remove_file(&partial).await?;
        fs::remove_file(&sidecar).await?;
        if let Some(tx) = events { let _ = tx.send(TransferEvent::Completed { bytes: received }); }
        Ok(TransferOutcome { bytes: received, destination: destination.to_path_buf() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn orphan_partial_is_not_trusted() {
        let directory = tempfile::tempdir().unwrap();
        let destination = directory.path().join("file");
        let (partial, _) = paths(&destination);
        fs::write(&partial, b"not verified").await.unwrap();
        let result = ManagedDownloader::new().unwrap().download(
            "http://127.0.0.1:1/file", &destination, &CancellationToken::new(), None).await;
        assert!(matches!(result, Err(ManagedError::InvalidResponse)));
    }
}
