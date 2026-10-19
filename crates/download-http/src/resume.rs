//! Opt-in resumable single-stream HTTP downloads with a strong ETag.
//! Partial files are preserved only after validation; never append to unverified data.
use reqwest::header::{ACCEPT_ENCODING, CONTENT_RANGE, ETAG, IF_RANGE, RANGE};
use reqwest::{Client, StatusCode, Url};
use std::path::Path;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum ResumeError {
    InvalidUrl,
    MissingStrongValidator,
    InvalidRange,
    Cancelled,
    DestinationExists,
    Http(reqwest::Error),
    Io(std::io::Error),
}
impl std::fmt::Display for ResumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ResumeError {}
impl From<reqwest::Error> for ResumeError {
    fn from(value: reqwest::Error) -> Self { Self::Http(value) }
}
impl From<std::io::Error> for ResumeError {
    fn from(value: std::io::Error) -> Self { Self::Io(value) }
}

/// A checkpoint must be acquired from an earlier verified response for the
/// exact resource and stored alongside the partial file by the coordinator.
#[derive(Debug, Clone)]
pub struct ResumeIdentity {
    pub final_url: String,
    pub total_bytes: u64,
    pub strong_etag: String,
}

impl ResumeIdentity {
    pub fn validate(&self) -> Result<(), ResumeError> {
        if self.total_bytes == 0
            || !self.strong_etag.starts_with('"')
            || !self.strong_etag.ends_with('"')
            || self.strong_etag.contains(['\r', '\n'])
        { return Err(ResumeError::MissingStrongValidator); }
        let url = Url::parse(&self.final_url).map_err(|_| ResumeError::InvalidUrl)?;
        if !matches!(url.scheme(), "https" | "http") { return Err(ResumeError::InvalidUrl); }
        Ok(())
    }
}

pub struct ResumeEngine {
    client: Client,
}

impl ResumeEngine {
    pub fn new() -> Result<Self, ResumeError> {
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Self { client })
    }

    /// Resumes an existing partial file. On cancellation, syncs and preserves
    /// its bytes. The caller owns the accompanying checkpoint lifecycle.
    /// Finalization uses hard links to avoid replacing existing output files.
    pub async fn resume(
        &self,
        identity: &ResumeIdentity,
        partial: &Path,
        destination: &Path,
        cancel: &CancellationToken,
    ) -> Result<u64, ResumeError> {
        identity.validate()?;
        if cancel.is_cancelled() { return Err(ResumeError::Cancelled); }
        if fs::try_exists(destination).await? { return Err(ResumeError::DestinationExists); }
        let length = fs::metadata(partial).await?.len();
        if length > identity.total_bytes { return Err(ResumeError::InvalidRange); }
        if length == identity.total_bytes {
            // Cannot prove the complete object's correctness from file length
            // alone. Never publish without a verified file hash.
            return Err(ResumeError::InvalidRange);
        }
        let url = Url::parse(&identity.final_url).map_err(|_| ResumeError::InvalidUrl)?;
        let mut response = tokio::select! {
            _ = cancel.cancelled() => return Err(ResumeError::Cancelled),
            response = self.client.get(url)
                .header(RANGE, format!("bytes={length}-"))
                .header(IF_RANGE, &identity.strong_etag)
                .header(ACCEPT_ENCODING, "identity")
                .send() => response?,
        };
        let expected_range = format!("bytes {}-{}/{}", length, identity.total_bytes - 1, identity.total_bytes);
        if response.status() != StatusCode::PARTIAL_CONTENT
            || response.headers().get(CONTENT_RANGE).and_then(|h| h.to_str().ok()) != Some(expected_range.as_str())
            || response.headers().get(ETAG).and_then(|h| h.to_str().ok()) != Some(identity.strong_etag.as_str())
            || response.headers().get(reqwest::header::CONTENT_ENCODING).is_some_and(
                |v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
        { return Err(ResumeError::InvalidRange); }
        let mut file = OpenOptions::new().append(true).open(partial).await?;
        // A separate coordinator must serialize access to this partial path.
        if file.metadata().await?.len() != length { return Err(ResumeError::InvalidRange); }
        let mut received = length;
        loop {
            let chunk = tokio::select! {
                _ = cancel.cancelled() => {
                    file.sync_all().await?;
                    return Err(ResumeError::Cancelled);
                }
                result = response.chunk() => result?,
            };
            let Some(chunk) = chunk else { break };
            received = received.checked_add(chunk.len() as u64).ok_or(ResumeError::InvalidRange)?;
            if received > identity.total_bytes { return Err(ResumeError::InvalidRange); }
            file.write_all(&chunk).await?;
        }
        file.sync_all().await?;
        drop(file);
        if received != identity.total_bytes { return Err(ResumeError::InvalidRange); }
        if cancel.is_cancelled() { return Err(ResumeError::Cancelled); }
        match fs::hard_link(partial, destination).await {
            Ok(()) => { fs::remove_file(partial).await?; Ok(received) }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(ResumeError::DestinationExists),
            Err(e) => Err(ResumeError::Io(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_weak_and_invalid_validators() {
        let mut identity = ResumeIdentity {
            final_url: "https://example.org/file".into(),
            total_bytes: 42,
            strong_etag: "W/\"weak\"".into(),
        };
        assert!(identity.validate().is_err());
        identity.strong_etag = "\"strong\"".into();
        assert!(identity.validate().is_ok());
    }
    #[tokio::test]
    async fn rejects_oversized_partial_without_network() {
        let dir = tempfile::tempdir().unwrap();
        let partial = dir.path().join("part");
        fs::write(&partial, b"too many bytes").await.unwrap();
        let engine = ResumeEngine::new().unwrap();
        let identity = ResumeIdentity {
            final_url: "http://127.0.0.1:1/file".into(),
            total_bytes: 4,
            strong_etag: "\"etag\"".into(),
        };
        assert!(matches!(
            engine.resume(&identity, &partial, &dir.path().join("final"), &CancellationToken::new()).await,
            Err(ResumeError::InvalidRange)
        ));
    }
}
