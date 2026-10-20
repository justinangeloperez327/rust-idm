//! Byte-accurate per-transfer token-bucket pacing.
use download_scheduler::bandwidth::TokenBucket;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

pub struct TransferThrottle {
    bucket: Option<TokenBucket>,
}

impl TransferThrottle {
    /// Zero or an absent limit disables throttling.
    pub fn new(bytes_per_second: Option<u64>) -> Self {
        Self {
            bucket: bytes_per_second.filter(|rate| *rate > 0)
                .map(|rate| TokenBucket::new(rate, Instant::now()).expect("positive rate")),
        }
    }

    pub fn from_environment() -> Self {
        let rate = std::env::var("RUST_IDM_BYTES_PER_SECOND")
            .ok().and_then(|value| value.parse::<u64>().ok());
        Self::new(rate)
    }

    pub async fn write(
        &mut self,
        file: &mut tokio::fs::File,
        bytes: &[u8],
        cancel: &CancellationToken,
    ) -> std::io::Result<()> {
        let mut remaining = bytes;
        while !remaining.is_empty() {
            if cancel.is_cancelled() {
                return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "transfer cancelled"));
            }
            let allowed = self.bucket.as_mut()
                .map_or(remaining.len() as u64, |bucket| bucket.take(remaining.len() as u64, Instant::now()));
            if allowed == 0 {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "transfer cancelled")),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
                continue;
            }
            let size = usize::try_from(allowed).unwrap_or(usize::MAX).min(remaining.len());
            tokio::select! {
                _ = cancel.cancelled() => return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "transfer cancelled")),
                result = file.write_all(&remaining[..size]) => result?,
            }
            remaining = &remaining[size..];
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn paced_write_preserves_bytes() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("paced.bin");
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        let mut throttle = TransferThrottle::new(Some(1024));
        throttle.write(&mut file, &vec![17; 1100], &CancellationToken::new()).await.unwrap();
        file.sync_all().await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), vec![17; 1100]);
    }
    #[tokio::test]
    async fn disabled_limit_writes_immediately() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("unlimited.bin");
        let mut file = tokio::fs::File::create(&path).await.unwrap();
        TransferThrottle::new(None).write(&mut file, b"hello", &CancellationToken::new()).await.unwrap();
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
    }
}
