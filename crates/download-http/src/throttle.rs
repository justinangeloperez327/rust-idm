//! Per-transfer and shared process-wide disk-write pacing.
//! Shared allocation is synchronized across separate transfer runtime threads.
use download_scheduler::bandwidth::TokenBucket;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

pub struct TransferThrottle {
    local: Option<TokenBucket>,
    global: Option<Arc<Mutex<Option<TokenBucket>>>>,
}
static GLOBAL_BUCKET: OnceLock<Arc<Mutex<Option<TokenBucket>>>> = OnceLock::new();

fn global_bucket() -> Arc<Mutex<Option<TokenBucket>>> {
    GLOBAL_BUCKET.get_or_init(|| {
        let rate = std::env::var("RUST_IDM_GLOBAL_BYTES_PER_SECOND")
            .ok().and_then(|value| value.parse::<u64>().ok()).filter(|value| *value > 0);
        Arc::new(Mutex::new(rate.map(|rate| TokenBucket::new(rate, Instant::now())
            .expect("positive rate"))))
    }).clone()
}

/// Applies a global write-rate limit immediately to all active workers.
/// None disables the shared limit.
pub fn configure_global_limit(rate: Option<u64>) -> std::io::Result<()> {
    let next = rate.filter(|value| *value > 0)
        .map(|rate| TokenBucket::new(rate, Instant::now()).expect("positive rate"));
    let global = global_bucket();
    let mut guard = global.lock().map_err(|_| std::io::Error::other("bandwidth lock poisoned"))?;
    *guard = next;
    Ok(())
}

fn cancelled() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "transfer cancelled")
}

impl TransferThrottle {
    pub fn new(bytes_per_second: Option<u64>) -> Self {
        Self::with_shared(bytes_per_second, None)
    }

    pub fn with_shared(
        local_rate: Option<u64>,
        shared: Option<Arc<Mutex<Option<TokenBucket>>>>,
    ) -> Self {
        Self {
            local: local_rate.filter(|rate| *rate > 0)
                .map(|rate| TokenBucket::new(rate, Instant::now()).expect("positive rate")),
            global: shared,
        }
    }

    /// RUST_IDM_BYTES_PER_SECOND is per transfer.
    /// RUST_IDM_GLOBAL_BYTES_PER_SECOND is a single process-wide budget,
    /// initialized on first access and shared across transfer worker threads.
    pub fn from_environment() -> Self {
        let local = std::env::var("RUST_IDM_BYTES_PER_SECOND")
            .ok().and_then(|value| value.parse::<u64>().ok());
        let global = Some(global_bucket());
        Self::with_shared(local, global)
    }

    pub async fn write(
        &mut self,
        file: &mut tokio::fs::File,
        bytes: &[u8],
        cancel: &CancellationToken,
    ) -> std::io::Result<()> {
        let mut remaining = bytes;
        while !remaining.is_empty() {
            if cancel.is_cancelled() { return Err(cancelled()); }
            let now = Instant::now();
            let locally_available = self.local.as_mut()
                .map_or(remaining.len() as u64, |bucket| bucket.available(now));
            let requested = locally_available.min(remaining.len() as u64).min(64 * 1024);
            let granted = if let Some(shared) = &self.global {
                let mut bucket = shared.lock().map_err(|_| std::io::Error::other("bandwidth lock poisoned"))?;
                bucket.as_mut().map_or(requested, |active| active.take(requested, now))
            } else { requested };
            if granted == 0 {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(cancelled()),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {},
                }
                continue;
            }
            if let Some(local) = &mut self.local { local.take(granted, now); }
            let count = usize::try_from(granted).unwrap_or(usize::MAX).min(remaining.len());
            tokio::select! {
                _ = cancel.cancelled() => return Err(cancelled()),
                result = file.write_all(&remaining[..count]) => result?,
            }
            remaining = &remaining[count..];
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

    #[tokio::test]
    async fn shared_budget_is_consumed_by_both_transfers() {
        let shared = Arc::new(Mutex::new(Some(TokenBucket::new(100, Instant::now()).unwrap())));
        let mut a = TransferThrottle::with_shared(None, Some(shared.clone()));
        let mut b = TransferThrottle::with_shared(None, Some(shared.clone()));
        let dir = tempfile::tempdir().unwrap();
        let mut first = tokio::fs::File::create(dir.path().join("first")).await.unwrap();
        let mut second = tokio::fs::File::create(dir.path().join("second")).await.unwrap();
        let cancel = CancellationToken::new();
        a.write(&mut first, &[1; 70], &cancel).await.unwrap();
        let remaining = shared.lock().unwrap().as_mut().unwrap().available(Instant::now());
        assert!(remaining <= 30);
        // Both write callers use the same bucket; the second must await
        // capacity rather than receiving a separate initial burst.
        b.write(&mut second, &[2; 40], &cancel).await.unwrap();
        assert_eq!(tokio::fs::read(dir.path().join("first")).await.unwrap().len(), 70);
        assert_eq!(tokio::fs::read(dir.path().join("second")).await.unwrap().len(), 40);
    }
}
