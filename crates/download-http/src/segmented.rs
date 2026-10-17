//! Concurrent HTTP range downloader using bounded in-memory chunks and segment files.
//! This Group 5 implementation deliberately does not persist checkpoints.

use reqwest::header::{CONTENT_ENCODING, CONTENT_RANGE, ETAG, IF_RANGE, RANGE};
use reqwest::{Client, StatusCode, Url};
use std::path::Path;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum SegmentedError {
    InvalidInput,
    InvalidResponse,
    Network(reqwest::Error),
    Io(std::io::Error),
    Cancelled,
    Worker(tokio::task::JoinError),
    DestinationExists,
}

impl std::fmt::Display for SegmentedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for SegmentedError {}
impl From<reqwest::Error> for SegmentedError {
    fn from(value: reqwest::Error) -> Self { Self::Network(value) }
}
impl From<std::io::Error> for SegmentedError {
    fn from(value: std::io::Error) -> Self { Self::Io(value) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn len(self) -> u64 { self.end - self.start + 1 }
    pub fn is_empty(self) -> bool { false }
}

pub fn partition_ranges(total: u64, count: usize) -> Result<Vec<ByteRange>, SegmentedError> {
    if total == 0 || count == 0 || count > 16 { return Err(SegmentedError::InvalidInput); }
    let count = std::cmp::min(count as u64, total);
    let base = total / count;
    let remainder = total % count;
    let mut start = 0;
    let mut ranges = Vec::with_capacity(count as usize);
    for index in 0..count {
        let length = base + u64::from(index < remainder);
        let end = start + length - 1;
        ranges.push(ByteRange { start, end });
        start = end + 1;
    }
    Ok(ranges)
}

/// Strictly parses Content-Range for a single, fully known range.
pub fn validate_content_range(header: &str, expected: ByteRange, total: u64) -> bool {
    let Some(rest) = header.strip_prefix("bytes ") else { return false };
    let Some((range, size)) = rest.split_once('/') else { return false };
    let Some((start, end)) = range.split_once('-') else { return false };
    matches!(
        (start.parse::<u64>(), end.parse::<u64>(), size.parse::<u64>()),
        (Ok(start), Ok(end), Ok(size))
            if start == expected.start && end == expected.end && size == total
    )
}

pub struct SegmentedDownloader {
    client: Client,
}

impl SegmentedDownloader {
    pub fn new() -> Result<Self, SegmentedError> {
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

    /// Downloads into a dedicated temporary directory, then publishes the assembled file.
    /// Caller must supply a trusted size and, if available, a strong ETag from a probe.
    pub async fn download(
        &self,
        url: &str,
        destination: &Path,
        total: u64,
        segments: usize,
        etag: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<u64, SegmentedError> {
        let parsed = Url::parse(url).map_err(|_| SegmentedError::InvalidInput)?;
        if !matches!(parsed.scheme(), "http" | "https")
            || destination.as_os_str().is_empty()
            || total == 0
            || etag.is_some_and(|tag| tag.starts_with("W/") || tag.contains('\r') || tag.contains('\n'))
        {
            return Err(SegmentedError::InvalidInput);
        }
        if cancel.is_cancelled() { return Err(SegmentedError::Cancelled); }
        if fs::try_exists(destination).await? { return Err(SegmentedError::DestinationExists); }
        let ranges = partition_ranges(total, segments)?;
        let parts_dir = destination.with_extension("rust-idm-parts");
        fs::create_dir(&parts_dir).await?;
        let result = self.download_inner(
            parsed, destination, &parts_dir, total, &ranges, etag, cancel
        ).await;
        let _ = fs::remove_dir_all(&parts_dir).await;
        result
    }

    #[allow(clippy::too_many_arguments)] // download context retained until Group 6 integration refactor
    async fn download_inner(
        &self,
        url: Url,
        destination: &Path,
        parts_dir: &Path,
        total: u64,
        ranges: &[ByteRange],
        etag: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<u64, SegmentedError> {
        let mut workers = JoinSet::new();
        for (index, range) in ranges.iter().copied().enumerate() {
            let client = self.client.clone();
            let url = url.clone();
            let file_path = parts_dir.join(format!("{index:04}.part"));
            let etag = etag.map(str::to_owned);
            let token = cancel.child_token();
            workers.spawn(async move {
                let result = fetch_range(client, url, range, total, etag.as_deref(), &file_path, &token).await;
                (index, result)
            });
        }
        let mut completed = vec![false; ranges.len()];
        while !workers.is_empty() {
            let outcome = tokio::select! {
                _ = cancel.cancelled() => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    return Err(SegmentedError::Cancelled);
                },
                outcome = workers.join_next() => outcome,
            };
            if let Some(outcome) = outcome {
                match outcome {
                    Ok((index, Ok(()))) => completed[index] = true,
                    Ok((_, Err(error))) => {
                        workers.abort_all();
                        while workers.join_next().await.is_some() {}
                        return Err(error);
                    }
                    Err(error) => {
                        workers.abort_all();
                        while workers.join_next().await.is_some() {}
                        return Err(SegmentedError::Worker(error));
                    }
                }
            }
        }
        if completed.iter().any(|&value| !value) { return Err(SegmentedError::InvalidResponse); }
        let assembly = parts_dir.join("assembled.tmp");
        let mut output = OpenOptions::new().write(true).create_new(true).open(&assembly).await?;
        let mut copied = 0;
        for index in 0..ranges.len() {
            if cancel.is_cancelled() { return Err(SegmentedError::Cancelled); }
            let mut part = fs::File::open(parts_dir.join(format!("{index:04}.part"))).await?;
            copied += tokio::io::copy(&mut part, &mut output).await?;
        }
        if copied != total { return Err(SegmentedError::InvalidResponse); }
        output.sync_all().await?;
        drop(output);
        if cancel.is_cancelled() { return Err(SegmentedError::Cancelled); }
        if fs::try_exists(destination).await? { return Err(SegmentedError::DestinationExists); }
        // A no-replace publish operation is still required for production safety.
        fs::rename(&assembly, destination).await?;
        Ok(copied)
    }
}

async fn fetch_range(
    client: Client,
    url: Url,
    range: ByteRange,
    total: u64,
    etag: Option<&str>,
    path: &Path,
    cancel: &CancellationToken,
) -> Result<(), SegmentedError> {
    let mut request = client
        .get(url)
        .header(RANGE, format!("bytes={}-{}", range.start, range.end))
        .header(reqwest::header::ACCEPT_ENCODING, "identity");
    if let Some(etag) = etag { request = request.header(IF_RANGE, etag); }
    let mut response = tokio::select! {
        _ = cancel.cancelled() => return Err(SegmentedError::Cancelled),
        result = request.send() => result?,
    };
    if response.status() != StatusCode::PARTIAL_CONTENT
        || response.headers().get(CONTENT_RANGE).and_then(|v| v.to_str().ok())
            .is_none_or(|value| !validate_content_range(value, range, total))
        || response.headers().get(CONTENT_ENCODING)
            .is_some_and(|encoding| !encoding.as_bytes().eq_ignore_ascii_case(b"identity"))
        || etag.is_some_and(|expected| {
            response.headers().get(ETAG).and_then(|v| v.to_str().ok())
                .is_some_and(|actual| actual != expected)
        })
    {
        return Err(SegmentedError::InvalidResponse);
    }
    let mut output = OpenOptions::new().write(true).create_new(true).open(path).await?;
    let mut received = 0u64;
    loop {
        let next = tokio::select! {
            _ = cancel.cancelled() => return Err(SegmentedError::Cancelled),
            result = response.chunk() => result?,
        };
        let Some(chunk) = next else { break };
        received = received.checked_add(chunk.len() as u64).ok_or(SegmentedError::InvalidResponse)?;
        if received > range.len() { return Err(SegmentedError::InvalidResponse); }
        tokio::select! {
            _ = cancel.cancelled() => return Err(SegmentedError::Cancelled),
            result = output.write_all(&chunk) => result?,
        };
    }
    if received != range.len() { return Err(SegmentedError::InvalidResponse); }
    output.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partitions_without_gaps_or_overlaps() {
        for size in 1..128 {
            for count in 1..=16 {
                let ranges = partition_ranges(size, count).unwrap();
                assert_eq!(ranges.first().unwrap().start, 0);
                assert_eq!(ranges.last().unwrap().end, size - 1);
                assert_eq!(ranges.iter().map(|r| r.len()).sum::<u64>(), size);
                for pair in ranges.windows(2) {
                    assert_eq!(pair[0].end + 1, pair[1].start);
                }
            }
        }
    }

    #[test]
    fn rejects_unsupported_partition_sizes() {
        assert!(partition_ranges(0, 4).is_err());
        assert!(partition_ranges(100, 0).is_err());
        assert!(partition_ranges(100, 17).is_err());
    }

    #[test]
    fn rejects_mismatched_content_range() {
        let range = ByteRange { start: 0, end: 49 };
        assert!(validate_content_range("bytes 0-49/100", range, 100));
        for invalid in ["bytes 0-50/100", "bytes 1-49/100", "bytes 0-49/*", "bytes 0-49/101", "bytes */100"] {
            assert!(!validate_content_range(invalid, range, 100));
        }
    }
}
