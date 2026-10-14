//! HTTP(S) capability probing. Files are not written by this crate yet.

use reqwest::header::{ACCEPT_RANGES, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE};
use reqwest::{Client, StatusCode, Url};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteMetadata {
    pub final_url: Url,
    pub content_length: Option<u64>,
    pub supports_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

#[derive(Debug)]
pub enum ProbeError {
    InvalidUrl(url::ParseError),
    UnsupportedScheme,
    Http(reqwest::Error),
    InvalidRangeResponse,
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUrl(error) => write!(f, "invalid URL: {error}"),
            Self::UnsupportedScheme => write!(f, "only HTTP and HTTPS are supported"),
            Self::Http(error) => write!(f, "HTTP request failed: {error}"),
            Self::InvalidRangeResponse => write!(f, "unexpected ranged response"),
        }
    }
}

impl std::error::Error for ProbeError {}

impl From<reqwest::Error> for ProbeError {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error)
    }
}

pub struct HttpClient {
    client: Client,
}

impl HttpClient {
    pub fn new() -> Result<Self, ProbeError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
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

    pub async fn probe(&self, input: &str) -> Result<RemoteMetadata, ProbeError> {
        let url = Url::parse(input).map_err(ProbeError::InvalidUrl)?;
        ensure_http(&url)?;
        let head = self.client.head(url.clone()).send().await?;
        ensure_http(head.url())?;
        if head.status().is_success() {
            let mut result = read_metadata(&head);
            if result.content_length == Some(0) {
                result.supports_ranges = false;
                return Ok(result);
            }
        }
        // A GET with bytes=0-0 provides evidence that the server actually supports ranges.
        let response = self.client.get(url).header(RANGE, "bytes=0-0").send().await?;
        ensure_http(response.url())?;
        if response.status() == StatusCode::PARTIAL_CONTENT {
            let content_range = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_first_byte_range)
                .ok_or(ProbeError::InvalidRangeResponse)?;
            let mut result = read_metadata(&response);
            result.content_length = content_range;
            result.supports_ranges = true;
            return Ok(result);
        }
        if !response.status().is_success() {
            response.error_for_status()?;
            unreachable!("successful status required");
        }
        let mut result = read_metadata(&response);
        result.supports_ranges = false;
        Ok(result)
    }
}

fn ensure_http(url: &Url) -> Result<(), ProbeError> {
    if matches!(url.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(ProbeError::UnsupportedScheme)
    }
}

fn read_metadata(response: &reqwest::Response) -> RemoteMetadata {
    let header = |key| {
        response
            .headers()
            .get(key)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    RemoteMetadata {
        final_url: response.url().clone(),
        content_length: response.content_length(),
        supports_ranges: response
            .headers()
            .get(ACCEPT_RANGES)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"bytes")),
        etag: header(ETAG),
        last_modified: header(LAST_MODIFIED),
    }
}

fn parse_first_byte_range(header: &str) -> Option<Option<u64>> {
    let size = header.strip_prefix("bytes 0-0/")?;
    if size == "*" {
        Some(None)
    } else {
        let size = size.parse::<u64>().ok()?;
        (size > 0).then_some(Some(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_range_size() {
        assert_eq!(parse_first_byte_range("bytes 0-0/100"), Some(Some(100)));
        assert_eq!(parse_first_byte_range("bytes 0-0/*"), Some(None));
        assert_eq!(parse_first_byte_range("bytes 1-1/100"), None);
        assert_eq!(parse_first_byte_range("bytes 0-0/0"), None);
    }

    #[tokio::test]
    async fn rejects_local_file_urls() {
        let client = HttpClient::new().unwrap();
        assert!(matches!(
            client.probe("file:///tmp/example").await,
            Err(ProbeError::UnsupportedScheme)
        ));
    }
}
