//! Deterministic token-bucket bandwidth controller and fair active-job allocator.
//! This is an admission controller; network workers will integrate it in a later group.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BandwidthError {
    InvalidRate,
    DuplicateJob,
    MissingJob,
    InvalidRequest,
}

#[derive(Debug, Clone)]
pub struct TokenBucket {
    rate: u64,
    capacity: u64,
    tokens: f64,
    updated: Instant,
}

impl TokenBucket {
    /// Bytes/sec. Capacity is one second of bandwidth with a minimum of one byte.
    pub fn new(bytes_per_second: u64, now: Instant) -> Result<Self, BandwidthError> {
        if bytes_per_second == 0 {
            return Err(BandwidthError::InvalidRate);
        }
        Ok(Self {
            rate: bytes_per_second,
            capacity: bytes_per_second,
            tokens: bytes_per_second as f64,
            updated: now,
        })
    }

    fn replenish(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.capacity as f64);
        if now > self.updated {
            self.updated = now;
        }
    }

    pub fn available(&mut self, now: Instant) -> u64 {
        self.replenish(now);
        self.tokens.floor() as u64
    }

    pub fn take(&mut self, requested: u64, now: Instant) -> u64 {
        let allowance = requested.min(self.available(now));
        self.tokens -= allowance as f64;
        allowance
    }

    pub fn change_rate(&mut self, rate: u64, now: Instant) -> Result<(), BandwidthError> {
        if rate == 0 {
            return Err(BandwidthError::InvalidRate);
        }
        self.replenish(now);
        self.rate = rate;
        self.capacity = rate;
        self.tokens = self.tokens.min(rate as f64);
        Ok(())
    }
}

#[derive(Debug)]
pub struct BandwidthManager {
    global: Option<TokenBucket>,
    jobs: BTreeMap<u64, Option<TokenBucket>>,
    order: VecDeque<u64>,
}

impl BandwidthManager {
    pub fn new(global_bytes_per_second: Option<u64>, now: Instant) -> Result<Self, BandwidthError> {
        let global = global_bytes_per_second.map(|rate| TokenBucket::new(rate, now)).transpose()?;
        Ok(Self { global, jobs: BTreeMap::new(), order: VecDeque::new() })
    }

    pub fn register(&mut self, job: u64, limit: Option<u64>, now: Instant) -> Result<(), BandwidthError> {
        if job == 0 || self.jobs.contains_key(&job) {
            return Err(BandwidthError::DuplicateJob);
        }
        let bucket = limit.map(|rate| TokenBucket::new(rate, now)).transpose()?;
        self.jobs.insert(job, bucket);
        self.order.push_back(job);
        Ok(())
    }

    pub fn remove(&mut self, job: u64) -> Result<(), BandwidthError> {
        if self.jobs.remove(&job).is_none() {
            return Err(BandwidthError::MissingJob);
        }
        self.order.retain(|id| *id != job);
        Ok(())
    }

    pub fn set_global_limit(&mut self, rate: Option<u64>, now: Instant) -> Result<(), BandwidthError> {
        self.global = rate.map(|rate| TokenBucket::new(rate, now)).transpose()?;
        Ok(())
    }

    pub fn set_job_limit(&mut self, job: u64, rate: Option<u64>, now: Instant) -> Result<(), BandwidthError> {
        let bucket = rate.map(|rate| TokenBucket::new(rate, now)).transpose()?;
        *self.jobs.get_mut(&job).ok_or(BandwidthError::MissingJob)? = bucket;
        Ok(())
    }

    /// Select the next job in rotating order, then grant up to max_bytes.
    /// Callers must only request bytes immediately before performing I/O.
    /// Zero allowance indicates backpressure; retry when time advances.
    pub fn grant_next(&mut self, max_bytes: u64, now: Instant) -> Result<Option<(u64, u64)>, BandwidthError> {
        if max_bytes == 0 {
            return Err(BandwidthError::InvalidRequest);
        }
        for _ in 0..self.order.len() {
            let Some(job) = self.order.pop_front() else { break };
            self.order.push_back(job);
            let job_available = self.jobs.get_mut(&job).and_then(Option::as_mut)
                .map_or(u64::MAX, |bucket| bucket.available(now));
            let global_available = self.global.as_mut()
                .map_or(u64::MAX, |bucket| bucket.available(now));
            let grant = max_bytes.min(job_available).min(global_available);
            if grant == 0 { continue; }
            if let Some(bucket) = self.jobs.get_mut(&job).and_then(Option::as_mut) {
                bucket.take(grant, now);
            }
            if let Some(bucket) = self.global.as_mut() {
                bucket.take(grant, now);
            }
            return Ok(Some((job, grant)));
        }
        Ok(None)
    }

    pub fn active_jobs(&self) -> usize { self.jobs.len() }
}

/// Recommended minimum polling interval for a bounded async integration.
/// The scheduler itself never sleeps or blocks.
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_replenish_without_overflowing_capacity() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(100, now).unwrap();
        assert_eq!(bucket.take(100, now), 100);
        assert_eq!(bucket.take(100, now), 0);
        assert_eq!(bucket.take(80, now + Duration::from_millis(500)), 50);
        assert_eq!(bucket.available(now + Duration::from_secs(100)), 100);
    }

    #[test]
    fn round_robin_fairness() {
        let now = Instant::now();
        let mut manager = BandwidthManager::new(None, now).unwrap();
        manager.register(1, None, now).unwrap();
        manager.register(2, None, now).unwrap();
        assert_eq!(manager.grant_next(100, now).unwrap(), Some((1, 100)));
        assert_eq!(manager.grant_next(100, now).unwrap(), Some((2, 100)));
        assert_eq!(manager.grant_next(100, now).unwrap(), Some((1, 100)));
    }

    #[test]
    fn global_and_local_limits_apply_together() {
        let now = Instant::now();
        let mut manager = BandwidthManager::new(Some(100), now).unwrap();
        manager.register(1, Some(25), now).unwrap();
        manager.register(2, None, now).unwrap();
        assert_eq!(manager.grant_next(1000, now).unwrap(), Some((1, 25)));
        assert_eq!(manager.grant_next(1000, now).unwrap(), Some((2, 75)));
        assert_eq!(manager.grant_next(1000, now).unwrap(), None);
    }

    #[test]
    fn rejects_invalid_limits_and_duplicate_jobs() {
        let now = Instant::now();
        assert!(BandwidthManager::new(Some(0), now).is_err());
        let mut manager = BandwidthManager::new(None, now).unwrap();
        manager.register(1, None, now).unwrap();
        assert_eq!(manager.register(1, None, now), Err(BandwidthError::DuplicateJob));
        assert_eq!(manager.set_job_limit(1, Some(0), now), Err(BandwidthError::InvalidRate));
    }

    #[test]
    fn removal_preserves_other_jobs() {
        let now = Instant::now();
        let mut manager = BandwidthManager::new(None, now).unwrap();
        manager.register(1, None, now).unwrap();
        manager.register(2, None, now).unwrap();
        manager.remove(1).unwrap();
        assert_eq!(manager.grant_next(10, now).unwrap(), Some((2, 10)));
    }
}
