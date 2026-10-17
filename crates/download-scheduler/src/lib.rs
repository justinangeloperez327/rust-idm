//! Deterministic priority scheduler. Network workers are integrated later.
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low,
    Normal,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    InvalidLimit,
    DuplicateJob,
    MissingJob,
    NotRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Running,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Pending {
    id: JobId,
    priority: Priority,
    sequence: u64,
}

pub struct Scheduler {
    limit: usize,
    next_sequence: u64,
    pending: VecDeque<Pending>,
    running: HashSet<JobId>,
    priorities: HashMap<JobId, Priority>,
}

impl Scheduler {
    pub fn new(limit: usize) -> Result<Self, SchedulerError> {
        if !(1..=64).contains(&limit) {
            return Err(SchedulerError::InvalidLimit);
        }
        Ok(Self {
            limit,
            next_sequence: 0,
            pending: VecDeque::new(),
            running: HashSet::new(),
            priorities: HashMap::new(),
        })
    }

    pub fn enqueue(&mut self, id: JobId, priority: Priority) -> Result<(), SchedulerError> {
        if id.0 == 0 || self.priorities.contains_key(&id) {
            return Err(SchedulerError::DuplicateJob);
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.checked_add(1).ok_or(SchedulerError::InvalidLimit)?;
        self.priorities.insert(id, priority);
        self.pending.push_back(Pending { id, priority, sequence });
        Ok(())
    }

    pub fn set_priority(&mut self, id: JobId, priority: Priority) -> Result<(), SchedulerError> {
        let current = self.priorities.get_mut(&id).ok_or(SchedulerError::MissingJob)?;
        *current = priority;
        for item in &mut self.pending {
            if item.id == id {
                item.priority = priority;
            }
        }
        Ok(())
    }

    pub fn set_limit(&mut self, limit: usize) -> Result<(), SchedulerError> {
        if !(1..=64).contains(&limit) {
            return Err(SchedulerError::InvalidLimit);
        }
        self.limit = limit;
        Ok(())
    }

    pub fn dispatch_ready(&mut self) -> Vec<JobId> {
        let mut result = Vec::new();
        while self.running.len() < self.limit && !self.pending.is_empty() {
            let selected = self.pending.iter().enumerate().max_by_key(|(_, pending)| {
                (pending.priority, std::cmp::Reverse(pending.sequence))
            }).map(|(index, _)| index);
            if let Some(index) = selected {
                if let Some(item) = self.pending.remove(index) {
                    self.running.insert(item.id);
                    result.push(item.id);
                }
            }
        }
        result
    }

    pub fn complete(&mut self, id: JobId) -> Result<(), SchedulerError> {
        if !self.running.remove(&id) {
            return Err(SchedulerError::NotRunning);
        }
        self.priorities.remove(&id);
        Ok(())
    }

    pub fn cancel_queued(&mut self, id: JobId) -> Result<(), SchedulerError> {
        let position = self.pending.iter().position(|item| item.id == id).ok_or(SchedulerError::MissingJob)?;
        self.pending.remove(position);
        self.priorities.remove(&id);
        Ok(())
    }

    pub fn running_count(&self) -> usize { self.running.len() }
    pub fn queued_count(&self) -> usize { self.pending.len() }
    pub fn state(&self, id: JobId) -> Option<JobState> {
        if self.running.contains(&id) { Some(JobState::Running) }
        else if self.priorities.contains_key(&id) { Some(JobState::Queued) }
        else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn higher_priority_first_fifo_within_priority() {
        let mut scheduler = Scheduler::new(2).unwrap();
        scheduler.enqueue(JobId(1), Priority::Normal).unwrap();
        scheduler.enqueue(JobId(2), Priority::High).unwrap();
        scheduler.enqueue(JobId(3), Priority::High).unwrap();
        assert_eq!(scheduler.dispatch_ready(), vec![JobId(2), JobId(3)]);
        assert!(scheduler.dispatch_ready().is_empty());
        scheduler.complete(JobId(2)).unwrap();
        assert_eq!(scheduler.dispatch_ready(), vec![JobId(1)]);
    }

    #[test]
    fn reprioritization_and_cancellation() {
        let mut scheduler = Scheduler::new(1).unwrap();
        scheduler.enqueue(JobId(1), Priority::Normal).unwrap();
        scheduler.enqueue(JobId(2), Priority::Low).unwrap();
        scheduler.set_priority(JobId(2), Priority::High).unwrap();
        assert_eq!(scheduler.dispatch_ready(), vec![JobId(2)]);
        assert_eq!(scheduler.state(JobId(1)), Some(JobState::Queued));
        scheduler.cancel_queued(JobId(1)).unwrap();
        assert_eq!(scheduler.queued_count(), 0);
    }

    #[test]
    fn limit_changes_do_not_interrupt_workers() {
        let mut scheduler = Scheduler::new(2).unwrap();
        scheduler.enqueue(JobId(1), Priority::Normal).unwrap();
        scheduler.enqueue(JobId(2), Priority::Normal).unwrap();
        scheduler.enqueue(JobId(3), Priority::Normal).unwrap();
        assert_eq!(scheduler.dispatch_ready().len(), 2);
        scheduler.set_limit(1).unwrap();
        assert!(scheduler.dispatch_ready().is_empty());
        scheduler.complete(JobId(1)).unwrap();
        assert!(scheduler.dispatch_ready().is_empty());
        scheduler.complete(JobId(2)).unwrap();
        assert_eq!(scheduler.dispatch_ready(), vec![JobId(3)]);
    }

    #[test]
    fn duplicate_and_invalid_limits_rejected() {
        assert!(Scheduler::new(0).is_err());
        let mut scheduler = Scheduler::new(1).unwrap();
        scheduler.enqueue(JobId(1), Priority::Normal).unwrap();
        assert_eq!(scheduler.enqueue(JobId(1), Priority::Normal), Err(SchedulerError::DuplicateJob));
    }
}
