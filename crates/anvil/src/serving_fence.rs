//! Local validation for the cluster-wide serving fence.
//!
//! Grant issuance and renewal are peer-protocol concerns. This module only
//! captures the request start time on the receiving node and decides whether a
//! returned grant is safe to use locally.

use std::time::Duration;

use anvil_consensus::ClusterId;
use thiserror::Error;

pub(crate) const SERVING_LEASE_MAX_LIFETIME: Duration = Duration::from_secs(2);
pub(crate) const SERVING_LEASE_RENEWAL_CADENCE: Duration = Duration::from_millis(500);
pub(crate) const SERVING_MEMBERSHIP_CUTOVER_WAIT: Duration = Duration::from_secs(3);

/// The Raft state to which one serving grant is bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ServingFenceIdentity {
    pub(crate) cluster_id: ClusterId,
    pub(crate) raft_term: u64,
    pub(crate) active_membership_log_index: u64,
}

/// The bounded information returned by a future grant RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ServingGrant {
    pub(crate) identity: ServingFenceIdentity,
    pub(crate) lifetime: Duration,
}

/// A monotonic instant measured from Linux boot, including suspended time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct BootInstant(Duration);

impl BootInstant {
    fn checked_add(self, duration: Duration) -> Option<Self> {
        self.0.checked_add(duration).map(Self)
    }
}

/// A clock suitable for excluding a stale server after host suspension.
pub(crate) trait BootClock: Clone + Send + Sync + 'static {
    fn now(&self) -> Result<BootInstant, BootClockError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LinuxBootClock;

impl BootClock for LinuxBootClock {
    fn now(&self) -> Result<BootInstant, BootClockError> {
        linux_boot_time()
    }
}

#[cfg(target_os = "linux")]
fn linux_boot_time() -> Result<BootInstant, BootClockError> {
    let mut reading = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: `reading` points to writable storage for one `timespec`, and
    // CLOCK_BOOTTIME does not retain the pointer after this call.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, reading.as_mut_ptr()) };
    if result != 0 {
        return Err(BootClockError::System(
            std::io::Error::last_os_error().raw_os_error().unwrap_or(-1),
        ));
    }

    // SAFETY: a successful clock_gettime call initialized the entire value.
    let reading = unsafe { reading.assume_init() };
    let seconds = u64::try_from(reading.tv_sec).map_err(|_| BootClockError::InvalidReading)?;
    let nanoseconds = u32::try_from(reading.tv_nsec)
        .ok()
        .filter(|value| *value < 1_000_000_000)
        .ok_or(BootClockError::InvalidReading)?;
    Ok(BootInstant(Duration::new(seconds, nanoseconds)))
}

#[cfg(not(target_os = "linux"))]
fn linux_boot_time() -> Result<BootInstant, BootClockError> {
    Err(BootClockError::UnsupportedPlatform)
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum BootClockError {
    #[error("CLOCK_BOOTTIME is unavailable on this platform")]
    UnsupportedPlatform,
    #[error("CLOCK_BOOTTIME failed with operating-system error {0}")]
    System(i32),
    #[error("CLOCK_BOOTTIME returned an invalid value")]
    InvalidReading,
}

/// The local time captured immediately before sending a grant request.
#[derive(Debug)]
pub(crate) struct ServingGrantRequest {
    identity: ServingFenceIdentity,
    started_at: BootInstant,
}

#[derive(Clone, Copy, Debug)]
struct ActiveGrant {
    identity: ServingFenceIdentity,
    expires_at: BootInstant,
}

/// Local state for one node-wide serving fence.
pub(crate) struct ServingFence<C = LinuxBootClock> {
    clock: C,
    current: ServingFenceIdentity,
    active: Option<ActiveGrant>,
    next_renewal_at: Option<BootInstant>,
    last_clock_reading: Option<BootInstant>,
}

impl<C: BootClock> ServingFence<C> {
    pub(crate) fn new(clock: C, current: ServingFenceIdentity) -> Self {
        Self {
            clock,
            current,
            active: None,
            next_renewal_at: None,
            last_clock_reading: None,
        }
    }

    /// Records newly applied Raft state without making the old grant valid for
    /// it. The next validation observes the identity mismatch and fails closed.
    pub(crate) fn observe_consensus(
        &mut self,
        raft_term: u64,
        active_membership_log_index: u64,
    ) -> Result<(), ServingFenceError> {
        if raft_term < self.current.raft_term {
            return Err(ServingFenceError::TermRegression {
                current: self.current.raft_term,
                received: raft_term,
            });
        }
        if active_membership_log_index < self.current.active_membership_log_index {
            return Err(ServingFenceError::MembershipRegression {
                current: self.current.active_membership_log_index,
                received: active_membership_log_index,
            });
        }

        let changed = self.current.raft_term != raft_term
            || self.current.active_membership_log_index != active_membership_log_index;
        self.current.raft_term = raft_term;
        self.current.active_membership_log_index = active_membership_log_index;
        if changed {
            self.next_renewal_at = None;
        }
        Ok(())
    }

    /// Whether the first request or the next fixed-cadence renewal should be
    /// started now. A slow in-flight request never postpones the cadence.
    pub(crate) fn renewal_due(&mut self) -> Result<bool, ServingFenceError> {
        let Some(next_renewal_at) = self.next_renewal_at else {
            return Ok(true);
        };
        Ok(self.read_clock()? >= next_renewal_at)
    }

    /// Captures the local start of a future grant round trip.
    pub(crate) fn start_grant_request(&mut self) -> Result<ServingGrantRequest, ServingFenceError> {
        let started_at = self.read_clock()?;
        self.next_renewal_at = Some(
            started_at
                .checked_add(SERVING_LEASE_RENEWAL_CADENCE)
                .ok_or(ServingFenceError::ClockRangeExceeded)?,
        );
        Ok(ServingGrantRequest {
            identity: self.current,
            started_at,
        })
    }

    /// Accepts a returned grant. Its expiry is anchored to `request.started_at`,
    /// so network and scheduling delay consume rather than extend its lifetime.
    pub(crate) fn accept_grant(
        &mut self,
        request: ServingGrantRequest,
        grant: ServingGrant,
    ) -> Result<(), ServingFenceError> {
        if grant.lifetime > SERVING_LEASE_MAX_LIFETIME {
            return Err(ServingFenceError::LifetimeTooLong {
                maximum: SERVING_LEASE_MAX_LIFETIME,
                received: grant.lifetime,
            });
        }
        validate_identity(self.current, request.identity)?;
        validate_identity(self.current, grant.identity)?;

        let expires_at = request
            .started_at
            .checked_add(grant.lifetime)
            .ok_or(ServingFenceError::ClockRangeExceeded)?;
        let now = self.read_clock()?;
        if now >= expires_at {
            return Err(ServingFenceError::Expired);
        }

        // Out-of-order renewal responses must not shorten a still-valid grant.
        let should_replace = self.active.is_none_or(|active| {
            active.identity != grant.identity || expires_at > active.expires_at
        });
        if should_replace {
            self.active = Some(ActiveGrant {
                identity: grant.identity,
                expires_at,
            });
        }
        Ok(())
    }

    /// Proves that this node currently holds a grant for its exact applied
    /// cluster, term, and active membership.
    pub(crate) fn validate(&mut self) -> Result<(), ServingFenceError> {
        let active = self.active.ok_or(ServingFenceError::NoGrant)?;
        validate_identity(self.current, active.identity)?;
        if self.read_clock()? >= active.expires_at {
            return Err(ServingFenceError::Expired);
        }
        Ok(())
    }

    fn read_clock(&mut self) -> Result<BootInstant, ServingFenceError> {
        let now = self.clock.now()?;
        if self.last_clock_reading.is_some_and(|last| now < last) {
            return Err(ServingFenceError::ClockMovedBackwards);
        }
        self.last_clock_reading = Some(now);
        Ok(now)
    }
}

fn validate_identity(
    current: ServingFenceIdentity,
    received: ServingFenceIdentity,
) -> Result<(), ServingFenceError> {
    if received.cluster_id != current.cluster_id {
        return Err(ServingFenceError::WrongCluster {
            current: current.cluster_id,
            received: received.cluster_id,
        });
    }
    if received.active_membership_log_index != current.active_membership_log_index {
        return Err(ServingFenceError::WrongMembership {
            current: current.active_membership_log_index,
            received: received.active_membership_log_index,
        });
    }
    if received.raft_term < current.raft_term {
        return Err(ServingFenceError::TermRegression {
            current: current.raft_term,
            received: received.raft_term,
        });
    }
    if received.raft_term != current.raft_term {
        return Err(ServingFenceError::WrongTerm {
            current: current.raft_term,
            received: received.raft_term,
        });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub(crate) enum ServingFenceError {
    #[error("serving grant belongs to another cluster")]
    WrongCluster {
        current: ClusterId,
        received: ClusterId,
    },
    #[error("serving grant membership index is {received}, not {current}")]
    WrongMembership { current: u64, received: u64 },
    #[error("serving membership index regressed from {current} to {received}")]
    MembershipRegression { current: u64, received: u64 },
    #[error("serving grant Raft term regressed from {current} to {received}")]
    TermRegression { current: u64, received: u64 },
    #[error("serving grant Raft term is {received}, not {current}")]
    WrongTerm { current: u64, received: u64 },
    #[error("serving grant lifetime {received:?} exceeds {maximum:?}")]
    LifetimeTooLong {
        maximum: Duration,
        received: Duration,
    },
    #[error("no serving grant is installed")]
    NoGrant,
    #[error("serving grant has expired")]
    Expired,
    #[error("serving grant expiry exceeds the boot-clock range")]
    ClockRangeExceeded,
    #[error("boot clock moved backwards")]
    ClockMovedBackwards,
    #[error(transparent)]
    Clock(#[from] BootClockError),
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone)]
    struct FakeBootClock {
        state: Arc<Mutex<Result<BootInstant, BootClockError>>>,
    }

    impl FakeBootClock {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(Ok(BootInstant(Duration::ZERO)))),
            }
        }

        fn set(&self, elapsed: Duration) {
            *self.state.lock().unwrap() = Ok(BootInstant(elapsed));
        }

        fn fail(&self) {
            *self.state.lock().unwrap() = Err(BootClockError::System(5));
        }
    }

    impl BootClock for FakeBootClock {
        fn now(&self) -> Result<BootInstant, BootClockError> {
            *self.state.lock().unwrap()
        }
    }

    fn cluster(byte: u8) -> ClusterId {
        ClusterId([byte; 16])
    }

    fn identity(term: u64, membership: u64) -> ServingFenceIdentity {
        ServingFenceIdentity {
            cluster_id: cluster(1),
            raft_term: term,
            active_membership_log_index: membership,
        }
    }

    fn grant(identity: ServingFenceIdentity) -> ServingGrant {
        ServingGrant {
            identity,
            lifetime: SERVING_LEASE_MAX_LIFETIME,
        }
    }

    #[test]
    fn protocol_intervals_are_fixed() {
        assert_eq!(SERVING_LEASE_MAX_LIFETIME, Duration::from_secs(2));
        assert_eq!(SERVING_LEASE_RENEWAL_CADENCE, Duration::from_millis(500));
        assert_eq!(SERVING_MEMBERSHIP_CUTOVER_WAIT, Duration::from_secs(3));
    }

    #[test]
    fn response_delay_consumes_the_grant_lifetime() {
        let clock = FakeBootClock::new();
        let current = identity(7, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let request = fence.start_grant_request().unwrap();

        clock.set(Duration::from_millis(1_500));
        fence.accept_grant(request, grant(current)).unwrap();
        clock.set(Duration::from_millis(1_999));
        fence.validate().unwrap();
        clock.set(Duration::from_secs(2));
        assert_eq!(fence.validate(), Err(ServingFenceError::Expired));
    }

    #[test]
    fn response_arriving_after_expiry_is_rejected() {
        let clock = FakeBootClock::new();
        let current = identity(7, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let request = fence.start_grant_request().unwrap();

        clock.set(Duration::from_millis(2_001));
        assert_eq!(
            fence.accept_grant(request, grant(current)),
            Err(ServingFenceError::Expired)
        );
    }

    #[test]
    fn suspend_like_time_advance_expires_the_grant() {
        let clock = FakeBootClock::new();
        let current = identity(7, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let request = fence.start_grant_request().unwrap();
        fence.accept_grant(request, grant(current)).unwrap();

        clock.set(Duration::from_secs(60));
        assert_eq!(fence.validate(), Err(ServingFenceError::Expired));
    }

    #[test]
    fn membership_change_rejects_an_in_flight_and_an_active_grant() {
        let clock = FakeBootClock::new();
        let old = identity(7, 11);
        let mut fence = ServingFence::new(clock, old);
        let request = fence.start_grant_request().unwrap();
        fence.observe_consensus(7, 12).unwrap();

        assert_eq!(
            fence.accept_grant(request, grant(old)),
            Err(ServingFenceError::WrongMembership {
                current: 12,
                received: 11,
            })
        );

        let current = identity(7, 12);
        let request = fence.start_grant_request().unwrap();
        fence.accept_grant(request, grant(current)).unwrap();
        fence.observe_consensus(7, 13).unwrap();
        assert_eq!(
            fence.validate(),
            Err(ServingFenceError::WrongMembership {
                current: 13,
                received: 12,
            })
        );
    }

    #[test]
    fn stale_term_is_rejected() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock, current);
        let request = fence.start_grant_request().unwrap();
        let stale = identity(4, 11);

        assert_eq!(
            fence.accept_grant(request, grant(stale)),
            Err(ServingFenceError::TermRegression {
                current: 5,
                received: 4,
            })
        );
    }

    #[test]
    fn wrong_cluster_is_rejected() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock, current);
        let request = fence.start_grant_request().unwrap();
        let mut wrong = current;
        wrong.cluster_id = cluster(2);

        assert!(matches!(
            fence.accept_grant(request, grant(wrong)),
            Err(ServingFenceError::WrongCluster { .. })
        ));
    }

    #[test]
    fn overlong_grant_is_rejected() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock, current);
        let request = fence.start_grant_request().unwrap();
        let overlong = ServingGrant {
            identity: current,
            lifetime: SERVING_LEASE_MAX_LIFETIME + Duration::from_nanos(1),
        };

        assert!(matches!(
            fence.accept_grant(request, overlong),
            Err(ServingFenceError::LifetimeTooLong { .. })
        ));
    }

    #[test]
    fn renewal_extends_from_its_own_request_start() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let first = fence.start_grant_request().unwrap();
        fence.accept_grant(first, grant(current)).unwrap();

        clock.set(SERVING_LEASE_RENEWAL_CADENCE);
        let renewal = fence.start_grant_request().unwrap();
        clock.set(Duration::from_millis(600));
        fence.accept_grant(renewal, grant(current)).unwrap();

        clock.set(Duration::from_millis(2_100));
        fence.validate().unwrap();
        clock.set(Duration::from_millis(2_500));
        assert_eq!(fence.validate(), Err(ServingFenceError::Expired));
    }

    #[test]
    fn renewal_cadence_is_anchored_to_each_request_start() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock.clone(), current);

        assert_eq!(fence.renewal_due(), Ok(true));
        let _first = fence.start_grant_request().unwrap();
        clock.set(Duration::from_millis(499));
        assert_eq!(fence.renewal_due(), Ok(false));
        clock.set(SERVING_LEASE_RENEWAL_CADENCE);
        assert_eq!(fence.renewal_due(), Ok(true));

        let _second = fence.start_grant_request().unwrap();
        clock.set(Duration::from_millis(999));
        assert_eq!(fence.renewal_due(), Ok(false));
        clock.set(Duration::from_millis(1_000));
        assert_eq!(fence.renewal_due(), Ok(true));
    }

    #[test]
    fn consensus_change_requests_a_fresh_grant_immediately() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let request = fence.start_grant_request().unwrap();
        fence.accept_grant(request, grant(current)).unwrap();

        clock.set(Duration::from_millis(100));
        assert_eq!(fence.renewal_due(), Ok(false));
        fence.observe_consensus(6, 12).unwrap();
        assert_eq!(fence.renewal_due(), Ok(true));
    }

    #[test]
    fn clock_failure_and_regression_fail_closed() {
        let clock = FakeBootClock::new();
        let current = identity(5, 11);
        let mut fence = ServingFence::new(clock.clone(), current);
        let request = fence.start_grant_request().unwrap();
        fence.accept_grant(request, grant(current)).unwrap();

        clock.fail();
        assert!(matches!(
            fence.validate(),
            Err(ServingFenceError::Clock(BootClockError::System(5)))
        ));

        clock.set(Duration::from_secs(1));
        fence.validate().unwrap();
        clock.set(Duration::from_millis(900));
        assert_eq!(
            fence.validate(),
            Err(ServingFenceError::ClockMovedBackwards)
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_clock_reads_clock_boottime() {
        let first = LinuxBootClock.now().unwrap();
        let second = LinuxBootClock.now().unwrap();
        assert!(second >= first);
    }
}
