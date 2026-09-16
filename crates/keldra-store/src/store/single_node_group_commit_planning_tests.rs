use super::test_support::*;
use super::*;
use crate::BucketPolicy;

#[test]
fn conflicting_governance_stops_the_current_group() {
    let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
    let enqueued_at = Instant::now();
    let mut state = QueueState::default();
    let first = governance_stub();
    let mut second = first.clone();
    second.policy = BucketPolicy {
        immutable_prefixes: vec!["immutable".into()],
        ..BucketPolicy::default()
    };
    state.requests.push_back(queued_request(
        &queue,
        request("objects/first", "first", first),
        context(1),
        enqueued_at,
    ));
    state.requests.push_back(queued_request(
        &queue,
        request("objects/second", "second", second),
        context(1),
        enqueued_at,
    ));
    assert!(matches!(
        queue.next_queue_action(&mut state, enqueued_at),
        QueueAction::Group {
            requests,
            stop_reason: "governance",
            ..
        } if requests.len() == 1
    ));
    assert_eq!(state.requests.len(), 1);
}

#[test]
fn incompatible_context_stops_the_current_group() {
    let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
    let enqueued_at = Instant::now();
    let mut state = QueueState::default();
    let governance = governance_stub();
    state.requests.push_back(queued_request(
        &queue,
        request("objects/context-first", "context-first", governance.clone()),
        context(1),
        enqueued_at,
    ));
    state.requests.push_back(queued_request(
        &queue,
        request("objects/context-second", "context-second", governance),
        context(2),
        enqueued_at,
    ));
    assert!(matches!(
        queue.next_queue_action(&mut state, enqueued_at),
        QueueAction::Group {
            requests,
            stop_reason: "context",
            ..
        } if requests.len() == 1
    ));
    assert_eq!(state.requests.len(), 1);
}

#[test]
fn derived_progress_is_never_combined_with_an_ordinary_group() {
    let queue = SingleNodeGroupCommit::new(SingleNodeGroupCommitConfig::default());
    let enqueued_at = Instant::now();
    let mut state = QueueState::default();
    let governance = governance_stub();
    state.requests.push_back(queued_request(
        &queue,
        request("objects/ordinary", "ordinary", governance.clone()),
        context(1),
        enqueued_at,
    ));
    let mut derived = queued_request(
        &queue,
        request("objects/derived", "derived", governance),
        context(1),
        enqueued_at,
    );
    derived.source_journal_admission = SourceJournalAdmission::DerivedProgress;
    state.requests.push_back(derived);

    assert!(matches!(
        queue.next_queue_action(&mut state, enqueued_at),
        QueueAction::Group {
            requests,
            stop_reason: "source_journal_admission",
            ..
        } if requests.len() == 1
    ));
    assert_eq!(state.requests.len(), 1);
}

#[test]
fn shared_queue_accounting_does_not_wrap() {
    let queue = SingleNodeGroupCommit::new(
        SingleNodeGroupCommitConfig::default()
            .with_commit_lanes(2)
            .unwrap(),
    );
    assert_eq!(queue.record_enqueued_request(), 1);
    assert_eq!(queue.record_enqueued_request(), 2);
    assert_eq!(queue.record_dequeued_requests(1), 1);
    assert_eq!(queue.record_dequeued_requests(1), 0);
    assert_eq!(queue.queued_requests_peak.load(Ordering::Acquire), 2);
}
