use super::*;

#[test]
fn marker_ids_do_not_overlap_small_corpus_ids() {
    assert_eq!((1u64 << 63) | 7, 9_223_372_036_854_775_815);
    assert!(data::marker_path(7).contains("0000000000000007"));
}

#[test]
fn recipes_use_one_public_multivalue_field_and_distinct_source_pointers() {
    assert_eq!(recipe_probe_pointer(0), "/probes/00");
    assert_eq!(recipe_probe_pointer(1), "/probes/01");
    assert_eq!(recipe_probe_pointer(63), "/probes/63");
}

#[test]
fn p1_definitions_have_identical_physical_recipes() {
    assert_eq!(physical_recipe(0, 1), 0);
    assert_eq!(physical_recipe(249_999, 1), 0);
}

#[test]
fn recipe_identity_is_bounded_independently_of_definition_count() {
    let recipes = (0..250_000)
        .map(|position| physical_recipe(position, 64))
        .collect::<BTreeSet<_>>();
    assert_eq!(recipes.len(), 64);
    let pointers = (0..64).map(recipe_probe_pointer).collect::<BTreeSet<_>>();
    assert_eq!(pointers.len(), 64);
}

#[test]
fn large_catalog_uses_a_bounded_spanning_query_sample() {
    let positions = qualification_definition_positions(250_000, 64, 1_024);
    assert_eq!(positions.len(), MAX_ACTIVE_QUERY_DEFINITIONS);
    assert_eq!(positions[0], 0);
    assert_eq!(*positions.last().unwrap(), 249_999);
    assert!((0..64).all(|recipe| positions.contains(&recipe)));
}

#[test]
fn optional_observed_tail_never_manufactures_zero_lag() {
    let unavailable = IndexSourceFreshness {
        indexed_next_offset: 12,
        lag_hint: 0,
        observed_tail: None,
        ..Default::default()
    };
    assert!(source_has_no_observed_lag(&unavailable));
    assert!(unavailable.observed_tail.is_none());

    let current = IndexSourceFreshness {
        observed_tail: Some(11),
        ..unavailable.clone()
    };
    assert!(source_has_no_observed_lag(&current));

    let behind = IndexSourceFreshness {
        observed_tail: Some(12),
        lag_hint: 1,
        ..unavailable
    };
    assert!(!source_has_no_observed_lag(&behind));
}

#[test]
fn visibility_samples_rotate_independently_of_canary_interval() {
    let positions = (0..20)
        .map(|ordinal| visibility_definition_position(ordinal, 16))
        .collect::<Vec<_>>();
    assert_eq!(&positions[..16], &(0..16).collect::<Vec<_>>());
    assert_eq!(&positions[16..], &[0, 1, 2, 3]);
}

#[test]
fn projection_preserving_visibility_samples_are_never_overwritten() {
    assert_eq!(
        marker_ordinal(MutationWorkload::ProjectionPreserving, 7, false),
        7
    );
    assert_eq!(
        marker_ordinal(MutationWorkload::ProjectionPreserving, 263, false),
        7
    );
    assert_eq!(
        marker_ordinal(MutationWorkload::ProjectionPreserving, 0, true),
        data::PROJECTION_PRESERVING_MARKERS
    );
    assert_eq!(
        marker_ordinal(MutationWorkload::ProjectionPreserving, 256, true),
        data::PROJECTION_PRESERVING_MARKERS + 256
    );
}

#[test]
fn visibility_failure_errors_are_bounded_on_character_boundaries() {
    let error = "é".repeat(MAX_VISIBILITY_SAMPLE_ERROR_CHARS + 10);
    let bounded = bounded_error(&error);
    assert_eq!(bounded.chars().count(), MAX_VISIBILITY_SAMPLE_ERROR_CHARS);
    assert!(error.starts_with(&bounded));
}

#[test]
fn mutation_failure_messages_are_bounded_on_character_boundaries() {
    let message = "é".repeat(MAX_MUTATION_FAILURE_MESSAGE_CHARS + 10);
    let bounded = bounded_mutation_failure_message(&message);
    assert_eq!(bounded.chars().count(), MAX_MUTATION_FAILURE_MESSAGE_CHARS);
    assert!(message.starts_with(&bounded));
}

#[test]
fn mutation_failure_classes_are_counted_and_bounded() {
    let mut report = MutationReport::default();
    for ordinal in 0..MAX_MUTATION_FAILURE_CLASSES {
        record_mutation_failure(
            &mut report,
            MutationRequestFailure::one(
                "outcome",
                ordinal as i32,
                format!("Code{ordinal}"),
                format!("failure-{ordinal}"),
            ),
        );
    }
    record_mutation_failure(
        &mut report,
        MutationRequestFailure::one("outcome", 0, "Code0".into(), "failure-0".into()),
    );
    record_mutation_failure(
        &mut report,
        MutationRequestFailure::one(
            "rpc-status",
            tonic::Code::Unavailable as i32,
            "Unavailable".into(),
            "queue closed".into(),
        ),
    );

    assert_eq!(report.failure_classes.len(), MAX_MUTATION_FAILURE_CLASSES);
    assert_eq!(report.failure_classes[0].count, 2);
    assert_eq!(report.failure_occurrences_omitted, 1);
}

#[tokio::test]
async fn fixed_rate_records_every_schedule_and_queue_drop() {
    let (job_tx, mut job_rx) = mpsc::channel(2);
    let started = Instant::now();
    let report = produce_fixed_rate_jobs(
        started,
        started + Duration::from_millis(70),
        32,
        3_300.0,
        job_tx,
    )
    .await
    .unwrap();
    assert_eq!(report.scheduled_batches, 7);
    assert_eq!(report.scheduler_deadline_missed_batches, 0);
    assert_eq!(report.client_queue_enqueued_batches, 2);
    assert_eq!(report.client_queue_dropped_batches, 5);
    assert_eq!(job_rx.recv().await.unwrap().sequence, 0);
    assert_eq!(job_rx.recv().await.unwrap().sequence, 1);
}

#[tokio::test]
async fn fixed_rate_does_not_dispatch_schedules_after_deadline() {
    let (job_tx, mut job_rx) = mpsc::channel(16);
    let deadline = Instant::now() - Duration::from_millis(1);
    let started = deadline - Duration::from_millis(70);
    let report = produce_fixed_rate_jobs(started, deadline, 32, 3_300.0, job_tx)
        .await
        .unwrap();
    assert_eq!(report.scheduled_batches, 7);
    assert_eq!(report.scheduler_deadline_missed_batches, 7);
    assert_eq!(report.client_queue_enqueued_batches, 0);
    assert!(job_rx.try_recv().is_err());
}
