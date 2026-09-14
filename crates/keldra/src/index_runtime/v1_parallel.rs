//! Bounded execution for independent format-v1 partition work.

use std::collections::VecDeque;
use std::future::Future;

use keldra_index::IndexError;
use tonic::Status;

use super::cpu::IndexQueryScheduler;

pub(super) fn partition_lane_parallelism(
    total_parallelism: usize,
    active_writers: usize,
    writer_ordinal: usize,
) -> usize {
    let total_parallelism = total_parallelism.max(1);
    let active_writers = active_writers.clamp(1, total_parallelism);
    total_parallelism / active_writers
        + usize::from(writer_ordinal < total_parallelism % active_writers)
}

/// Run independent work concurrently and return every completed outcome in
/// stable key order. A task-level application error is an ordinary outcome, so
/// peers continue to completion before the producer selects an error and
/// reloads durable Current state.
pub(super) async fn run_bounded_ordered<K, I, O, F, Fut>(
    work: Vec<(K, I)>,
    maximum_parallelism: usize,
    operation: F,
) -> Result<Vec<(K, O)>, Status>
where
    K: Copy + Ord + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    let mut pending = VecDeque::from(work);
    let mut active = tokio::task::JoinSet::new();
    let mut output = Vec::with_capacity(pending.len());
    let maximum_parallelism = maximum_parallelism.max(1);

    while active.len() < maximum_parallelism {
        let Some((key, input)) = pending.pop_front() else {
            break;
        };
        let operation = operation.clone();
        active.spawn(async move { (key, operation(input).await) });
    }

    let mut join_failure = None;
    while let Some(completed) = active.join_next().await {
        match completed {
            Ok(completed) => output.push(completed),
            Err(error) => {
                join_failure.get_or_insert_with(|| error.to_string());
            }
        }
        if let Some((key, input)) = pending.pop_front() {
            let operation = operation.clone();
            active.spawn(async move { (key, operation(input).await) });
        }
    }

    if let Some(error) = join_failure {
        return Err(Status::internal(format!(
            "v1 bounded partition task failed: {error}"
        )));
    }
    output.sort_unstable_by_key(|(key, _)| *key);
    Ok(output)
}

/// Run bounded independent query-partition I/O/async jobs and preserve their
/// stable key order. The scheduler's configured lane count is already capped
/// by the shared index CPU pool.
pub(super) async fn run_query_partitions_ordered<K, I, O, F, Fut>(
    scheduler: &IndexQueryScheduler,
    work: Vec<(K, I)>,
    operation: F,
) -> Result<Vec<(K, O)>, Status>
where
    K: Copy + Ord + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    let maximum_parallelism = scheduler.maximum_parallelism();
    let scheduler = scheduler.clone();
    run_bounded_ordered(work, maximum_parallelism, move |input| {
        let scheduler = scheduler.clone();
        let operation = operation.clone();
        async move { scheduler.run_partition(move || operation(input)).await }
    })
    .await
}

/// Run bounded independent query-partition CPU jobs on the one shared index
/// Rayon pool. Application errors remain ordered outcomes, so all admitted
/// siblings drain before the caller chooses a recovery action.
pub(super) async fn run_query_partition_cpu_ordered<K, I, O, F>(
    scheduler: &IndexQueryScheduler,
    work: Vec<(K, I)>,
    operation: F,
) -> Result<Vec<(K, Result<O, IndexError>)>, Status>
where
    K: Copy + Ord + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
    F: Fn(I) -> Result<O, IndexError> + Clone + Send + 'static,
{
    let maximum_parallelism = scheduler.maximum_parallelism();
    let scheduler = scheduler.clone();
    run_bounded_ordered(work, maximum_parallelism, move |input| {
        let scheduler = scheduler.clone();
        let operation = operation.clone();
        async move {
            let cpu = scheduler.clone();
            scheduler
                .run_partition(move || async move { cpu.run_cpu(move || operation(input)).await })
                .await
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use keldra_index::v1::{IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage};

    use super::*;
    use crate::index_runtime::cpu::IndexCpuPool;

    fn memory(bytes: usize) -> IndexingMemoryCredits {
        IndexingMemoryCredits::new(
            bytes,
            IndexingMemoryLimits {
                hot_payload_bytes: bytes,
                worker_scratch_bytes: bytes,
                prepared_rows_bytes: bytes,
                replay_input_bytes: bytes,
                projection_accumulator_bytes: bytes,
                seal_scratch_bytes: bytes,
                ordering_catalog_bytes: bytes,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn execution_is_bounded_and_results_are_stably_ordered() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let output = run_bounded_ordered((0..8).rev().map(|key| (key, key)).collect(), 2, {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            move |value| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(now, Ordering::AcqRel);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    active.fetch_sub(1, Ordering::AcqRel);
                    value
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(peak.load(Ordering::Acquire), 2);
        assert_eq!(output, (0..8).map(|key| (key, key)).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn application_failure_drains_independent_work_for_durable_recovery() {
        let completed = Arc::new(AtomicUsize::new(0));
        let output = run_bounded_ordered((0..5).map(|key| (key, key)).collect(), 2, {
            let completed = Arc::clone(&completed);
            move |value| {
                let completed = Arc::clone(&completed);
                async move {
                    tokio::task::yield_now().await;
                    completed.fetch_add(1, Ordering::AcqRel);
                    if value == 2 { Err(value) } else { Ok(value) }
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(completed.load(Ordering::Acquire), 5);
        assert_eq!(output[2], (2, Err(2)));
    }

    #[tokio::test]
    async fn each_partition_keeps_prepare_artifacts_current_order() {
        let stages = Arc::new(Mutex::new(BTreeMap::<u64, Vec<&'static str>>::new()));
        run_bounded_ordered(vec![(2, 2), (1, 1)], 2, {
            let stages = Arc::clone(&stages);
            move |partition| {
                let stages = Arc::clone(&stages);
                async move {
                    for stage in ["prepare", "artifacts", "current"] {
                        stages
                            .lock()
                            .unwrap()
                            .entry(partition)
                            .or_default()
                            .push(stage);
                        tokio::task::yield_now().await;
                    }
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(
            stages.lock().unwrap().get(&1).unwrap(),
            &["prepare", "artifacts", "current"]
        );
        assert_eq!(
            stages.lock().unwrap().get(&2).unwrap(),
            &["prepare", "artifacts", "current"]
        );
    }

    #[tokio::test]
    async fn concurrent_work_remains_inside_shared_memory_credits() {
        let credits = memory(2);
        let peak = Arc::new(AtomicUsize::new(0));
        run_bounded_ordered((0..6).map(|key| (key, ())).collect(), 2, {
            let credits = credits.clone();
            let peak = Arc::clone(&peak);
            move |_| {
                let credits = credits.clone();
                let peak = Arc::clone(&peak);
                async move {
                    let _permit = credits
                        .acquire(IndexingMemoryStage::ReplayInput, 1)
                        .unwrap();
                    peak.fetch_max(credits.used_bytes(), Ordering::AcqRel);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(peak.load(Ordering::Acquire), 2);
        assert_eq!(credits.used_bytes(), 0);
    }

    #[tokio::test]
    async fn query_scheduler_caps_lanes_and_drains_ordered_cpu_failures() {
        let scheduler = IndexCpuPool::new(2).unwrap().query_scheduler(8);
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let output = run_query_partition_cpu_ordered(
            &scheduler,
            (0..6).rev().map(|key| (key, key)).collect(),
            {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                let completed = Arc::clone(&completed);
                move |value| {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(now, Ordering::AcqRel);
                    std::thread::sleep(Duration::from_millis(2));
                    active.fetch_sub(1, Ordering::AcqRel);
                    completed.fetch_add(1, Ordering::AcqRel);
                    if value == 3 {
                        Err(IndexError::Integrity)
                    } else {
                        Ok(value)
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(scheduler.maximum_parallelism(), 2);
        assert_eq!(peak.load(Ordering::Acquire), 2);
        assert_eq!(completed.load(Ordering::Acquire), 6);
        assert_eq!(
            output.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
            (0..6).collect::<Vec<_>>()
        );
        assert!(matches!(&output[3].1, Err(IndexError::Integrity)));
    }

    #[tokio::test]
    async fn query_scheduler_bounds_clones_and_orders_async_partition_work() {
        let pool = IndexCpuPool::new(2).unwrap();
        let first_scheduler = pool.query_scheduler(2);
        let second_scheduler = first_scheduler.clone();
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let operation = {
            let active = Arc::clone(&active);
            let peak = Arc::clone(&peak);
            move |value| {
                let active = Arc::clone(&active);
                let peak = Arc::clone(&peak);
                async move {
                    let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                    peak.fetch_max(now, Ordering::AcqRel);
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    active.fetch_sub(1, Ordering::AcqRel);
                    value
                }
            }
        };
        let first = run_query_partitions_ordered(
            &first_scheduler,
            (0..4).rev().map(|key| (key, key)).collect(),
            operation.clone(),
        );
        let second = run_query_partitions_ordered(
            &second_scheduler,
            (4..8).rev().map(|key| (key, key)).collect(),
            operation,
        );
        let (first, second) = tokio::join!(first, second);
        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(peak.load(Ordering::Acquire), 2);
        assert_eq!(first, (0..4).map(|key| (key, key)).collect::<Vec<_>>());
        assert_eq!(second, (4..8).map(|key| (key, key)).collect::<Vec<_>>());
    }

    #[test]
    fn preparation_lanes_are_distributed_without_oversubscription() {
        let lanes = (0..3)
            .map(|ordinal| partition_lane_parallelism(8, 3, ordinal))
            .collect::<Vec<_>>();
        assert_eq!(lanes, vec![3, 3, 2]);
        assert_eq!(lanes.into_iter().sum::<usize>(), 8);
        let worker_bytes = 1_024 / 8;
        assert_eq!((3 + 3 + 2) * worker_bytes, 1_024);
        assert_eq!(partition_lane_parallelism(8, 8, 7), 1);
    }
}
