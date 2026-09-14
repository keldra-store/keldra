//! Storage-neutral scheduling for independent query-partition jobs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::IndexError;

use super::{
    PinnedPartitionQueryRoot, QueryArtifactLoader, QueryBlockCredits, QueryBlockLimits,
    QueryCandidateAdmission, QueryCommonCut, QueryExecutionLimits, ScalarSortKeyValueEncoder,
    TypedJsonQueryRequest, TypedJsonQueryResult, ValidatedQuerySnapshot,
};

/// One owned partition job. Jobs are owned so a runtime executor may move them
/// onto its bounded scheduler without borrowing the query coordinator.
pub type QueryPartitionJob<O> =
    Pin<Box<dyn Future<Output = Result<O, IndexError>> + Send + 'static>>;

/// Executes already-independent partition jobs under a runtime-owned bound.
///
/// Implementations must await every admitted job, including after one job
/// fails, must run no more than `maximum_parallelism()` jobs concurrently, and
/// return successful results in ascending key order. Use
/// [`resolve_query_partition_results`] after concurrent execution to apply the
/// canonical ordering and deterministic error-selection rule.
pub trait QueryPartitionExecutor: Send + Sync {
    fn maximum_parallelism(&self) -> usize;

    fn execute_ordered<K, O>(
        &self,
        jobs: Vec<(K, QueryPartitionJob<O>)>,
    ) -> impl Future<Output = Result<Vec<(K, O)>, IndexError>> + Send
    where
        K: Copy + Ord + Send + 'static,
        O: Send + 'static;
}

/// Existing callers retain serial behavior until a runtime injects its bounded
/// scheduler explicitly.
#[derive(Clone, Copy, Debug, Default)]
pub struct SerialQueryPartitionExecutor;

impl QueryPartitionExecutor for SerialQueryPartitionExecutor {
    fn maximum_parallelism(&self) -> usize {
        1
    }

    async fn execute_ordered<K, O>(
        &self,
        jobs: Vec<(K, QueryPartitionJob<O>)>,
    ) -> Result<Vec<(K, O)>, IndexError>
    where
        K: Copy + Ord + Send + 'static,
        O: Send + 'static,
    {
        let mut outcomes = Vec::with_capacity(jobs.len());
        for (key, job) in jobs {
            outcomes.push((key, job.await));
        }
        resolve_query_partition_results(outcomes)
    }
}

/// Execute a query with a runtime-supplied partition scheduler. Existing
/// callers use `execute_typed_json_query`, which injects the serial executor.
#[allow(clippy::too_many_arguments)]
pub async fn execute_typed_json_query_with_executor<
    L: QueryArtifactLoader + 'static,
    A: QueryCandidateAdmission,
    X: QueryPartitionExecutor,
>(
    loader: &mut L,
    admission: &mut A,
    common_cut: QueryCommonCut,
    pins: &[PinnedPartitionQueryRoot],
    validated_snapshot: Option<Arc<ValidatedQuerySnapshot>>,
    request: &TypedJsonQueryRequest,
    execution_limits: QueryExecutionLimits,
    block_limits: QueryBlockLimits,
    block_credits: &mut QueryBlockCredits,
    partition_executor: &X,
) -> Result<(TypedJsonQueryResult, Arc<ValidatedQuerySnapshot>), IndexError> {
    let (result, snapshot, _) =
        super::query_executor::execute_typed_json_query_with_cursor_and_executor(
            loader,
            admission,
            common_cut,
            pins,
            validated_snapshot,
            request,
            None,
            &ScalarSortKeyValueEncoder,
            execution_limits,
            block_limits,
            block_credits,
            partition_executor,
        )
        .await?;
    Ok((result, snapshot))
}

/// Canonicalize completed parallel outcomes after all admitted jobs drain.
/// If multiple jobs failed, the lowest-key failure is returned deterministically.
pub fn resolve_query_partition_results<K, O>(
    mut outcomes: Vec<(K, Result<O, IndexError>)>,
) -> Result<Vec<(K, O)>, IndexError>
where
    K: Copy + Ord,
{
    outcomes.sort_unstable_by_key(|(key, _)| *key);
    outcomes
        .into_iter()
        .map(|(key, outcome)| outcome.map(|value| (key, value)))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    use super::*;

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn ready<T>(future: impl Future<Output = T>) -> T {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("in-memory future unexpectedly yielded"),
        }
    }

    #[test]
    fn serial_executor_drains_all_jobs_and_selects_failure_by_key() {
        let completed = Arc::new(AtomicUsize::new(0));
        let jobs = [3usize, 1, 2]
            .into_iter()
            .map(|key| {
                let completed = Arc::clone(&completed);
                let job: QueryPartitionJob<usize> = Box::pin(async move {
                    completed.fetch_add(1, Ordering::AcqRel);
                    if key == 1 {
                        Err(IndexError::Integrity)
                    } else if key == 3 {
                        Err(IndexError::Io("later partition failed".into()))
                    } else {
                        Ok(key)
                    }
                });
                (key, job)
            })
            .collect();

        let result = ready(SerialQueryPartitionExecutor.execute_ordered(jobs));
        assert_eq!(result, Err(IndexError::Integrity));
        assert_eq!(completed.load(Ordering::Acquire), 3);
    }

    #[test]
    fn completed_parallel_results_are_canonically_ordered() {
        let output =
            resolve_query_partition_results(vec![(3, Ok(30)), (1, Ok(10)), (2, Ok(20))]).unwrap();
        assert_eq!(output, vec![(1, 10), (2, 20), (3, 30)]);
    }
}
