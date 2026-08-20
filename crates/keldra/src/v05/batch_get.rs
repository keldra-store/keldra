//! Bounded parallel execution for independent batch reads.

use std::sync::Arc;

use keldra_api::v1::BatchGetOutcome;
use keldra_api::v1::batch_get_outcome::Outcome as BatchGetResult;
use keldra_store::{ObjectKey, VersionId};
use tonic::Status;

use super::{api_address, distributed_reads};
use crate::cluster_object_read::ClusterObjectReader;
use crate::object_distribution::ObjectDistribution;

const MAX_PARALLEL_READS: usize = 32;

struct SelectedRead {
    index: usize,
    key: ObjectKey,
    requested_version: Option<VersionId>,
}

pub(super) async fn read_accepted(
    distribution: ObjectDistribution,
    reader: ClusterObjectReader,
    accepted: Vec<(usize, ObjectKey, Option<VersionId>)>,
    maximum_payload_bytes: u64,
) -> Result<(Vec<BatchGetOutcome>, Option<u64>), Status> {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_READS));
    let mut declarations = tokio::task::JoinSet::new();
    for (index, key, requested_version) in accepted {
        let distribution = distribution.clone();
        let permits = permits.clone();
        declarations.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .map_err(|_| Status::internal("batch-read scheduler stopped"))?;
            let result = distribution
                .reconciled_object_snapshot(&key)
                .await
                .and_then(|snapshot| {
                    distributed_reads::declared_payload_length(
                        snapshot.as_ref(),
                        &key,
                        requested_version,
                    )
                });
            Ok::<_, Status>((index, key, requested_version, result))
        });
    }

    let mut outcomes = Vec::new();
    let mut selected = Vec::new();
    let mut declared_total = 0_u64;
    while let Some(joined) = declarations.join_next().await {
        let (index, key, requested_version, result) = joined
            .map_err(|error| Status::internal(format!("batch-read task failed: {error}")))??;
        match result {
            Ok(declared_bytes) => {
                declared_total = declared_total.saturating_add(declared_bytes);
                selected.push(SelectedRead {
                    index,
                    key,
                    requested_version,
                });
            }
            Err(error) => outcomes.push(failed(index, &key, error)),
        }
    }
    enforce_limit(declared_total, maximum_payload_bytes)?;

    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_PARALLEL_READS));
    let mut reads = tokio::task::JoinSet::new();
    for selected in selected {
        let reader = reader.clone();
        let permits = permits.clone();
        reads.spawn(async move {
            let _permit = permits
                .acquire_owned()
                .await
                .map_err(|_| Status::internal("batch-read scheduler stopped"))?;
            let result = distributed_reads::read_batch_result(
                &reader,
                &selected.key,
                selected.requested_version,
            )
            .await;
            Ok::<_, Status>((selected, result))
        });
    }

    let mut materialized_total = 0_u64;
    let mut maximum_program_cursor = None;
    while let Some(joined) = reads.join_next().await {
        let (selected, result) = joined
            .map_err(|error| Status::internal(format!("batch-read task failed: {error}")))??;
        let outcome = match result {
            Ok((outcome, length, program_cursor)) => {
                materialized_total = materialized_total.saturating_add(length);
                maximum_program_cursor = maximum_program_cursor.max(program_cursor);
                outcome
            }
            Err(error) => BatchGetResult::Failure(distributed_reads::status_failure(error)),
        };
        outcomes.push(BatchGetOutcome {
            index: selected.index as u32,
            address: Some(api_address(&selected.key)),
            outcome: Some(outcome),
        });
    }
    enforce_limit(materialized_total, maximum_payload_bytes)?;
    Ok((outcomes, maximum_program_cursor))
}

fn failed(index: usize, key: &ObjectKey, error: Status) -> BatchGetOutcome {
    BatchGetOutcome {
        index: index as u32,
        address: Some(api_address(key)),
        outcome: Some(BatchGetResult::Failure(distributed_reads::status_failure(
            error,
        ))),
    }
}

fn enforce_limit(actual: u64, maximum: u64) -> Result<(), Status> {
    if actual > maximum {
        Err(Status::resource_exhausted(format!(
            "batch read payload exceeds the {maximum}-byte limit"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_limit_is_exact_and_overflow_safe() {
        assert!(enforce_limit(64, 64).is_ok());
        assert_eq!(
            enforce_limit(65, 64).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
        assert!(enforce_limit(u64::MAX, u64::MAX).is_ok());
    }
}
