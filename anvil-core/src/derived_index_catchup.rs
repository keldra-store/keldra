use crate::{
    derived_index_proof::{DerivedIndexProof, DerivedIndexValidity, validate_derived_index_source},
    watch_resume::{WatchResumeInput, WatchResumePlan, plan_watch_resume},
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedIndexCatchUpInput {
    pub index_id: String,
    pub consumer_id: String,
    pub watch_stream_id: String,
    pub checkpoint_cursor: u128,
    pub retained_start_cursor: u128,
    pub latest_cursor: u128,
    pub manifest_checkpoint_cursor: u128,
    pub source_manifest_hash: String,
    pub required_source_cursor: u128,
    pub min_generation: u64,
    pub latest_proof: Option<DerivedIndexProof>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedIndexCatchUpPlan {
    UpToDate {
        cursor: u128,
        generation: u64,
    },
    Replay {
        from_exclusive: u128,
        to_inclusive: u128,
        generation: u64,
    },
    RebuildFromManifest {
        reason: DerivedIndexRebuildReason,
        manifest_checkpoint_cursor: u128,
        source_manifest_hash: String,
        resume_after_cursor: u128,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DerivedIndexRebuildReason {
    MissingProof,
    StaleProof,
    ExpiredWatchWindow,
}

impl DerivedIndexCatchUpPlan {
    pub fn resume_cursor(&self) -> u128 {
        match self {
            Self::UpToDate { cursor, .. } => *cursor,
            Self::Replay { to_inclusive, .. } => *to_inclusive,
            Self::RebuildFromManifest {
                resume_after_cursor,
                ..
            } => *resume_after_cursor,
        }
    }
}

pub fn plan_derived_index_catch_up(
    input: DerivedIndexCatchUpInput,
    signing_key: &[u8],
) -> Result<DerivedIndexCatchUpPlan> {
    validate_input(&input)?;
    let Some(proof) = input.latest_proof.as_ref() else {
        return Ok(rebuild_plan(
            &input,
            DerivedIndexRebuildReason::MissingProof,
        ));
    };
    if proof.index_id != input.index_id {
        return Err(anyhow!("derived index proof index scope mismatch"));
    }
    match validate_derived_index_source(
        proof,
        input.required_source_cursor,
        &input.source_manifest_hash,
        input.min_generation,
        signing_key,
    )? {
        DerivedIndexValidity::Valid => {}
        DerivedIndexValidity::RebuildRequired => {
            return Ok(rebuild_plan(&input, DerivedIndexRebuildReason::StaleProof));
        }
    }

    match plan_watch_resume(WatchResumeInput {
        watch_stream_id: input.watch_stream_id.clone(),
        consumer_id: input.consumer_id.clone(),
        checkpoint_cursor: input.checkpoint_cursor,
        retained_start_cursor: input.retained_start_cursor,
        latest_cursor: input.latest_cursor,
        manifest_checkpoint_cursor: input.manifest_checkpoint_cursor,
        source_manifest_hash: input.source_manifest_hash.clone(),
    })? {
        WatchResumePlan::UpToDate { cursor } => Ok(DerivedIndexCatchUpPlan::UpToDate {
            cursor,
            generation: proof.generation,
        }),
        WatchResumePlan::Replay {
            from_exclusive,
            to_inclusive,
        } => Ok(DerivedIndexCatchUpPlan::Replay {
            from_exclusive,
            to_inclusive,
            generation: proof.generation,
        }),
        WatchResumePlan::RebuildFromManifest {
            manifest_checkpoint_cursor,
            source_manifest_hash,
            resume_after_cursor,
        } => Ok(DerivedIndexCatchUpPlan::RebuildFromManifest {
            reason: DerivedIndexRebuildReason::ExpiredWatchWindow,
            manifest_checkpoint_cursor,
            source_manifest_hash,
            resume_after_cursor,
        }),
    }
}

fn rebuild_plan(
    input: &DerivedIndexCatchUpInput,
    reason: DerivedIndexRebuildReason,
) -> DerivedIndexCatchUpPlan {
    DerivedIndexCatchUpPlan::RebuildFromManifest {
        reason,
        manifest_checkpoint_cursor: input.manifest_checkpoint_cursor,
        source_manifest_hash: input.source_manifest_hash.clone(),
        resume_after_cursor: input.manifest_checkpoint_cursor,
    }
}

fn validate_input(input: &DerivedIndexCatchUpInput) -> Result<()> {
    require_nonempty(&input.index_id, "index_id")?;
    require_nonempty(&input.consumer_id, "consumer_id")?;
    require_nonempty(&input.watch_stream_id, "watch_stream_id")?;
    if input.min_generation == 0 {
        return Err(anyhow!("derived index catch-up generation must be nonzero"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        derived_index_proof::DerivedIndexProof,
        task_lease::{TaskLeaseAcquire, TaskLeaseOwner, acquire_task_lease, checkpoint_task_lease},
    };
    use tempfile::tempdir;

    const KEY: &[u8] = b"derived index catchup signing key";

    #[test]
    fn catch_up_replays_after_handoff_when_watch_window_is_retained() {
        let input = input_with_checkpoint(42, 30, 50, 30).with_proof(proof(42, 7));

        let plan = plan_derived_index_catch_up(input, KEY).unwrap();

        assert_eq!(
            plan,
            DerivedIndexCatchUpPlan::Replay {
                from_exclusive: 42,
                to_inclusive: 50,
                generation: 7,
            }
        );
    }

    #[test]
    fn catch_up_rebuilds_from_manifest_when_watch_window_expired() {
        let input = input_with_checkpoint(10, 30, 80, 40).with_proof(proof(40, 7));

        let plan = plan_derived_index_catch_up(input, KEY).unwrap();

        assert_eq!(
            plan,
            DerivedIndexCatchUpPlan::RebuildFromManifest {
                reason: DerivedIndexRebuildReason::ExpiredWatchWindow,
                manifest_checkpoint_cursor: 40,
                source_manifest_hash: hex::encode([4; 32]),
                resume_after_cursor: 40,
            }
        );
    }

    #[test]
    fn catch_up_rebuilds_when_proof_is_missing_or_stale() {
        let missing = input_with_checkpoint(42, 30, 50, 30);
        assert_eq!(
            plan_derived_index_catch_up(missing, KEY).unwrap(),
            DerivedIndexCatchUpPlan::RebuildFromManifest {
                reason: DerivedIndexRebuildReason::MissingProof,
                manifest_checkpoint_cursor: 30,
                source_manifest_hash: hex::encode([4; 32]),
                resume_after_cursor: 30,
            }
        );

        let stale = input_with_checkpoint(42, 30, 50, 30).with_proof(proof(41, 7));
        assert_eq!(
            plan_derived_index_catch_up(stale, KEY).unwrap(),
            DerivedIndexCatchUpPlan::RebuildFromManifest {
                reason: DerivedIndexRebuildReason::StaleProof,
                manifest_checkpoint_cursor: 30,
                source_manifest_hash: hex::encode([4; 32]),
                resume_after_cursor: 30,
            }
        );
    }

    #[tokio::test]
    async fn catch_up_uses_checkpoint_carried_by_handed_off_task_lease() {
        let temp = tempdir().unwrap();
        let storage = crate::storage::Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_task_lease(&storage, lease_acquire("node-a", 10, 30, 10), KEY)
            .await
            .unwrap();
        checkpoint_task_lease(&storage, &first, 42, 20, KEY)
            .await
            .unwrap();
        let handed_off = acquire_task_lease(&storage, lease_acquire("node-b", 50, 100, 10), KEY)
            .await
            .unwrap();

        let plan = plan_derived_index_catch_up(
            input_with_checkpoint(handed_off.checkpoint_cursor, 30, 50, 30)
                .with_proof(proof(42, 7)),
            KEY,
        )
        .unwrap();

        assert_eq!(handed_off.fence_token, 2);
        assert_eq!(handed_off.checkpoint_cursor, 42);
        assert_eq!(
            plan,
            DerivedIndexCatchUpPlan::Replay {
                from_exclusive: 42,
                to_inclusive: 50,
                generation: 7,
            }
        );
    }

    fn input_with_checkpoint(
        checkpoint_cursor: u128,
        retained_start_cursor: u128,
        latest_cursor: u128,
        manifest_checkpoint_cursor: u128,
    ) -> DerivedIndexCatchUpInput {
        DerivedIndexCatchUpInput {
            index_id: "search-index-a".to_string(),
            consumer_id: "search-index-builder".to_string(),
            watch_stream_id: "object-prefix".to_string(),
            checkpoint_cursor,
            retained_start_cursor,
            latest_cursor,
            manifest_checkpoint_cursor,
            source_manifest_hash: hex::encode([4; 32]),
            required_source_cursor: checkpoint_cursor,
            min_generation: 7,
            latest_proof: None,
        }
    }

    trait WithProof {
        fn with_proof(self, proof: DerivedIndexProof) -> Self;
    }

    impl WithProof for DerivedIndexCatchUpInput {
        fn with_proof(mut self, proof: DerivedIndexProof) -> Self {
            self.latest_proof = Some(proof);
            self
        }
    }

    fn proof(source_cursor: u128, generation: u64) -> DerivedIndexProof {
        DerivedIndexProof {
            format_version: 1,
            index_id: "search-index-a".to_string(),
            index_kind: "full_text".to_string(),
            partition_family: "full_text_index".to_string(),
            partition_id: hex::encode([8; 32]),
            source_watch_stream_id: "object-prefix".to_string(),
            source_cursor,
            source_manifest_hash: hex::encode([4; 32]),
            generation,
            segment_hashes: vec![hex::encode([5; 32])],
            built_by_node: "node-a".to_string(),
            built_at_nanos: 10,
            proof_hash: None,
            proof_signature: None,
        }
        .seal(KEY)
        .unwrap()
    }

    fn lease_acquire(
        owner_node_id: &str,
        now_nanos: i64,
        ttl_nanos: i64,
        source_cursor: u128,
    ) -> TaskLeaseAcquire {
        TaskLeaseAcquire {
            task_id: "search-index-builder".to_string(),
            task_kind: "index_build".to_string(),
            partition_family: "full_text_index".to_string(),
            partition_id: hex::encode([8; 32]),
            owner: TaskLeaseOwner::node(owner_node_id),
            source_cursor,
            now_nanos,
            ttl_nanos,
        }
    }
}
