use super::local_mutation_commit::validate_core_meta_row_precondition;
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamWatchVisibility {
    Visible,
    Pending,
    TerminalInvisible,
}

impl CoreStore {
    pub(super) async fn stream_record_watch_visibility(
        &self,
        _record: &StreamRecord,
    ) -> Result<StreamWatchVisibility> {
        Ok(StreamWatchVisibility::Visible)
    }

    pub(super) async fn stream_record_identity_is_visible(
        &self,
        _stream_id: &str,
        _sequence: u64,
        _event_hash: &str,
        _transaction_id: Option<&str>,
    ) -> Result<bool> {
        Ok(true)
    }

    pub(super) async fn filter_committed_stream_records(
        &self,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>> {
        Ok(records)
    }

    pub(super) fn committed_coremeta_payload_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.read_coremeta_row(canonical_coremeta_cf_name(cf)?, table_id, tuple_key)
    }

    pub(super) async fn validate_mutation_preconditions_unlocked(
        &self,
        preconditions: &[CoreMutationPrecondition],
        committed_by_principal: &str,
    ) -> Result<()> {
        for precondition in preconditions {
            match precondition {
                CoreMutationPrecondition::Fence {
                    fence_name,
                    fence_token,
                } => {
                    self.validate_fence_precondition_unlocked(&CoreFencePrecondition {
                        fence_name: fence_name.clone(),
                        fence_token: *fence_token,
                        authenticated_principal: committed_by_principal.to_string(),
                    })
                    .await?;
                }
                CoreMutationPrecondition::CoreMetaRow {
                    cf,
                    table_id,
                    tuple_key,
                    expected_payload_hash,
                    require_absent,
                    require_present,
                } => {
                    let current =
                        self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?;
                    validate_core_meta_row_precondition(
                        current.as_deref(),
                        cf,
                        *table_id,
                        tuple_key,
                        expected_payload_hash.as_deref(),
                        *require_absent,
                        *require_present,
                    )?;
                }
                CoreMutationPrecondition::CoreMetaLease {
                    cf,
                    table_id,
                    tuple_key,
                    expected_payload_hash,
                    expires_at_unix_nanos,
                } => {
                    if *expires_at_unix_nanos == 0
                        || current_unix_nanos_u64()? >= *expires_at_unix_nanos
                    {
                        return Err(CoreStoreCommitError::CoreMetaRowPreconditionFailed {
                            cf: cf.clone(),
                            table_id: *table_id,
                            tuple_key_hex: hex::encode(tuple_key),
                            reason: "lease expired before commit admission".to_string(),
                        }
                        .into());
                    }
                    let current =
                        self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?;
                    validate_core_meta_row_precondition(
                        current.as_deref(),
                        cf,
                        *table_id,
                        tuple_key,
                        Some(expected_payload_hash),
                        false,
                        true,
                    )?;
                }
                CoreMutationPrecondition::StreamHead {
                    stream_id,
                    expected_last_sequence,
                    expected_last_event_hash,
                } => {
                    let (actual_sequence, actual_hash) = self.stream_head_unlocked(stream_id)?;
                    if actual_sequence != *expected_last_sequence
                        || actual_hash != *expected_last_event_hash
                    {
                        return Err(CoreStoreCommitError::StreamHeadMismatch {
                            stream_id: stream_id.clone(),
                            expected_last_sequence: *expected_last_sequence,
                            expected_last_event_hash: expected_last_event_hash.clone(),
                            actual_sequence,
                            actual_event_hash: actual_hash,
                        }
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn validate_source_watch_cursor_unlocked(&self, cursor: &str) -> Result<()> {
        let (stream_id, sequence) = parse_stream_cursor(cursor)?;
        let Some(record) = self
            .read_stream(ReadStream {
                stream_id,
                after_sequence: sequence.saturating_sub(1),
                limit: 1,
            })
            .await?
            .into_iter()
            .next()
        else {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        };
        if record.cursor != cursor {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        }
        Ok(())
    }

    pub(super) async fn validate_fence_precondition_unlocked(
        &self,
        precondition: &CoreFencePrecondition,
    ) -> Result<()> {
        validate_logical_id(&precondition.fence_name, "fence name")?;
        validate_logical_id(
            &precondition.authenticated_principal,
            "fence authenticated principal",
        )?;
        let Some(record) = super::local_stream_control::read_core_fence_current_row(
            self,
            &precondition.fence_name,
        )?
        else {
            bail!("CoreStore fence {} is not held", precondition.fence_name);
        };
        if record.owner_principal != precondition.authenticated_principal
            || record.fence_token != precondition.fence_token
            || record.expires_at_ms <= Utc::now().timestamp_millis()
        {
            bail!(
                "CoreStore fence {} precondition failed",
                precondition.fence_name
            );
        }
        Ok(())
    }
}
