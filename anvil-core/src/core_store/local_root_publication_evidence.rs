use super::local_root_failover::RootOwnerTerms;
use super::local_root_publication_recovery::{RootPublicationIntent, RootPublicationIntentRoot};
use super::*;
use crate::anvil_api::CoreMetaRootPublicationEvidence;

struct ValidatedParticipantEvidence {
    anchor: CoreRootAnchorRecord,
    anchor_bytes: Vec<u8>,
    certificate: CoreMetaCommitCertificate,
    outcome: CoreMetaQuorumCommitOutcome,
}

impl CoreStore {
    pub(crate) fn persist_replica_publication_certificate_evidence(
        &self,
        transaction_id: &str,
        rows_by_root: &BTreeMap<String, Vec<CoreMetaEncodedOwnedRow>>,
        evidence_rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<()> {
        let intent = self.validate_staged_publication_rows(transaction_id, rows_by_root)?;
        if evidence_rows.is_empty()
            || evidence_rows.iter().any(|row| {
                row.cf != CF_TRANSACTIONS
                    || core_meta_record_table_id(&row.core_meta_key).map_or(true, |table_id| {
                        table_id != TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW
                    })
            })
        {
            bail!("CoreMeta replica certificate persistence accepts only evidence rows");
        }
        if intent.all_outcomes_recorded() {
            return Ok(());
        }
        if !intent.no_outcomes_recorded() {
            bail!("CoreMeta publication intent has a torn quorum-outcome state");
        }
        self.write_coremeta_encoded_rows(&borrow_publication_rows(evidence_rows))
    }

    pub(crate) async fn install_root_publication_commit_evidence(
        &self,
        source_node_id: &str,
        transaction_id: &str,
        participants: &[CoreMetaRootPublicationEvidence],
    ) -> Result<Vec<Vec<u8>>> {
        self.install_root_publication_commit_evidence_with_authority(
            source_node_id,
            transaction_id,
            participants,
            super::local_root_publication_recovery::RootPublicationAuthority::LocalOwnerState,
        )
        .await
    }

    pub(crate) async fn install_recovered_root_publication_commit_evidence(
        &self,
        source_node_id: &str,
        transaction_id: &str,
        participants: &[CoreMetaRootPublicationEvidence],
    ) -> Result<Vec<Vec<u8>>> {
        self.install_root_publication_commit_evidence_with_authority(
            source_node_id,
            transaction_id,
            participants,
            super::local_root_publication_recovery::RootPublicationAuthority::RegisterQuorum,
        )
        .await
    }

    async fn install_root_publication_commit_evidence_with_authority(
        &self,
        source_node_id: &str,
        transaction_id: &str,
        participants: &[CoreMetaRootPublicationEvidence],
        authority: super::local_root_publication_recovery::RootPublicationAuthority,
    ) -> Result<Vec<Vec<u8>>> {
        validate_logical_id(source_node_id, "CoreMeta publication source node id")?;
        validate_logical_id(transaction_id, "CoreMeta publication transaction id")?;
        if participants.is_empty() || participants.len() > CORE_META_MAX_SCAN_PAGE_ROWS {
            bail!("CoreMeta publication evidence count is outside the bounded range");
        }

        let mut validated = participants
            .iter()
            .map(|participant| {
                self.validate_root_publication_participant(transaction_id, participant)
            })
            .collect::<Result<Vec<_>>>()?;
        validated.sort_by(|left, right| left.anchor.root_key_hash.cmp(&right.anchor.root_key_hash));
        if validated
            .windows(2)
            .any(|pair| pair[0].anchor.root_key_hash == pair[1].anchor.root_key_hash)
        {
            bail!("CoreMeta publication evidence contains a duplicate root");
        }
        if authority
            == super::local_root_publication_recovery::RootPublicationAuthority::LocalOwnerState
            && validated
                .iter()
                .any(|participant| participant.anchor.publisher_node_id != source_node_id)
        {
            bail!("CoreMeta publication evidence was not sent by its publisher");
        }

        let Some(intent) = self.read_root_publication_intent(transaction_id)? else {
            for participant in &validated {
                let current = self
                    .read_internal_root_anchor_by_hash(
                        &participant.anchor.root_key_hash,
                        participant.anchor.root_generation,
                    )
                    .await?;
                if current.root_anchor_record != participant.anchor_bytes {
                    bail!("CoreMeta completed publication retry changed a participant root");
                }
            }
            return Ok(validated
                .into_iter()
                .map(|participant| participant.anchor_bytes)
                .collect());
        };
        if intent.roots.len() != validated.len() {
            bail!("CoreMeta publication evidence root cardinality mismatch");
        }

        let by_root = intent
            .roots
            .iter()
            .map(|root| (root.publication.descriptor.root_key_hash(), root))
            .collect::<BTreeMap<_, _>>();
        for participant in &validated {
            let root = by_root
                .get(&participant.anchor.root_key_hash)
                .ok_or_else(|| anyhow!("CoreMeta publication evidence references unknown root"))?;
            self.validate_participant_against_intent(
                &intent,
                root,
                participant,
                source_node_id,
                authority,
            )?;
        }

        let outcomes = validated
            .iter()
            .map(|participant| participant.outcome.clone())
            .collect::<Vec<_>>();
        if intent.no_outcomes_recorded() {
            self.record_root_publication_outcomes(&intent, &outcomes)?;
        } else if intent.all_outcomes_recorded() {
            let existing = self.root_publication_outcomes(&intent)?;
            validate_same_publication_outcomes(&existing, &outcomes)?;
        } else {
            bail!("CoreMeta publication intent has a torn quorum-outcome state");
        }

        Ok(validated
            .into_iter()
            .map(|participant| participant.anchor_bytes)
            .collect())
    }

    fn validate_root_publication_participant(
        &self,
        transaction_id: &str,
        participant: &CoreMetaRootPublicationEvidence,
    ) -> Result<ValidatedParticipantEvidence> {
        let anchor = decode_root_anchor_record(&participant.root_anchor_record)?;
        validate_root_anchor_record(&anchor)?;
        if super::local_root_publication_recovery::publication_transaction_id(&anchor)?
            != transaction_id
        {
            bail!("CoreMeta publication participant transaction mismatch");
        }
        let certificate_api = participant
            .commit_certificate
            .clone()
            .ok_or_else(|| anyhow!("CoreMeta publication participant certificate is missing"))?;
        let certificate = api_commit_certificate_to_core(certificate_api.clone())?;
        let receipts = participant
            .certificate_persist_receipts
            .iter()
            .cloned()
            .map(api_persist_receipt_to_core)
            .collect::<Result<Vec<_>>>()?;
        validate_commit_evidence_with_verifier(
            &self.default_coremeta_quorum_profile()?,
            &certificate,
            &receipts,
            |node_id, signed_payload_hash, signature| {
                self.verify_internal_core_receipt_signature(node_id, signed_payload_hash, signature)
            },
        )?;
        let mut receipt_hashes = receipts
            .iter()
            .map(certificate_persist_receipt_payload_hash)
            .collect::<Result<Vec<_>>>()?;
        receipt_hashes.sort();
        receipt_hashes.dedup();
        let mut anchor_receipt_hashes = anchor.certificate_persist_receipt_hashes.clone();
        anchor_receipt_hashes.sort();
        anchor_receipt_hashes.dedup();
        if certificate.transaction_id != transaction_id
            || certificate.root_key_hash != anchor.root_key_hash
            || certificate.post_root_generation != anchor.root_generation
            || anchor.core_meta_commit_certificate_hash.as_deref()
                != Some(certificate.certificate_hash.as_str())
            || anchor_receipt_hashes != receipt_hashes
            || receipts
                .iter()
                .any(|receipt| receipt.committed_batch_hash != participant.committed_batch_hash)
        {
            bail!("CoreMeta publication participant evidence scope mismatch");
        }
        let mut metadata_replica_node_ids = receipts
            .iter()
            .map(|receipt| receipt.replica_node_id.clone())
            .collect::<Vec<_>>();
        metadata_replica_node_ids.sort();
        metadata_replica_node_ids.dedup();
        Ok(ValidatedParticipantEvidence {
            anchor,
            anchor_bytes: participant.root_anchor_record.clone(),
            certificate: certificate.clone(),
            outcome: CoreMetaQuorumCommitOutcome {
                root_key_hash: certificate.root_key_hash,
                post_root_generation: certificate.post_root_generation,
                certificate_hash: certificate.certificate_hash,
                committed_batch_hash: participant.committed_batch_hash.clone(),
                certificate_bytes: encode_deterministic_proto(&certificate_api),
                certificate_persist_receipt_hashes: receipt_hashes,
                certificate_persist_receipts: receipts,
                metadata_replica_node_ids,
            },
        })
    }

    fn validate_participant_against_intent(
        &self,
        intent: &RootPublicationIntent,
        root: &RootPublicationIntentRoot,
        participant: &ValidatedParticipantEvidence,
        source_node_id: &str,
        authority: super::local_root_publication_recovery::RootPublicationAuthority,
    ) -> Result<()> {
        let row_hashes = publication_row_hashes(&root.rows);
        let expected_pending_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
            root_key_hash: participant.anchor.root_key_hash.clone(),
            expected_root_generation: root.expected_root_generation,
            post_root_generation: root.publication.post_root_generation,
            transaction_id: intent.transaction_id.clone(),
            row_hashes: row_hashes.clone(),
        })?;
        let expected_committed_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
            root_key_hash: participant.anchor.root_key_hash.clone(),
            expected_root_generation: root.expected_root_generation,
            post_root_generation: root.publication.post_root_generation,
            transaction_id: intent.transaction_id.clone(),
            pending_batch_hash: expected_pending_hash.clone(),
            committed_row_hashes: row_hashes,
        })?;
        if participant.certificate.expected_root_generation != root.expected_root_generation
            || participant.certificate.post_root_generation != root.publication.post_root_generation
            || participant.certificate.pending_batch_hash != expected_pending_hash
            || participant.outcome.committed_batch_hash != expected_committed_hash
        {
            bail!("CoreMeta publication evidence does not match staged candidate rows");
        }
        let expected_anchor = match authority {
            super::local_root_publication_recovery::RootPublicationAuthority::LocalOwnerState => {
                self.prepared_root_anchor_for_publisher(
                    &root.publication,
                    &participant.outcome,
                    &intent.transaction_id,
                    source_node_id,
                )?
            }
            super::local_root_publication_recovery::RootPublicationAuthority::RegisterQuorum => {
                self.prepared_root_anchor_with_owner_terms(
                    &root.publication,
                    &participant.outcome,
                    &intent.transaction_id,
                    RootOwnerTerms {
                        owner_node_id: participant.anchor.publisher_node_id.clone(),
                        owner_epoch: participant.anchor.publisher_epoch,
                        owner_fence: participant.anchor.partition_owner_fence,
                    },
                )?
            }
        };
        if encode_root_anchor_record(&expected_anchor)? != participant.anchor_bytes {
            bail!("CoreMeta publication participant anchor bytes changed after staging");
        }
        Ok(())
    }
}

fn borrow_publication_rows(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaEncodedRow<'_>> {
    rows.iter()
        .map(|row| CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        })
        .collect()
}

fn publication_row_hashes(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<String> {
    let mut hashes = rows
        .iter()
        .map(|row| {
            core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            )
        })
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    hashes
}

fn validate_same_publication_outcomes(
    existing: &[CoreMetaQuorumCommitOutcome],
    supplied: &[CoreMetaQuorumCommitOutcome],
) -> Result<()> {
    if existing.len() != supplied.len() {
        bail!("CoreMeta publication retry changed outcome cardinality");
    }
    for (existing, supplied) in existing.iter().zip(supplied) {
        if existing.root_key_hash != supplied.root_key_hash
            || existing.post_root_generation != supplied.post_root_generation
            || existing.certificate_hash != supplied.certificate_hash
            || existing.committed_batch_hash != supplied.committed_batch_hash
            || existing.certificate_bytes != supplied.certificate_bytes
            || existing.certificate_persist_receipt_hashes
                != supplied.certificate_persist_receipt_hashes
        {
            bail!("CoreMeta publication retry changed quorum evidence");
        }
    }
    Ok(())
}
