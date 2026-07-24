use super::*;

impl CoreStore {
    pub(crate) fn stage_replica_root_publication_intent(
        &self,
        encoded_intent: &[u8],
        rows_by_root: &BTreeMap<String, Vec<CoreMetaEncodedOwnedRow>>,
    ) -> Result<()> {
        let intent = decode_replica_root_publication_intent(encoded_intent, rows_by_root)?;
        self.persist_root_publication_intent(&intent)
    }

    pub(in crate::core_store::local) fn stage_committed_replica_root_publication_intent(
        &self,
        encoded_intent: &[u8],
        rows_by_root: &BTreeMap<String, Vec<CoreMetaEncodedOwnedRow>>,
        committed_anchors: &BTreeMap<(String, u64), Vec<u8>>,
    ) -> Result<()> {
        let intent = decode_replica_root_publication_intent(encoded_intent, rows_by_root)?;
        let Some(existing) = self.read_root_publication_intent(&intent.transaction_id)? else {
            return self.persist_root_publication_intent(&intent);
        };
        if publication_intent_retry_matches(&existing, &intent)? {
            let committed_certificates = committed_intent_certificates(&intent, committed_anchors)?;
            if existing.state == RootPublicationIntentState::Pending
                && existing.no_outcomes_recorded()
            {
                return Ok(());
            }
            if existing.state == RootPublicationIntentState::Pending
                && existing.all_outcomes_recorded()
                && intent_outcomes_match(&existing, &committed_certificates)
            {
                return Ok(());
            }
            self.validate_committed_outcome_replacement(&existing, &committed_certificates)?;
            return replace_stored_intent_rows(&self.meta, &existing, &intent);
        }
        self.validate_committed_direct_stream_intent_replacement(
            &existing,
            &intent,
            committed_anchors,
        )?;
        replace_stored_intent_rows(&self.meta, &existing, &intent)
    }

    fn validate_committed_outcome_replacement(
        &self,
        existing: &RootPublicationIntent,
        committed_certificates: &BTreeMap<(String, u64), String>,
    ) -> Result<()> {
        // A physical root-register quorum is the commit decision. Local
        // deadline, guard, or partial-outcome state recorded before this node
        // learned that decision cannot revoke it. Immutable published history
        // remains the only local state which may reject the replacement.
        for root in &existing.roots {
            let scope = (
                root.publication.descriptor.root_key_hash(),
                root.publication.post_root_generation,
            );
            let committed_certificate = committed_certificates
                .get(&scope)
                .ok_or_else(|| anyhow!("CoreMeta committed outcome authority is missing"))?;
            if !self.root_generation_is_published(&scope.0, scope.1, &existing.transaction_id)? {
                continue;
            }
            let published = self
                .read_complete_coremeta_generation_for_recovery(&scope.0, scope.1)?
                .ok_or_else(|| anyhow!("CoreMeta published generation history is missing"))?;
            if published.descriptor.certificate_hash != *committed_certificate {
                bail!("CoreMeta immutable history conflicts with committed root authority");
            }
        }
        Ok(())
    }

    fn validate_committed_direct_stream_intent_replacement(
        &self,
        existing: &RootPublicationIntent,
        replacement: &RootPublicationIntent,
        committed_anchors: &BTreeMap<(String, u64), Vec<u8>>,
    ) -> Result<()> {
        // Terminal is only a local candidate state. Once a different candidate
        // has physical Q2 for this generation, its exact committed bundle must
        // replace the loser regardless of why the loser was terminalised.
        let existing_scope = self
            .validate_direct_stream_publication_intent(existing)?
            .ok_or_else(|| anyhow!("existing publication is not a direct stream candidate"))?;
        let replacement_scope = self
            .validate_direct_stream_publication_intent(replacement)?
            .ok_or_else(|| anyhow!("committed publication is not a direct stream candidate"))?;
        if existing_scope != replacement_scope {
            bail!("committed direct stream publication changed its candidate range");
        }
        let existing_root = existing
            .roots
            .first()
            .ok_or_else(|| anyhow!("existing direct stream publication has no root"))?;
        let root = replacement
            .roots
            .first()
            .ok_or_else(|| anyhow!("committed direct stream publication has no root"))?;
        if existing_root.publication.descriptor.root_key_hash()
            != root.publication.descriptor.root_key_hash()
            || existing_root.publication.post_root_generation
                != root.publication.post_root_generation
            || existing_root.expected_root_generation != root.expected_root_generation
        {
            bail!("committed direct stream publication changed its physical root scope");
        }
        let scope = (
            root.publication.descriptor.root_key_hash(),
            root.publication.post_root_generation,
        );
        let anchor_bytes = committed_anchors
            .get(&scope)
            .ok_or_else(|| anyhow!("direct stream replacement has no committed anchor"))?;
        let anchor = decode_root_anchor_record(anchor_bytes)?;
        if anchor.root_key_hash != scope.0
            || anchor.root_generation != scope.1
            || publication_transaction_id(&anchor)? != replacement.transaction_id
        {
            bail!("direct stream replacement does not match committed root authority");
        }
        if let Some(certificate_hash) = root.certificate_hash.as_deref()
            && anchor.core_meta_commit_certificate_hash.as_deref() != Some(certificate_hash)
        {
            bail!("direct stream replacement certificate is not committed by its root anchor");
        }
        Ok(())
    }

    pub(super) fn mark_superseded_publication_if_still_current(
        &self,
        superseded: &RootPublicationIntent,
    ) -> Result<()> {
        let Some(current) = self.read_root_publication_intent(&superseded.transaction_id)? else {
            return Ok(());
        };
        if publication_intent_retry_matches(&current, superseded)? {
            self.mark_root_publication_intent_terminal(
                &current,
                "PublicationSupersededByCommittedRoot",
            )?;
        }
        Ok(())
    }
}

fn committed_intent_certificates(
    intent: &RootPublicationIntent,
    committed_anchors: &BTreeMap<(String, u64), Vec<u8>>,
) -> Result<BTreeMap<(String, u64), String>> {
    let mut certificates = BTreeMap::new();
    for root in &intent.roots {
        let scope = (
            root.publication.descriptor.root_key_hash(),
            root.publication.post_root_generation,
        );
        let anchor_bytes = committed_anchors
            .get(&scope)
            .ok_or_else(|| anyhow!("CoreMeta committed publication anchor is missing"))?;
        let anchor = decode_root_anchor_record(anchor_bytes)?;
        validate_root_anchor_record(&anchor)?;
        if anchor.root_key_hash != scope.0
            || anchor.root_generation != scope.1
            || publication_transaction_id(&anchor)? != intent.transaction_id
        {
            bail!("CoreMeta committed publication anchor changed its intent scope");
        }
        let certificate_hash = anchor
            .core_meta_commit_certificate_hash
            .ok_or_else(|| anyhow!("CoreMeta committed publication anchor has no certificate"))?;
        certificates.insert(scope, certificate_hash);
    }
    Ok(certificates)
}

fn intent_outcomes_match(
    intent: &RootPublicationIntent,
    committed_certificates: &BTreeMap<(String, u64), String>,
) -> bool {
    intent.roots.iter().all(|root| {
        let scope = (
            root.publication.descriptor.root_key_hash(),
            root.publication.post_root_generation,
        );
        root.certificate_hash.as_ref() == committed_certificates.get(&scope)
    })
}

fn decode_replica_root_publication_intent(
    encoded_intent: &[u8],
    rows_by_root: &BTreeMap<String, Vec<CoreMetaEncodedOwnedRow>>,
) -> Result<RootPublicationIntent> {
    let proto = decode_canonical::<ReplicaPublicationIntentProto>(
        encoded_intent,
        "CoreMeta replica publication intent",
    )?;
    if proto.schema != REPLICA_INTENT_SCHEMA || proto.roots.is_empty() {
        bail!("CoreMeta replica publication intent is invalid");
    }
    let local_rows = proto
        .local_rows
        .into_iter()
        .map(owned_row_from_proto)
        .collect::<Result<Vec<_>>>()?;
    let mut roots = Vec::with_capacity(proto.roots.len());
    for root in proto.roots {
        let rows = rows_by_root
            .get(&root.root_key_hash)
            .cloned()
            .ok_or_else(|| anyhow!("CoreMeta replica publication intent is missing rows"))?;
        if rows_hash(&rows) != root.rows_hash {
            bail!("CoreMeta replica publication rows hash mismatch");
        }
        let manifest_row = rows
            .iter()
            .find(|row| encoded_row_hash(row) == root.transaction_manifest_row_hash)
            .cloned()
            .ok_or_else(|| anyhow!("CoreMeta replica publication manifest row is missing"))?;
        let prepared = PreparedRootPublication {
            descriptor: CoreMetaRootPublication {
                root_anchor_key: root.root_anchor_key,
                writer_families: root.writer_families,
                logical_manifests: root
                    .logical_manifests
                    .into_iter()
                    .map(|bytes| {
                        crate::core_store::transaction_manifest_proto::decode_manifest_locator_proto(
                            &bytes,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
                idempotency_key_hashes: root.idempotency_key_hashes,
                transaction_coordinator: root.transaction_coordinator,
            },
            previous_root_hash: root.previous_root_hash,
            transaction_manifest_locator:
                crate::core_store::transaction_manifest_proto::decode_manifest_locator_proto(
                    &root.transaction_manifest_locator,
                )?,
            transaction_manifest_row: manifest_row,
            post_root_generation: root.post_root_generation,
            created_at_unix_nanos: root.created_at_unix_nanos,
        };
        roots.push(RootPublicationIntentRoot {
            ordinal: root.ordinal,
            publication: prepared,
            expected_root_generation: root.expected_root_generation,
            rows,
            certificate_hash: root.certificate_hash,
        });
    }
    if roots.len() != rows_by_root.len() {
        bail!("CoreMeta replica publication intent has unexpected root rows");
    }
    let intent = build_root_publication_intent(
        &proto.transaction_id,
        proto.plan_hash,
        proto.publisher_node_id,
        proto.created_at_unix_nanos,
        roots,
        local_rows,
    )?;
    let guard = intent.guard.as_ref();
    if proto.guard_context_hash.as_deref() != guard.map(|guard| guard.context_hash.as_str())
        || proto.transaction_expires_at_unix_nanos
            != guard.map_or(0, |guard| guard.transaction_expires_at_unix_nanos)
        || proto.guard_visible_update_count != guard.map_or(0, |guard| guard.visible_update_count)
        || proto.guard_precondition_count != guard.map_or(0, |guard| guard.precondition_count)
    {
        bail!("CoreMeta replica publication guard summary mismatch");
    }
    if plan_hash_from_intent(&intent)? != intent.plan_hash {
        bail!("CoreMeta replica publication bundle plan hash mismatch");
    }
    Ok(intent)
}

fn replace_stored_intent_rows(
    meta: &CoreMetaStore,
    existing: &RootPublicationIntent,
    replacement: &RootPublicationIntent,
) -> Result<()> {
    let existing_rows = encode_intent_rows(existing)?;
    let replacement_rows = encode_intent_rows(replacement)?;
    let mut ops = Vec::with_capacity(existing_rows.len().saturating_add(replacement_rows.len()));
    ops.extend(existing_rows.iter().map(|row| CoreMetaBatchOp {
        cf: CF_TRANSACTIONS,
        table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
        tuple_key: row.tuple_key.as_slice(),
        common: None,
        kind: CoreMetaBatchOpKind::Delete,
    }));
    ops.extend(replacement_rows.iter().map(|row| CoreMetaBatchOp {
        cf: CF_TRANSACTIONS,
        table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
        tuple_key: row.tuple_key.as_slice(),
        common: None,
        kind: CoreMetaBatchOpKind::Put(row.payload.as_slice()),
    }));
    meta.write_local_committed_batch(&ops)
}
