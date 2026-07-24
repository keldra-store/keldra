use super::*;

pub(super) fn encode_intent_rows(intent: &RootPublicationIntent) -> Result<Vec<StoredIntentRow>> {
    let header = intent_header_proto(intent)?;
    let mut rows = vec![StoredIntentRow {
        tuple_key: intent_header_key(&intent.transaction_id)?,
        payload: encode_deterministic_proto(&header),
    }];
    for root in &intent.roots {
        rows.push(StoredIntentRow {
            tuple_key: intent_root_key(&intent.transaction_id, root.ordinal)?,
            payload: encode_deterministic_proto(&root_to_proto(intent, root)?),
        });
        encode_stored_rows(intent, "root", root.ordinal, &root.rows, &mut rows)?;
    }
    encode_stored_rows(intent, "local", u64::MAX, &intent.local_rows, &mut rows)?;
    Ok(rows)
}

pub(super) fn intent_header_proto(
    intent: &RootPublicationIntent,
) -> Result<PublicationIntentHeaderProto> {
    let coordinator = intent.coordinator_scope()?;
    Ok(PublicationIntentHeaderProto {
        common: Some(intent_common(intent)),
        schema: PUBLICATION_INTENT_SCHEMA.to_string(),
        transaction_id: intent.transaction_id.clone(),
        plan_hash: intent.plan_hash.clone(),
        created_at_unix_nanos: intent.created_at_unix_nanos,
        root_count: intent.roots.len() as u64,
        local_row_count: intent.local_rows.len() as u64,
        coordinator_root_key_hash: coordinator.as_ref().map(|(hash, _)| hash.clone()),
        coordinator_root_generation: coordinator.as_ref().map(|(_, generation)| *generation),
        publisher_node_id: intent.publisher_node_id.clone(),
        guard_context_hash: intent
            .guard
            .as_ref()
            .map(|guard| guard.context_hash.clone()),
        transaction_expires_at_unix_nanos: intent
            .guard
            .as_ref()
            .map_or(0, |guard| guard.transaction_expires_at_unix_nanos),
        guard_visible_update_count: intent
            .guard
            .as_ref()
            .map_or(0, |guard| guard.visible_update_count),
        guard_precondition_count: intent
            .guard
            .as_ref()
            .map_or(0, |guard| guard.precondition_count),
        state: publication_intent_state_to_proto(intent.state) as i32,
        terminal_reason: intent.terminal_reason.clone(),
    })
}

pub(super) fn root_to_proto(
    intent: &RootPublicationIntent,
    root: &RootPublicationIntentRoot,
) -> Result<PublicationRootProto> {
    Ok(PublicationRootProto {
        common: Some(intent_common(intent)),
        schema: PUBLICATION_ROOT_SCHEMA.to_string(),
        transaction_id: intent.transaction_id.clone(),
        ordinal: root.ordinal,
        root_anchor_key: root.publication.descriptor.root_anchor_key.clone(),
        root_key_hash: root.publication.descriptor.root_key_hash(),
        expected_root_generation: root.expected_root_generation,
        post_root_generation: root.publication.post_root_generation,
        transaction_coordinator: root.publication.descriptor.transaction_coordinator,
        writer_families: root.publication.descriptor.writer_families.clone(),
        logical_manifests: root
            .publication
            .descriptor
            .logical_manifests
            .iter()
            .map(|locator| {
                crate::core_store::transaction_manifest_proto::encode_manifest_locator_proto(
                    locator,
                )
            })
            .collect::<Result<Vec<_>>>()?,
        idempotency_key_hashes: root.publication.descriptor.idempotency_key_hashes.clone(),
        previous_root_hash: root.publication.previous_root_hash.clone(),
        transaction_manifest_locator:
            crate::core_store::transaction_manifest_proto::encode_manifest_locator_proto(
                &root.publication.transaction_manifest_locator,
            )?,
        transaction_manifest_row_hash: encoded_row_hash(&root.publication.transaction_manifest_row),
        created_at_unix_nanos: intent.created_at_unix_nanos,
        row_count: root.rows.len() as u64,
        rows_hash: rows_hash(&root.rows),
        certificate_hash: root.certificate_hash.clone(),
    })
}

fn encode_stored_rows(
    intent: &RootPublicationIntent,
    scope: &str,
    root_ordinal: u64,
    encoded_rows: &[CoreMetaEncodedOwnedRow],
    output: &mut Vec<StoredIntentRow>,
) -> Result<()> {
    for (row_index, encoded_row) in encoded_rows.iter().enumerate() {
        let row_ordinal = u64::try_from(row_index)
            .map_err(|_| anyhow!("CoreMeta publication row ordinal exceeds u64"))?;
        let stored = stored_row_proto(encoded_row);
        let encoded = encode_deterministic_proto(&stored);
        let chunks = encoded
            .chunks(PUBLICATION_ROW_CHUNK_BYTES)
            .collect::<Vec<_>>();
        let row_hash = encoded_row_hash(encoded_row);
        output.push(StoredIntentRow {
            tuple_key: intent_row_key(&intent.transaction_id, scope, root_ordinal, row_ordinal)?,
            payload: encode_deterministic_proto(&PublicationRowProto {
                common: Some(intent_common(intent)),
                schema: PUBLICATION_ROW_SCHEMA.to_string(),
                transaction_id: intent.transaction_id.clone(),
                scope: scope.to_string(),
                root_ordinal,
                row_ordinal,
                row_hash: row_hash.clone(),
                encoded_length: encoded.len() as u64,
                chunk_count: chunks.len() as u64,
            }),
        });
        let chunk_count = chunks.len() as u64;
        for (chunk_index, bytes) in chunks.into_iter().enumerate() {
            let chunk_ordinal = u64::try_from(chunk_index)
                .map_err(|_| anyhow!("CoreMeta publication chunk ordinal exceeds u64"))?;
            output.push(StoredIntentRow {
                tuple_key: intent_chunk_key(
                    &intent.transaction_id,
                    scope,
                    root_ordinal,
                    row_ordinal,
                    chunk_ordinal,
                )?,
                payload: encode_deterministic_proto(&PublicationChunkProto {
                    common: Some(intent_common(intent)),
                    schema: PUBLICATION_CHUNK_SCHEMA.to_string(),
                    transaction_id: intent.transaction_id.clone(),
                    scope: scope.to_string(),
                    root_ordinal,
                    row_ordinal,
                    chunk_ordinal,
                    chunk_count,
                    row_hash: row_hash.clone(),
                    bytes: bytes.to_vec(),
                }),
            });
        }
    }
    Ok(())
}

pub(super) fn write_stored_intent_rows(
    meta: &CoreMetaStore,
    rows: &[StoredIntentRow],
) -> Result<()> {
    let ops = rows
        .iter()
        .map(|row| CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
            tuple_key: row.tuple_key.as_slice(),
            common: None,
            kind: CoreMetaBatchOpKind::Put(row.payload.as_slice()),
        })
        .collect::<Vec<_>>();
    meta.write_local_committed_batch(&ops)
}

pub(super) fn validate_header(header: &PublicationIntentHeaderProto) -> Result<()> {
    validate_common(
        header.common.as_ref(),
        &header.transaction_id,
        header.created_at_unix_nanos,
    )?;
    if header.schema != PUBLICATION_INTENT_SCHEMA
        || header.root_count == 0
        || header.root_count > MAX_PUBLICATION_ROOTS as u64
        || header.local_row_count > MAX_PUBLICATION_ROWS as u64
    {
        bail!("CoreMeta root publication intent header is invalid");
    }
    validate_hash(&header.plan_hash, "CoreMeta publication intent plan hash")?;
    validate_logical_id(
        &header.publisher_node_id,
        "CoreMeta publication intent publisher node id",
    )?;
    let state = publication_intent_state_from_proto(header.state)?;
    match state {
        RootPublicationIntentState::Pending if header.terminal_reason.is_none() => {}
        RootPublicationIntentState::Terminal
            if header
                .terminal_reason
                .as_deref()
                .is_some_and(|reason| !reason.trim().is_empty()) => {}
        _ => bail!("CoreMeta publication intent terminal state is invalid"),
    }
    match header.guard_context_hash.as_deref() {
        Some(hash) => {
            validate_hash(hash, "CoreMeta publication guard context hash")?;
            if header.guard_visible_update_count > MAX_PUBLICATION_ROWS as u64
                || header.guard_precondition_count > MAX_PUBLICATION_ROWS as u64
            {
                bail!("CoreMeta publication guard context exceeds its bounded count");
            }
        }
        None if header.transaction_expires_at_unix_nanos == 0
            && header.guard_visible_update_count == 0
            && header.guard_precondition_count == 0 => {}
        None => bail!("CoreMeta publication guard summary is incomplete"),
    }
    match (
        header.coordinator_root_key_hash.as_deref(),
        header.coordinator_root_generation,
    ) {
        (Some(hash), Some(generation)) if generation > 0 => {
            validate_hash(hash, "CoreMeta publication coordinator root hash")?;
        }
        (None, None) => {}
        _ => bail!("CoreMeta publication intent coordinator scope is incomplete"),
    }
    Ok(())
}

pub(super) fn validate_root_proto(
    root: &PublicationRootProto,
    header: &PublicationIntentHeaderProto,
    ordinal: u64,
) -> Result<()> {
    validate_common(
        root.common.as_ref(),
        &header.transaction_id,
        header.created_at_unix_nanos,
    )?;
    if root.schema != PUBLICATION_ROOT_SCHEMA
        || root.transaction_id != header.transaction_id
        || root.ordinal != ordinal
        || root.created_at_unix_nanos != header.created_at_unix_nanos
        || root.post_root_generation != root.expected_root_generation.saturating_add(1)
        || root.row_count == 0
        || root.row_count > MAX_PUBLICATION_ROWS as u64
        || root.root_key_hash != root_key_hash(&root.root_anchor_key)
    {
        bail!("CoreMeta root publication root row is invalid");
    }
    validate_hash(&root.root_key_hash, "CoreMeta publication root hash")?;
    validate_hash(
        &root.previous_root_hash,
        "CoreMeta publication previous root hash",
    )?;
    validate_hash(&root.rows_hash, "CoreMeta publication rows hash")?;
    validate_blake3_hash(
        &root.transaction_manifest_row_hash,
        "CoreMeta publication transaction manifest row hash",
    )?;
    if let Some(hash) = &root.certificate_hash {
        validate_blake3_hash(hash, "CoreMeta publication certificate hash")?;
    }
    Ok(())
}

pub(super) fn validate_row_proto(
    row: &PublicationRowProto,
    header: &PublicationIntentHeaderProto,
    scope: &str,
    root_ordinal: u64,
    row_ordinal: u64,
) -> Result<()> {
    validate_common(
        row.common.as_ref(),
        &header.transaction_id,
        header.created_at_unix_nanos,
    )?;
    if row.schema != PUBLICATION_ROW_SCHEMA
        || row.transaction_id != header.transaction_id
        || row.scope != scope
        || row.root_ordinal != root_ordinal
        || row.row_ordinal != row_ordinal
        || row.chunk_count == 0
        || row.chunk_count > 8
    {
        bail!("CoreMeta publication encoded-row header is invalid");
    }
    validate_blake3_hash(&row.row_hash, "CoreMeta publication encoded-row hash")
}

pub(super) fn validate_chunk_proto(
    chunk: &PublicationChunkProto,
    header: &PublicationIntentHeaderProto,
    row: &PublicationRowProto,
    chunk_ordinal: u64,
    chunk_count: u64,
) -> Result<()> {
    validate_common(
        chunk.common.as_ref(),
        &header.transaction_id,
        header.created_at_unix_nanos,
    )?;
    if chunk.schema != PUBLICATION_CHUNK_SCHEMA
        || chunk.transaction_id != header.transaction_id
        || chunk.scope != row.scope
        || chunk.root_ordinal != row.root_ordinal
        || chunk.row_ordinal != row.row_ordinal
        || chunk.chunk_ordinal != chunk_ordinal
        || chunk.chunk_count != chunk_count
        || chunk.row_hash != row.row_hash
        || chunk.bytes.len() > PUBLICATION_ROW_CHUNK_BYTES
    {
        bail!("CoreMeta publication encoded-row chunk is invalid");
    }
    Ok(())
}

pub(super) fn validate_common(
    common: Option<&CoreMetaRowCommonProto>,
    transaction_id: &str,
    created_at_unix_nanos: u64,
) -> Result<()> {
    let common =
        common.ok_or_else(|| anyhow!("CoreMeta publication intent common metadata is missing"))?;
    if common.realm_id != "system/coremeta-publication-intent"
        || !common.root_key_hash.is_empty()
        || common.root_generation != 0
        || common.transaction_id != transaction_id
        || common.visibility_state_enum() != CoreMetaVisibilityState::Committed
        || common.created_at_unix_nanos != created_at_unix_nanos
        || created_at_unix_nanos == 0
    {
        bail!("CoreMeta publication intent common metadata is invalid");
    }
    Ok(())
}

pub(super) fn validate_intent_root(
    transaction_id: &str,
    created_at_unix_nanos: u64,
    root: &RootPublicationIntentRoot,
) -> Result<()> {
    root.publication.descriptor.validate()?;
    if root.publication.created_at_unix_nanos != created_at_unix_nanos
        || root.publication.post_root_generation != root.expected_root_generation.saturating_add(1)
        || root.rows.is_empty()
    {
        bail!("CoreMeta publication intent root is invalid");
    }
    validate_hash(
        &root.publication.previous_root_hash,
        "CoreMeta publication previous root hash",
    )?;
    let root_hash = root.publication.descriptor.root_key_hash();
    for row in &root.rows {
        if row.root_key_hash != root_hash
            || row.root_generation != root.publication.post_root_generation
            || row.visibility_state != CoreMetaVisibilityState::Committed
        {
            bail!("CoreMeta publication intent row root scope mismatch");
        }
    }
    if root.publication.transaction_manifest_row.root_key_hash != root_hash
        || root.publication.transaction_manifest_row.root_generation
            != root.publication.post_root_generation
    {
        bail!("CoreMeta publication intent manifest row scope mismatch");
    }
    let manifest = decode_transaction_manifest_body_from_encoded_row(
        &root.publication.transaction_manifest_row,
    )?;
    if manifest.root_key_hash != root_hash
        || manifest.post_root_generation != root.publication.post_root_generation
        || !manifest.mutation_ids.iter().any(|id| id == transaction_id)
    {
        bail!("CoreMeta publication intent manifest identity mismatch");
    }
    if let Some(hash) = &root.certificate_hash {
        validate_blake3_hash(hash, "CoreMeta publication certificate hash")?;
    }
    Ok(())
}

fn validate_blake3_hash(value: &str, label: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("blake3:") else {
        bail!("{label} must have blake3: prefix");
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        bail!("{label} must be a canonical lowercase BLAKE3 digest");
    }
    Ok(())
}

pub(super) fn validate_local_intent_row(row: &CoreMetaEncodedOwnedRow) -> Result<()> {
    if !row.root_key_hash.is_empty()
        || row.root_generation != 0
        || row.visibility_state != CoreMetaVisibilityState::Committed
    {
        bail!("CoreMeta publication local row is not local committed metadata");
    }
    Ok(())
}

fn decode_transaction_manifest_body_from_encoded_row(
    row: &CoreMetaEncodedOwnedRow,
) -> Result<CoreTransactionManifestRecord> {
    let table_id = u16::from_be_bytes([
        *row.core_meta_key
            .get(1)
            .ok_or_else(|| anyhow!("CoreMeta publication manifest key is truncated"))?,
        *row.core_meta_key
            .get(2)
            .ok_or_else(|| anyhow!("CoreMeta publication manifest key is truncated"))?,
    ]);
    if row.cf != CF_TRANSACTIONS || table_id != TABLE_TRANSACTION_MANIFEST_BODY_ROW {
        bail!("CoreMeta publication manifest row has invalid table scope");
    }
    let payload = decode_payload_from_value_envelope(&row.value_envelope)?;
    let (_, manifest_bytes) = decode_transaction_manifest_body_row_for_recovery(&payload)?;
    decode_transaction_manifest_record(&manifest_bytes)
}

fn decode_payload_from_value_envelope(value_envelope: &[u8]) -> Result<Vec<u8>> {
    #[derive(Clone, PartialEq, Message)]
    struct Envelope {
        #[prost(uint32, tag = "1")]
        table_id: u32,
        #[prost(uint32, tag = "2")]
        schema_version: u32,
        #[prost(bytes, tag = "3")]
        payload: Vec<u8>,
        #[prost(string, tag = "4")]
        payload_hash: String,
    }
    let envelope = decode_canonical::<Envelope>(value_envelope, "CoreMeta value envelope")?;
    if envelope.table_id != u32::from(TABLE_TRANSACTION_MANIFEST_BODY_ROW)
        || envelope.schema_version != 1
        || envelope.payload_hash
            != core_meta_payload_digest(TABLE_TRANSACTION_MANIFEST_BODY_ROW, &envelope.payload)
    {
        bail!("CoreMeta publication manifest value envelope is invalid");
    }
    Ok(envelope.payload)
}

fn decode_transaction_manifest_body_row_for_recovery(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, Vec<u8>)> {
    #[derive(Clone, PartialEq, Message)]
    struct Row {
        #[prost(message, optional, tag = "1")]
        common: Option<CoreMetaRowCommonProto>,
        #[prost(string, tag = "2")]
        schema: String,
        #[prost(string, tag = "3")]
        manifest_hash: String,
        #[prost(bytes, tag = "4")]
        manifest_bytes: Vec<u8>,
    }
    let row = decode_canonical::<Row>(payload, "CoreMeta transaction manifest body row")?;
    if row.schema != "anvil.core.transaction_manifest_body.v1"
        || row.manifest_hash != format!("sha256:{}", sha256_hex(&row.manifest_bytes))
    {
        bail!("CoreMeta publication transaction manifest body row is invalid");
    }
    Ok((
        row.common
            .ok_or_else(|| anyhow!("CoreMeta transaction manifest row common is missing"))?,
        row.manifest_bytes,
    ))
}

pub(super) fn plan_hash_from_intent(intent: &RootPublicationIntent) -> Result<String> {
    let roots = intent
        .roots
        .iter()
        .map(|root| {
            let manifest_hash = encoded_row_hash(&root.publication.transaction_manifest_row);
            let rows = root
                .rows
                .iter()
                .filter(|row| encoded_row_hash(row) != manifest_hash)
                .cloned()
                .collect::<Vec<_>>();
            (root.publication.descriptor.clone(), rows)
        })
        .collect::<Vec<_>>();
    root_publication_plan_hash(&intent.transaction_id, &roots, &intent.local_rows)
}

pub(super) fn rows_hash(rows: &[CoreMetaEncodedOwnedRow]) -> String {
    let mut hashes = rows.iter().map(encoded_row_hash).collect::<Vec<_>>();
    hashes.sort();
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, b"anvil.core.root_publication_rows.v1");
    for hash in hashes {
        append_hash_part(&mut bytes, hash.as_bytes());
    }
    format!("sha256:{}", sha256_hex(&bytes))
}

pub(super) fn encoded_row_hash(row: &CoreMetaEncodedOwnedRow) -> String {
    core_meta_encoded_row_hash_with_delete(
        &row.cf,
        &row.core_meta_key,
        &row.value_envelope,
        row.delete_marker,
    )
}

pub(super) fn sort_encoded_rows(rows: &mut [CoreMetaEncodedOwnedRow]) {
    rows.sort_by(|left, right| {
        left.cf
            .cmp(&right.cf)
            .then_with(|| left.core_meta_key.cmp(&right.core_meta_key))
            .then_with(|| left.delete_marker.cmp(&right.delete_marker))
            .then_with(|| left.value_envelope.cmp(&right.value_envelope))
    });
}

pub(super) fn borrow_encoded_rows(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaEncodedRow<'_>> {
    rows.iter()
        .map(|row| CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        })
        .collect()
}

pub(super) fn owned_row_from_proto(row: StoredEncodedRowProto) -> Result<CoreMetaEncodedOwnedRow> {
    if row.schema != STORED_ROW_SCHEMA {
        bail!("CoreMeta publication stored row has invalid schema");
    }
    let visibility_state = CoreMetaVisibilityState::try_from(row.visibility_state)
        .map_err(|_| anyhow!("CoreMeta publication stored row visibility is invalid"))?;
    Ok(CoreMetaEncodedOwnedRow {
        cf: row.cf,
        core_meta_key: row.core_meta_key,
        value_envelope: row.value_envelope,
        delete_marker: row.delete_marker,
        root_key_hash: row.root_key_hash,
        root_generation: row.root_generation,
        visibility_state,
    })
}

pub(super) fn stored_row_proto(row: &CoreMetaEncodedOwnedRow) -> StoredEncodedRowProto {
    StoredEncodedRowProto {
        schema: STORED_ROW_SCHEMA.to_string(),
        cf: row.cf.clone(),
        core_meta_key: row.core_meta_key.clone(),
        value_envelope: row.value_envelope.clone(),
        delete_marker: row.delete_marker,
        root_key_hash: row.root_key_hash.clone(),
        root_generation: row.root_generation,
        visibility_state: row.visibility_state as i32,
    }
}

fn intent_common(intent: &RootPublicationIntent) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        "system/coremeta-publication-intent",
        "",
        0,
        &intent.transaction_id,
        intent.created_at_unix_nanos,
    )
}

pub(super) fn append_hash_part(output: &mut Vec<u8>, value: &[u8]) {
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
}

pub(super) fn publication_intent_state_to_proto(
    state: RootPublicationIntentState,
) -> PublicationIntentStateProto {
    match state {
        RootPublicationIntentState::Pending => PublicationIntentStateProto::Pending,
        RootPublicationIntentState::Terminal => PublicationIntentStateProto::Terminal,
    }
}

pub(super) fn publication_intent_state_from_proto(
    state: i32,
) -> Result<RootPublicationIntentState> {
    match PublicationIntentStateProto::try_from(state)
        .map_err(|_| anyhow!("CoreMeta publication intent state is invalid"))?
    {
        PublicationIntentStateProto::Pending => Ok(RootPublicationIntentState::Pending),
        PublicationIntentStateProto::Terminal => Ok(RootPublicationIntentState::Terminal),
        PublicationIntentStateProto::Unspecified => {
            bail!("CoreMeta publication intent state is unspecified")
        }
    }
}

pub(super) fn decode_canonical<M>(payload: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(payload).with_context(|| format!("decode {label}"))?;
    let mut canonical = Vec::with_capacity(value.encoded_len());
    value.encode(&mut canonical)?;
    if canonical != payload {
        bail!("{label} is not deterministic protobuf");
    }
    Ok(value)
}

pub(super) fn usize_from_bounded(value: u64, maximum: usize, label: &str) -> Result<usize> {
    let value = usize::try_from(value).map_err(|_| anyhow!("{label} exceeds usize"))?;
    if value > maximum {
        bail!("{label} exceeds its bounded maximum");
    }
    Ok(value)
}

pub(super) fn intent_header_key(transaction_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-publication-intent"),
        CoreMetaTuplePart::Utf8(transaction_id),
    ])
}

pub(super) fn intent_root_key(transaction_id: &str, ordinal: u64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-publication-root"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

pub(super) fn intent_row_key(
    transaction_id: &str,
    scope: &str,
    root_ordinal: u64,
    row_ordinal: u64,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-publication-row"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8(scope),
        CoreMetaTuplePart::U64(root_ordinal),
        CoreMetaTuplePart::U64(row_ordinal),
    ])
}

pub(super) fn intent_chunk_key(
    transaction_id: &str,
    scope: &str,
    root_ordinal: u64,
    row_ordinal: u64,
    chunk_ordinal: u64,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-publication-chunk"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8(scope),
        CoreMetaTuplePart::U64(root_ordinal),
        CoreMetaTuplePart::U64(row_ordinal),
        CoreMetaTuplePart::U64(chunk_ordinal),
    ])
}
