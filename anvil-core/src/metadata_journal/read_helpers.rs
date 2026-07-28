use super::*;

pub(super) async fn current_directory_entries_from_index(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut directory_records = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(mvcc, bucket)? {
        let (manifest, recovered_directory) =
            recover_object_directory_partition(storage, mvcc, bucket, manifest_signing_key)
                .await
                .context("recover object directory partition from CoreStore manifest")?;
        compacted_through_sequence = manifest.compacted_through_sequence;
        directory_records.extend(recovered_directory);
    }

    for record in read_all_metadata_journal_records(mvcc, bucket)? {
        if record.partition_sequence <= compacted_through_sequence {
            continue;
        }
        let body = directory_entry_from_object_version_body(&record.object_version_body()?);
        directory_records.insert(directory_segment_key(&body), body);
    }
    Ok(directory_records)
}

pub(super) async fn expected_directory_entries_from_metadata(
    storage: &Storage,
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    directory_entries_from_object_version_bodies(
        read_object_version_bodies_from_metadata_only(storage, mvcc, bucket, manifest_signing_key)
            .await?,
    )
}

fn directory_entries_from_object_version_bodies(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut versions_by_key = object_versions_by_key(body_records);
    let mut entries = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key(versions);
        if let Some((_, body)) = versions.last() {
            let directory = directory_entry_from_object_version_body(body);
            entries.insert(directory_segment_key(&directory), directory);
        }
    }
    Ok(entries)
}

pub(super) fn directory_entry_from_object_version_body(
    body: &ObjectVersionBody,
) -> DirectoryEntryBody {
    DirectoryEntryBody {
        fence_token: body.fence_token,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        object_key: body.object_key.clone(),
        event: body.event.clone(),
        kind: body.kind,
        id: body.id,
        version_id: body.version_id.clone(),
        mutation_id: body.mutation_id.clone(),
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        record_hash: body.record_hash.clone(),
        storage_class: body.storage_class.clone(),
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: body.checksum.clone(),
        link: body.link.clone(),
        delete_marker: body.delete_marker,
        created_at: body.created_at.clone(),
        deleted_at: body.deleted_at.clone(),
    }
}

pub(super) fn directory_index_snapshot(
    entries: &std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>,
) -> Result<DirectoryIndexSnapshot> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.directory_index.snapshot.v1");
    for (key, body) in entries {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key);
        let body = encode_directory_entry_body(body)?;
        hasher.update(&(body.len() as u64).to_le_bytes());
        hasher.update(&body);
    }
    Ok(DirectoryIndexSnapshot {
        entry_count: entries.len(),
        snapshot_hash: hasher.finalize().to_hex().to_string(),
    })
}

pub(super) fn read_all_metadata_journal_records(
    mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
    bucket: &Bucket,
) -> Result<Vec<ObjectMetadataRecord>> {
    let snapshot = mvcc.runtime.applied_version()?;
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let head_key =
        crate::mvcc_product::stream_logical_key(TABLE_STREAM_HEAD_ROW, &stream_id, None)?;
    let Some(head_payload) = read_metadata_product_at(mvcc, &head_key, snapshot)? else {
        return Ok(Vec::new());
    };
    let head = decode_head(&head_payload)?;
    let prefix =
        crate::mvcc_product::stream_logical_key(TABLE_STREAM_RECORD_INDEX_ROW, &stream_id, None)?;
    let mut events = mvcc
        .runtime
        .scan_table_prefix_at(
            TABLE_STREAM_RECORD_INDEX_ROW,
            &prefix.application_key,
            snapshot,
        )?
        .into_iter()
        .map(|(_, row)| decode_event(&row.value))
        .collect::<Result<Vec<_>>>()?;
    events.sort_by_key(|event| event.partition_sequence);
    validate_event_chain(&events, 0, String::new())?;
    if events
        .last()
        .map(|event| event.partition_sequence)
        .unwrap_or(0)
        != head.last_sequence
        || events
            .last()
            .map(|event| event.event_hash.as_str())
            .unwrap_or("")
            != head.last_event_hash
    {
        bail!("object metadata journal head does not match its event chain");
    }
    events
        .into_iter()
        .map(|event| {
            let body = decode_object_version_body(&event.payload)?;
            Ok(ObjectMetadataRecord {
                partition_sequence: event.partition_sequence,
                event_hash: event.event_hash,
                record_kind: ObjectMetadataRecordKind::from_str(&event.record_kind)?,
                payload: event.payload,
                body,
            })
        })
        .collect()
}
