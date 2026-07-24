use super::*;

mod idempotency;
mod portable_bootstrap;
mod root_directory;
mod stream_publication_recovery;

const TEST_PENDING_MUTATION_NODE_ID: &str = "local-corestore-node";
const TEST_PENDING_MUTATION_EPOCH: u64 = 1;

fn test_pending_mutation_record(
    mutation_id: &str,
    created_at_unix_nanos: u64,
    sequence: u64,
) -> CorePendingMutationRecord {
    CorePendingMutationRecord {
        schema: CORE_PENDING_MUTATION_RECORD_SCHEMA.to_string(),
        node_id: TEST_PENDING_MUTATION_NODE_ID.to_string(),
        mutation_epoch: TEST_PENDING_MUTATION_EPOCH,
        sequence,
        mutation_id: mutation_id.to_string(),
        idempotency_key_hash: None,
        anvil_storage_tenant_id: "local".to_string(),
        authz_scope: test_pending_authz_scope(),
        operation_family: "mutation.batch".to_string(),
        writer_family: "core_control".to_string(),
        target: test_mutation_target(),
        precondition_fingerprints: Vec::new(),
        boundary_values: Vec::new(),
        landed_bytes: Vec::new(),
        created_at_unix_nanos,
    }
}

fn test_pending_authz_scope() -> CorePendingAuthzScope {
    CorePendingAuthzScope {
        realm_id: "system".to_string(),
        revision: None,
    }
}

fn test_mutation_target() -> CorePendingMutationTarget {
    CorePendingMutationTarget::MutationBatch {
        transaction_id: "test-transaction".to_string(),
        scope_partition: "core-control".to_string(),
        operation_count: 0,
    }
}

fn test_object_put_target(logical_name: &str) -> CorePendingMutationTarget {
    let payload = b"sample-payload";
    let object_hash = format!("sha256:{}", sha256_hex(payload));
    CorePendingMutationTarget::ObjectPut {
        logical_name: logical_name.to_string(),
        region_id: "local".to_string(),
        erasure_profile_id: LOCAL_ERASURE_PROFILE_ID.to_string(),
        encryption: "none".to_string(),
        block_plain_hash: object_hash.clone(),
        object_hash,
        object_logical_size: payload.len() as u64,
        compression: none_compression_descriptor(payload),
        writer_generation: 0,
        block_ordinal: 0,
        logical_offset: 0,
    }
}

fn test_stream_append_target(
    stream_id: &str,
    partition_id: &str,
    record_kind: &str,
) -> CorePendingMutationTarget {
    CorePendingMutationTarget::StreamAppend {
        stream_id: stream_id.to_string(),
        partition_id: partition_id.to_string(),
        record_kind: record_kind.to_string(),
        transaction_id: None,
    }
}

fn count_root_cache_generations(store: &CoreStore, root_key_hash_value: &str) -> usize {
    store
        .meta
        .scan_prefix_page(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_prefix(root_key_hash_value),
            None,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )
        .unwrap()
        .len()
}

fn count_files_with_extension(root: &std::path::Path, extension: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    let mut count = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += count_files_with_extension(&path, extension);
        } else if path.extension().is_some_and(|actual| actual == extension) {
            count += 1;
        }
    }
    count
}

fn assert_control_record_not_json_or_cbor(label: &str, bytes: &[u8]) {
    assert!(!bytes.is_empty(), "{label} must not be empty");
    assert!(
        serde_json::from_slice::<serde_json::Value>(bytes).is_err(),
        "{label} must not be JSON"
    );
    assert!(
        !looks_like_complete_cbor_value(bytes),
        "{label} must not be CBOR"
    );
}

fn looks_like_complete_cbor_value(bytes: &[u8]) -> bool {
    let mut cursor = CborCursor {
        bytes,
        offset: 0,
        depth: 0,
    };
    cursor.parse_value().is_some() && cursor.offset == bytes.len()
}

enum CborLen {
    Definite(usize),
    Indefinite,
}

struct CborCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
    depth: usize,
}

impl<'a> CborCursor<'a> {
    fn parse_value(&mut self) -> Option<()> {
        if self.depth > 64 {
            return None;
        }
        let initial = self.read_u8()?;
        if initial == 0xff {
            return None;
        }
        let major = initial >> 5;
        let additional = initial & 0x1f;
        match major {
            0 | 1 => {
                self.read_len(additional, false)?;
            }
            2 | 3 => self.parse_string_like(major, additional)?,
            4 => self.parse_array(additional)?,
            5 => self.parse_map(additional)?,
            6 => {
                self.read_len(additional, false)?;
                self.depth += 1;
                let parsed = self.parse_value();
                self.depth -= 1;
                parsed?;
            }
            7 => self.parse_simple(additional)?,
            _ => return None,
        }
        Some(())
    }

    fn parse_string_like(&mut self, major: u8, additional: u8) -> Option<()> {
        match self.read_len(additional, true)? {
            CborLen::Definite(len) => self.skip(len),
            CborLen::Indefinite => {
                loop {
                    if self.peek_u8()? == 0xff {
                        self.offset += 1;
                        break;
                    }
                    let chunk_initial = self.read_u8()?;
                    if chunk_initial >> 5 != major {
                        return None;
                    }
                    let chunk_additional = chunk_initial & 0x1f;
                    let CborLen::Definite(len) = self.read_len(chunk_additional, false)? else {
                        return None;
                    };
                    self.skip(len)?;
                }
                Some(())
            }
        }
    }

    fn parse_array(&mut self, additional: u8) -> Option<()> {
        match self.read_len(additional, true)? {
            CborLen::Definite(len) => {
                for _ in 0..len {
                    self.depth += 1;
                    let parsed = self.parse_value();
                    self.depth -= 1;
                    parsed?;
                }
                Some(())
            }
            CborLen::Indefinite => {
                loop {
                    if self.peek_u8()? == 0xff {
                        self.offset += 1;
                        break;
                    }
                    self.depth += 1;
                    let parsed = self.parse_value();
                    self.depth -= 1;
                    parsed?;
                }
                Some(())
            }
        }
    }

    fn parse_map(&mut self, additional: u8) -> Option<()> {
        match self.read_len(additional, true)? {
            CborLen::Definite(len) => {
                for _ in 0..len.checked_mul(2)? {
                    self.depth += 1;
                    let parsed = self.parse_value();
                    self.depth -= 1;
                    parsed?;
                }
                Some(())
            }
            CborLen::Indefinite => {
                loop {
                    if self.peek_u8()? == 0xff {
                        self.offset += 1;
                        break;
                    }
                    self.depth += 1;
                    let key = self.parse_value();
                    let value = self.parse_value();
                    self.depth -= 1;
                    key?;
                    value?;
                }
                Some(())
            }
        }
    }

    fn parse_simple(&mut self, additional: u8) -> Option<()> {
        match additional {
            0..=23 => Some(()),
            24 => self.skip(1),
            25 => self.skip(2),
            26 => self.skip(4),
            27 => self.skip(8),
            _ => None,
        }
    }

    fn read_len(&mut self, additional: u8, allow_indefinite: bool) -> Option<CborLen> {
        let value = match additional {
            0..=23 => usize::from(additional),
            24 => usize::from(self.read_u8()?),
            25 => usize::from(u16::from_be_bytes(self.read_array()?)),
            26 => usize::try_from(u32::from_be_bytes(self.read_array()?)).ok()?,
            27 => usize::try_from(u64::from_be_bytes(self.read_array()?)).ok()?,
            31 if allow_indefinite => return Some(CborLen::Indefinite),
            _ => return None,
        };
        Some(CborLen::Definite(value))
    }

    fn read_array<const N: usize>(&mut self) -> Option<[u8; N]> {
        let end = self.offset.checked_add(N)?;
        let slice = self.bytes.get(self.offset..end)?;
        self.offset = end;
        slice.try_into().ok()
    }

    fn read_u8(&mut self) -> Option<u8> {
        let value = *self.bytes.get(self.offset)?;
        self.offset += 1;
        Some(value)
    }

    fn peek_u8(&self) -> Option<u8> {
        self.bytes.get(self.offset).copied()
    }

    fn skip(&mut self, len: usize) -> Option<()> {
        self.offset = self.offset.checked_add(len)?;
        if self.offset <= self.bytes.len() {
            Some(())
        } else {
            None
        }
    }
}

fn test_object_ref_for_payload(
    store: &CoreStore,
    _logical_file_id: &str,
    bytes: &[u8],
    profile: LocalErasureProfile,
) -> CoreObjectRef {
    let hash = sha256_hex(bytes);
    let object_hash = format!("sha256:{hash}");
    let block_id = local_block_id_for_stored_block(profile.id, &object_hash);
    let shards = encode_erasure_shards(bytes, profile).unwrap();
    let placements = plan_local_shard_placements(profile).unwrap();
    let boundary_values = Vec::<CoreBoundaryValue>::new();
    let boundary_summary_hash = boundary_summary_hash(&boundary_values).unwrap();
    let mut object_placements = Vec::new();
    let mut stripe_size = 0u64;
    for (shard_index, shard) in shards.iter().enumerate() {
        let shard_hash = sha256_hex(shard);
        let placement = &placements[shard_index];
        stripe_size =
            stripe_size.max((shard.len() as u64).saturating_mul(profile.data_shards as u64));
        let written_at_unix_nanos = unix_timestamp_nanos();
        let shard_hash = format!("sha256:{shard_hash}");
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &block_id,
            shard_index: shard_index as u16,
            erasure_profile: profile.id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            shard_length: shard.len() as u64,
            shard_hash: &shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: &boundary_summary_hash,
        });
        object_placements.push(CoreObjectPlacement {
            shard_index: shard_index as u16,
            node_id: placement.node_id.clone(),
            region_id: placement.region_id.clone(),
            cell_id: placement.cell_id.clone(),
            shard_hash,
            stored_size: shard.len() as u64,
            generation: 1,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            receipt_signature: store.sign_core_receipt(&signed_payload_hash).unwrap(),
            signed_payload_hash,
            signature_algorithm: "ed25519".to_string(),
        });
    }
    CoreObjectRef {
        hash: object_hash,
        logical_size: bytes.len() as u64,
        manifest_ref: encode_manifest_ref_with_profile(&hash, profile.id),
        encoding: CoreObjectEncoding {
            block_id,
            profile_id: profile.id.to_string(),
            data_shards: profile.data_shards as u16,
            parity_shards: profile.parity_shards as u16,
            minimum_read_shards: profile.minimum_read_shards as u16,
            minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
            stripe_size,
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            stored_hash: format!("sha256:{hash}"),
            compression: none_compression_descriptor(bytes),
            encryption: "none".to_string(),
        },
        placements: object_placements,
    }
}

async fn write_test_pending_mutation_records(
    store: &CoreStore,
    records: Vec<CorePendingMutationRecord>,
) {
    let mut max_sequences = BTreeMap::<String, u64>::new();
    for record in records {
        let shard_hash = record.target.admission_shard().hash;
        let pending_row = encode_stored_pending_mutation_row(&record, b"").unwrap();
        let pending_hash_input = encode_pending_mutation_hash_input(&record, b"").unwrap();
        let payload_set_hash = format!("sha256:{}", sha256_hex(&pending_row));
        let evidence = store
            .local_admission_evidence_bytes(&record, &pending_hash_input, payload_set_hash)
            .unwrap();
        max_sequences
            .entry(shard_hash.clone())
            .and_modify(|sequence| *sequence = (*sequence).max(record.sequence))
            .or_insert(record.sequence);
        store
            .meta
            .put(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &admission_record_key(&shard_hash, record.sequence),
                &pending_row,
            )
            .unwrap();
        store
            .meta
            .put(
                CF_TRANSACTIONS,
                TABLE_LOCAL_ADMISSION_EVIDENCE_ROW,
                &admission_evidence_key(&shard_hash, record.sequence),
                &evidence,
            )
            .unwrap();
    }
    for (shard_hash, max_sequence) in max_sequences {
        store
            .meta
            .put(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_sequence_key(&shard_hash),
                &encode_admission_sequence_cursor_row(&shard_hash, max_sequence).unwrap(),
            )
            .unwrap();
    }
    store.install_admission_point_state_for_tests().unwrap();
}

async fn read_test_pending_mutation_records(
    store: &CoreStore,
) -> Vec<(CorePendingMutationRecord, Vec<u8>)> {
    store
        .read_pending_mutation_records_with_payload()
        .await
        .unwrap()
}

fn sample_boundary_schema(bucket: &str, generation: u64) -> CoreBoundarySchema {
    CoreBoundarySchema {
        schema: CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
        bucket: bucket.to_string(),
        generation,
        dimensions: vec![CoreBoundaryDimension {
            name: "customer_tenant".to_string(),
            source: CoreBoundarySource::UserMetadataJsonPointer {
                pointer: "/customer_tenant_id".to_string(),
            },
            value_type: "uuid".to_string(),
            categories: vec![
                "security_realm".to_string(),
                "storage_partition".to_string(),
                "query_prune".to_string(),
            ],
            required: true,
            cardinality: "extreme".to_string(),
            max_values_per_block: 1,
            placement_affinity: "prefer_colocate".to_string(),
            compaction_scope: "require_same_value".to_string(),
            shared_ranges_allowed: false,
            shared_record_kinds: Vec::new(),
            deprecated: false,
        }],
        created_at: String::new(),
    }
}

mod admission_accounting;
mod cancellation;
mod control_record_encoding;
mod coremeta_history;
mod erasure_roots;
mod explicit_precondition_boundaries;
mod explicit_stream_transactions;
mod final_linearization_guards;
mod logical;
mod object_metadata;
mod pending;
mod pending_terminal_recovery;
mod precondition_errors;
mod record_formats;
mod root_publication_recovery;
mod stream_paging;
mod task_publication_successor;
mod visibility;
