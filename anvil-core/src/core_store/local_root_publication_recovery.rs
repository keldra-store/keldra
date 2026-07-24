use super::local_root_failover::RootOwnerTerms;
use super::*;
use prost::Message;

#[path = "local_root_publication_recovery/distributed_publication.rs"]
mod distributed_publication;
#[path = "local_root_publication_recovery/intent_codec.rs"]
mod intent_codec;
#[path = "local_root_publication_recovery/intent_state.rs"]
mod intent_state;
#[path = "local_root_publication_recovery/publication_guard.rs"]
mod publication_guard;
#[path = "local_root_publication_recovery/recovery_bundle.rs"]
mod recovery_bundle;
#[path = "local_root_publication_recovery/replica_intent.rs"]
mod replica_intent;

use intent_codec::*;
use intent_state::publication_terminal_error;
pub(in crate::core_store::local) use intent_state::publication_terminal_reason;
use publication_guard::*;

pub(in crate::core_store::local) use recovery_bundle::{
    CoreMetaRecoveryPublicationBundle, decode_coremeta_recovery_publication_bundle,
};

const PUBLICATION_INTENT_SCHEMA: &str = "anvil.core.root_publication_intent.v2";
const PUBLICATION_ROOT_SCHEMA: &str = "anvil.core.root_publication_root.v1";
const PUBLICATION_ROW_SCHEMA: &str = "anvil.core.root_publication_row.v1";
const PUBLICATION_CHUNK_SCHEMA: &str = "anvil.core.root_publication_chunk.v1";
const REPLICA_INTENT_SCHEMA: &str = "anvil.core.root_publication_replica.v3";
const STORED_ROW_SCHEMA: &str = "anvil.core.root_publication_stored_row.v1";
const PUBLICATION_ROW_CHUNK_BYTES: usize = 24 * 1024;
const MAX_PUBLICATION_ROOTS: usize = CORE_META_MAX_SCAN_PAGE_ROWS;
pub(super) const MAX_PUBLICATION_ROWS: usize = 65_536;

#[derive(Debug, Clone)]
pub(super) struct RootPublicationIntentRoot {
    pub(super) ordinal: u64,
    pub(super) publication: PreparedRootPublication,
    pub(super) expected_root_generation: u64,
    pub(super) rows: Vec<CoreMetaEncodedOwnedRow>,
    pub(super) certificate_hash: Option<String>,
}

#[derive(Debug, Clone)]
pub(super) struct RootPublicationIntent {
    pub(super) transaction_id: String,
    pub(super) plan_hash: String,
    pub(super) publisher_node_id: String,
    pub(super) created_at_unix_nanos: u64,
    pub(super) roots: Vec<RootPublicationIntentRoot>,
    pub(super) local_rows: Vec<CoreMetaEncodedOwnedRow>,
    pub(super) guard: Option<super::local_tx_rows::CorePublicationGuardSummary>,
    pub(super) state: RootPublicationIntentState,
    pub(super) terminal_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RootPublicationIntentState {
    Pending,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RootPublicationAuthority {
    LocalOwnerState,
    RegisterQuorum,
}

#[derive(Clone, PartialEq, Message)]
struct PublicationIntentHeaderProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(string, tag = "4")]
    plan_hash: String,
    #[prost(uint64, tag = "5")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "6")]
    root_count: u64,
    #[prost(uint64, tag = "7")]
    local_row_count: u64,
    #[prost(string, optional, tag = "8")]
    coordinator_root_key_hash: Option<String>,
    #[prost(uint64, optional, tag = "9")]
    coordinator_root_generation: Option<u64>,
    #[prost(string, tag = "10")]
    publisher_node_id: String,
    #[prost(string, optional, tag = "11")]
    guard_context_hash: Option<String>,
    #[prost(uint64, tag = "12")]
    transaction_expires_at_unix_nanos: u64,
    #[prost(uint64, tag = "13")]
    guard_visible_update_count: u64,
    #[prost(uint64, tag = "14")]
    guard_precondition_count: u64,
    #[prost(enumeration = "PublicationIntentStateProto", tag = "15")]
    state: i32,
    #[prost(string, optional, tag = "16")]
    terminal_reason: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
enum PublicationIntentStateProto {
    Unspecified = 0,
    Pending = 1,
    Terminal = 2,
}

#[derive(Clone, PartialEq, Message)]
struct PublicationRootProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(string, tag = "5")]
    root_anchor_key: String,
    #[prost(string, tag = "6")]
    root_key_hash: String,
    #[prost(uint64, tag = "7")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "8")]
    post_root_generation: u64,
    #[prost(bool, tag = "9")]
    transaction_coordinator: bool,
    #[prost(string, repeated, tag = "10")]
    writer_families: Vec<String>,
    #[prost(bytes, repeated, tag = "11")]
    logical_manifests: Vec<Vec<u8>>,
    #[prost(string, repeated, tag = "12")]
    idempotency_key_hashes: Vec<String>,
    #[prost(string, tag = "13")]
    previous_root_hash: String,
    #[prost(bytes, tag = "14")]
    transaction_manifest_locator: Vec<u8>,
    #[prost(string, tag = "15")]
    transaction_manifest_row_hash: String,
    #[prost(uint64, tag = "16")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "17")]
    row_count: u64,
    #[prost(string, tag = "18")]
    rows_hash: String,
    #[prost(string, optional, tag = "19")]
    certificate_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct PublicationRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(uint64, tag = "5")]
    root_ordinal: u64,
    #[prost(uint64, tag = "6")]
    row_ordinal: u64,
    #[prost(string, tag = "7")]
    row_hash: String,
    #[prost(uint64, tag = "8")]
    encoded_length: u64,
    #[prost(uint64, tag = "9")]
    chunk_count: u64,
}

#[derive(Clone, PartialEq, Message)]
struct PublicationChunkProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(uint64, tag = "5")]
    root_ordinal: u64,
    #[prost(uint64, tag = "6")]
    row_ordinal: u64,
    #[prost(uint64, tag = "7")]
    chunk_ordinal: u64,
    #[prost(uint64, tag = "8")]
    chunk_count: u64,
    #[prost(string, tag = "9")]
    row_hash: String,
    #[prost(bytes, tag = "10")]
    bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct StoredEncodedRowProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    cf: String,
    #[prost(bytes, tag = "3")]
    core_meta_key: Vec<u8>,
    #[prost(bytes, tag = "4")]
    value_envelope: Vec<u8>,
    #[prost(bool, tag = "5")]
    delete_marker: bool,
    #[prost(string, tag = "6")]
    root_key_hash: String,
    #[prost(uint64, tag = "7")]
    root_generation: u64,
    #[prost(enumeration = "CoreMetaVisibilityState", tag = "8")]
    visibility_state: i32,
}

#[derive(Clone, PartialEq, Message)]
struct ReplicaPublicationIntentProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    transaction_id: String,
    #[prost(string, tag = "3")]
    plan_hash: String,
    #[prost(uint64, tag = "4")]
    created_at_unix_nanos: u64,
    #[prost(message, repeated, tag = "5")]
    roots: Vec<PublicationRootProto>,
    #[prost(string, tag = "6")]
    publisher_node_id: String,
    #[prost(string, optional, tag = "7")]
    guard_context_hash: Option<String>,
    #[prost(uint64, tag = "8")]
    transaction_expires_at_unix_nanos: u64,
    #[prost(uint64, tag = "9")]
    guard_visible_update_count: u64,
    #[prost(uint64, tag = "10")]
    guard_precondition_count: u64,
    #[prost(message, repeated, tag = "11")]
    local_rows: Vec<StoredEncodedRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct PublicationSchemaProbe {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
}

struct StoredIntentRow {
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
}

pub(super) fn root_publication_plan_hash(
    transaction_id: &str,
    roots: &[(CoreMetaRootPublication, Vec<CoreMetaEncodedOwnedRow>)],
    local_rows: &[CoreMetaEncodedOwnedRow],
) -> Result<String> {
    validate_logical_id(transaction_id, "CoreMeta publication plan transaction id")?;
    let mut roots = roots.to_vec();
    roots.sort_by_key(|(publication, _)| publication.root_key_hash());
    let mut bytes = Vec::new();
    append_hash_part(&mut bytes, b"anvil.core.root_publication_plan.v1");
    append_hash_part(&mut bytes, transaction_id.as_bytes());
    let mut guard_owned_rows = local_rows.to_vec();
    let mut sorted_local_rows = local_rows.to_vec();
    sort_encoded_rows(&mut sorted_local_rows);
    for row in &sorted_local_rows {
        append_hash_part(&mut bytes, encoded_row_hash(row).as_bytes());
    }
    for (publication, mut rows) in roots {
        publication.validate()?;
        append_hash_part(&mut bytes, publication.root_anchor_key.as_bytes());
        append_hash_part(&mut bytes, &[u8::from(publication.transaction_coordinator)]);
        for family in &publication.writer_families {
            append_hash_part(&mut bytes, family.as_bytes());
        }
        for locator in &publication.logical_manifests {
            append_hash_part(
                &mut bytes,
                &crate::core_store::transaction_manifest_proto::encode_manifest_locator_proto(
                    locator,
                )?,
            );
        }
        for hash in &publication.idempotency_key_hashes {
            append_hash_part(&mut bytes, hash.as_bytes());
        }
        sort_encoded_rows(&mut rows);
        guard_owned_rows.extend(rows.iter().cloned());
        for row in &rows {
            append_hash_part(&mut bytes, encoded_row_hash(row).as_bytes());
        }
    }
    let guard_rows = guard_owned_rows.iter().collect::<Vec<_>>();
    let guard = super::local_tx_rows::publication_guard_summary(transaction_id, &guard_rows)?;
    append_publication_guard_plan_hash(&mut bytes, guard.as_ref());
    Ok(format!("sha256:{}", sha256_hex(&bytes)))
}

pub(super) fn build_root_publication_intent(
    transaction_id: &str,
    plan_hash: String,
    publisher_node_id: String,
    created_at_unix_nanos: u64,
    roots: Vec<RootPublicationIntentRoot>,
    mut local_rows: Vec<CoreMetaEncodedOwnedRow>,
) -> Result<RootPublicationIntent> {
    validate_logical_id(transaction_id, "CoreMeta publication intent transaction id")?;
    validate_hash(&plan_hash, "CoreMeta publication intent plan hash")?;
    validate_logical_id(
        &publisher_node_id,
        "CoreMeta publication intent publisher node id",
    )?;
    if created_at_unix_nanos == 0 {
        bail!("CoreMeta publication intent timestamp must be nonzero");
    }
    if roots.is_empty() || roots.len() > MAX_PUBLICATION_ROOTS {
        bail!("CoreMeta publication intent root count is outside the bounded range");
    }
    let total_rows = roots
        .iter()
        .try_fold(local_rows.len(), |count, root| {
            count.checked_add(root.rows.len())
        })
        .ok_or_else(|| anyhow!("CoreMeta publication intent row count overflow"))?;
    if total_rows > MAX_PUBLICATION_ROWS {
        bail!("CoreMeta publication intent exceeds the bounded row count");
    }
    for row in &local_rows {
        validate_local_intent_row(row)?;
    }
    sort_encoded_rows(&mut local_rows);
    let mut roots = roots;
    roots.sort_by_key(|root| root.publication.descriptor.root_key_hash());
    for (ordinal, root) in roots.iter_mut().enumerate() {
        root.ordinal = u64::try_from(ordinal)
            .map_err(|_| anyhow!("CoreMeta publication root ordinal exceeds u64"))?;
        validate_intent_root(transaction_id, created_at_unix_nanos, root)?;
        sort_encoded_rows(&mut root.rows);
    }
    let guard_rows = roots
        .iter()
        .flat_map(|root| root.rows.iter())
        .chain(local_rows.iter())
        .collect::<Vec<_>>();
    let guard = super::local_tx_rows::publication_guard_summary(transaction_id, &guard_rows)?;
    let intent = RootPublicationIntent {
        transaction_id: transaction_id.to_string(),
        plan_hash,
        publisher_node_id,
        created_at_unix_nanos,
        roots,
        local_rows,
        guard,
        state: RootPublicationIntentState::Pending,
        terminal_reason: None,
    };
    intent.coordinator_scope()?;
    Ok(intent)
}

impl CoreStore {
    pub(super) fn persist_root_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        let rows = encode_intent_rows(intent)?;
        if let Some(existing) = self.read_root_publication_intent(&intent.transaction_id)? {
            if !publication_intent_retry_matches(&existing, intent)? {
                bail!("CoreMeta idempotent retry changed its publication plan");
            }
            return Ok(());
        }
        write_stored_intent_rows(&self.meta, &rows)
    }

    pub(super) fn read_root_publication_intent(
        &self,
        transaction_id: &str,
    ) -> Result<Option<RootPublicationIntent>> {
        validate_logical_id(transaction_id, "CoreMeta publication intent transaction id")?;
        let snapshot = self.meta.read_snapshot();
        let Some(payload) = snapshot.get(
            CF_TRANSACTIONS,
            TABLE_ROOT_PUBLICATION_INTENT_ROW,
            &intent_header_key(transaction_id)?,
        )?
        else {
            return Ok(None);
        };
        let header = decode_canonical::<PublicationIntentHeaderProto>(
            &payload,
            "CoreMeta root publication intent",
        )?;
        validate_header(&header)?;
        if header.transaction_id != transaction_id {
            bail!("CoreMeta publication intent transaction scope mismatch");
        }
        let mut roots = Vec::with_capacity(usize_from_bounded(
            header.root_count,
            MAX_PUBLICATION_ROOTS,
            "CoreMeta publication root count",
        )?);
        for ordinal in 0..header.root_count {
            roots.push(self.read_publication_root(&snapshot, &header, ordinal)?);
        }
        let local_count = usize_from_bounded(
            header.local_row_count,
            MAX_PUBLICATION_ROWS,
            "CoreMeta publication local row count",
        )?;
        let mut local_rows = Vec::with_capacity(local_count);
        for ordinal in 0..header.local_row_count {
            local_rows.push(self.read_intent_encoded_row(
                &snapshot,
                &header,
                "local",
                u64::MAX,
                ordinal,
            )?);
        }
        let intent = build_root_publication_intent(
            transaction_id,
            header.plan_hash.clone(),
            header.publisher_node_id.clone(),
            header.created_at_unix_nanos,
            roots,
            local_rows,
        )?;
        let mut intent = intent;
        intent.state = publication_intent_state_from_proto(header.state)?;
        intent.terminal_reason = header.terminal_reason.clone();
        if header != intent_header_proto(&intent)? {
            bail!("CoreMeta publication intent guard or lifecycle summary mismatch");
        }
        let expected_scope = intent.coordinator_scope()?;
        if header.coordinator_root_key_hash != expected_scope.as_ref().map(|(hash, _)| hash.clone())
            || header.coordinator_root_generation
                != expected_scope.as_ref().map(|(_, generation)| *generation)
        {
            bail!("CoreMeta publication intent coordinator scope mismatch");
        }
        let stored_plan = plan_hash_from_intent(&intent)?;
        if stored_plan != intent.plan_hash {
            bail!("CoreMeta publication intent plan hash mismatch");
        }
        Ok(Some(intent))
    }

    /// Validates the durable publication summary without rehydrating every
    /// encoded row. The intent rows are written atomically, so the header and
    /// per-root hashes are sufficient on the uninterrupted commit path. Crash
    /// recovery still uses `read_root_publication_intent` to verify every row.
    pub(super) fn validate_persisted_root_publication_intent_summary(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<bool> {
        let snapshot = self.meta.read_snapshot();
        let Some(payload) = snapshot.get(
            CF_TRANSACTIONS,
            TABLE_ROOT_PUBLICATION_INTENT_ROW,
            &intent_header_key(&intent.transaction_id)?,
        )?
        else {
            return Ok(false);
        };
        let header = decode_canonical::<PublicationIntentHeaderProto>(
            &payload,
            "CoreMeta root publication intent",
        )?;
        validate_header(&header)?;
        if header != intent_header_proto(intent)? {
            bail!("CoreMeta persisted publication intent header changed");
        }
        for root in &intent.roots {
            let payload = snapshot
                .get(
                    CF_TRANSACTIONS,
                    TABLE_ROOT_PUBLICATION_INTENT_ROW,
                    &intent_root_key(&intent.transaction_id, root.ordinal)?,
                )?
                .ok_or_else(|| anyhow!("CoreMeta persisted publication root is missing"))?;
            let stored = decode_canonical::<PublicationRootProto>(
                &payload,
                "CoreMeta root publication root row",
            )?;
            validate_root_proto(&stored, &header, root.ordinal)?;
            if stored != root_to_proto(intent, root)? {
                bail!("CoreMeta persisted publication root changed");
            }
        }
        Ok(true)
    }

    fn read_publication_root(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        header: &PublicationIntentHeaderProto,
        ordinal: u64,
    ) -> Result<RootPublicationIntentRoot> {
        let payload = snapshot
            .get(
                CF_TRANSACTIONS,
                TABLE_ROOT_PUBLICATION_INTENT_ROW,
                &intent_root_key(&header.transaction_id, ordinal)?,
            )?
            .ok_or_else(|| anyhow!("CoreMeta publication root row is missing"))?;
        let root = decode_canonical::<PublicationRootProto>(
            &payload,
            "CoreMeta root publication root row",
        )?;
        validate_root_proto(&root, header, ordinal)?;
        let row_count = usize_from_bounded(
            root.row_count,
            MAX_PUBLICATION_ROWS,
            "CoreMeta publication root row count",
        )?;
        let mut rows = Vec::with_capacity(row_count);
        for row_ordinal in 0..root.row_count {
            rows.push(self.read_intent_encoded_row(
                snapshot,
                header,
                "root",
                ordinal,
                row_ordinal,
            )?);
        }
        if rows_hash(&rows) != root.rows_hash {
            bail!("CoreMeta publication root rows hash mismatch");
        }
        let manifest_row = rows
            .iter()
            .find(|row| encoded_row_hash(row) == root.transaction_manifest_row_hash)
            .cloned()
            .ok_or_else(|| anyhow!("CoreMeta publication transaction manifest row is missing"))?;
        let descriptor = CoreMetaRootPublication {
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
        };
        let prepared = PreparedRootPublication {
            descriptor,
            previous_root_hash: root.previous_root_hash,
            transaction_manifest_locator:
                crate::core_store::transaction_manifest_proto::decode_manifest_locator_proto(
                    &root.transaction_manifest_locator,
                )?,
            transaction_manifest_row: manifest_row,
            post_root_generation: root.post_root_generation,
            created_at_unix_nanos: root.created_at_unix_nanos,
        };
        Ok(RootPublicationIntentRoot {
            ordinal,
            publication: prepared,
            expected_root_generation: root.expected_root_generation,
            rows,
            certificate_hash: root.certificate_hash,
        })
    }

    fn read_intent_encoded_row(
        &self,
        snapshot: &CoreMetaReadSnapshot<'_>,
        header: &PublicationIntentHeaderProto,
        scope: &str,
        root_ordinal: u64,
        row_ordinal: u64,
    ) -> Result<CoreMetaEncodedOwnedRow> {
        let payload = snapshot
            .get(
                CF_TRANSACTIONS,
                TABLE_ROOT_PUBLICATION_INTENT_ROW,
                &intent_row_key(&header.transaction_id, scope, root_ordinal, row_ordinal)?,
            )?
            .ok_or_else(|| anyhow!("CoreMeta publication encoded-row header is missing"))?;
        let row = decode_canonical::<PublicationRowProto>(
            &payload,
            "CoreMeta root publication encoded-row header",
        )?;
        validate_row_proto(&row, header, scope, root_ordinal, row_ordinal)?;
        let chunk_count = usize_from_bounded(
            row.chunk_count,
            8,
            "CoreMeta publication encoded-row chunk count",
        )?;
        let encoded_length = usize::try_from(row.encoded_length)
            .map_err(|_| anyhow!("CoreMeta publication encoded row length exceeds usize"))?;
        let mut encoded = Vec::with_capacity(encoded_length);
        for chunk_ordinal in 0..row.chunk_count {
            let payload = snapshot
                .get(
                    CF_TRANSACTIONS,
                    TABLE_ROOT_PUBLICATION_INTENT_ROW,
                    &intent_chunk_key(
                        &header.transaction_id,
                        scope,
                        root_ordinal,
                        row_ordinal,
                        chunk_ordinal,
                    )?,
                )?
                .ok_or_else(|| anyhow!("CoreMeta publication encoded-row chunk is missing"))?;
            let chunk = decode_canonical::<PublicationChunkProto>(
                &payload,
                "CoreMeta root publication encoded-row chunk",
            )?;
            validate_chunk_proto(
                &chunk,
                header,
                &row,
                chunk_ordinal,
                u64::try_from(chunk_count).unwrap_or(u64::MAX),
            )?;
            encoded.extend_from_slice(&chunk.bytes);
        }
        if encoded.len() != encoded_length {
            bail!("CoreMeta publication encoded-row length mismatch");
        }
        let stored = decode_canonical::<StoredEncodedRowProto>(
            &encoded,
            "CoreMeta publication stored encoded row",
        )?;
        let owned = owned_row_from_proto(stored)?;
        if encoded_row_hash(&owned) != row.row_hash {
            bail!("CoreMeta publication encoded-row hash mismatch");
        }
        Ok(owned)
    }

    pub(super) fn encode_replica_root_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<u8>> {
        intent.ensure_pending()?;
        let roots = intent
            .roots
            .iter()
            .map(|root| root_to_proto(intent, root))
            .collect::<Result<Vec<_>>>()?;
        Ok(encode_deterministic_proto(&ReplicaPublicationIntentProto {
            schema: REPLICA_INTENT_SCHEMA.to_string(),
            transaction_id: intent.transaction_id.clone(),
            plan_hash: intent.plan_hash.clone(),
            created_at_unix_nanos: intent.created_at_unix_nanos,
            roots,
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
            local_rows: intent.local_rows.iter().map(stored_row_proto).collect(),
        }))
    }

    pub(crate) fn root_publication_intent_created_at(&self, transaction_id: &str) -> Result<u64> {
        self.read_root_publication_intent(transaction_id)?
            .map(|intent| intent.created_at_unix_nanos)
            .ok_or_else(|| anyhow!("CoreMeta root publication intent is missing"))
    }

    pub(super) fn validate_staged_publication_rows(
        &self,
        transaction_id: &str,
        rows_by_root: &BTreeMap<String, Vec<CoreMetaEncodedOwnedRow>>,
    ) -> Result<RootPublicationIntent> {
        let intent = self
            .read_root_publication_intent(transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta root publication intent is missing"))?;
        if intent.roots.len() != rows_by_root.len() {
            bail!("CoreMeta staged publication root cardinality mismatch");
        }
        for root in &intent.roots {
            let rows = rows_by_root
                .get(&root.publication.descriptor.root_key_hash())
                .ok_or_else(|| anyhow!("CoreMeta staged publication root is missing"))?;
            if rows_hash(rows) != rows_hash(&root.rows) {
                bail!("CoreMeta staged publication rows do not match the durable intent");
            }
        }
        Ok(intent)
    }

    pub(super) fn record_root_publication_outcomes(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<RootPublicationIntent> {
        intent.ensure_pending()?;
        if !intent.no_outcomes_recorded() {
            bail!("CoreMeta publication outcomes were already recorded");
        }
        let by_root = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        if by_root.len() != intent.roots.len() {
            bail!("CoreMeta publication outcome cardinality mismatch");
        }

        let mut updated = intent.clone();
        let mut rows = Vec::with_capacity(updated.roots.len().saturating_mul(2));
        for root in &mut updated.roots {
            let root_hash = root.publication.descriptor.root_key_hash();
            let outcome = by_root
                .get(root_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta publication outcome is missing a root"))?;
            if outcome.post_root_generation != root.publication.post_root_generation {
                bail!("CoreMeta publication outcome generation mismatch");
            }
            root.certificate_hash = Some(outcome.certificate_hash.clone());
            rows.push(
                self.coremeta_commit_evidence_encoded_row_at(
                    &root_hash,
                    root.publication.post_root_generation,
                    &updated.transaction_id,
                    &outcome.certificate_hash,
                    &outcome.committed_batch_hash,
                    outcome.certificate_bytes.clone(),
                    outcome.certificate_persist_receipt_hashes.clone(),
                    outcome
                        .certificate_persist_receipts
                        .iter()
                        .map(|receipt| {
                            encode_deterministic_proto(&core_persist_receipt_to_api(receipt))
                        })
                        .collect(),
                    updated.created_at_unix_nanos,
                )?,
            );
        }

        let root_rows = updated
            .roots
            .iter()
            .map(|root| {
                Ok((
                    intent_root_key(&updated.transaction_id, root.ordinal)?,
                    encode_deterministic_proto(&root_to_proto(&updated, root)?),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let borrowed_ops = root_rows
            .iter()
            .map(|(tuple_key, payload)| CoreMetaBatchOp {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
                tuple_key: tuple_key.as_slice(),
                common: None,
                kind: CoreMetaBatchOpKind::Put(payload.as_slice()),
            })
            .collect::<Vec<_>>();
        rows.extend(self.meta.encode_batch_ops(&borrowed_ops)?);
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&rows))?;
        Ok(updated)
    }

    pub(super) fn root_publication_outcomes(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        intent.ensure_pending()?;
        if !intent.all_outcomes_recorded() {
            bail!("CoreMeta publication intent does not have complete quorum outcomes");
        }
        let profile = self.default_coremeta_quorum_profile()?;
        let mut outcomes = Vec::with_capacity(intent.roots.len());
        for root in &intent.roots {
            let certificate_hash = root
                .certificate_hash
                .as_deref()
                .ok_or_else(|| anyhow!("CoreMeta publication certificate hash is missing"))?;
            let evidence = self
                .read_coremeta_commit_evidence(certificate_hash)?
                .ok_or_else(|| anyhow!("CoreMeta publication commit evidence is missing"))?;
            let certificate_api =
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                    &evidence.certificate_bytes,
                    "CoreMeta publication certificate",
                )?;
            let certificate = api_commit_certificate_to_core(certificate_api)?;
            let receipts = evidence
                .certificate_persist_receipt_bytes
                .iter()
                .map(|bytes| {
                    decode_deterministic_proto::<
                        crate::anvil_api::CoreMetaCertificatePersistReceipt,
                    >(bytes, "CoreMeta publication persist receipt")
                    .and_then(api_persist_receipt_to_core)
                })
                .collect::<Result<Vec<_>>>()?;
            validate_commit_evidence_with_verifier(
                &profile,
                &certificate,
                &receipts,
                |node_id, signed_payload_hash, signature| {
                    self.verify_internal_core_receipt_signature(
                        node_id,
                        signed_payload_hash,
                        signature,
                    )
                },
            )?;
            let root_hash = root.publication.descriptor.root_key_hash();
            if certificate.certificate_hash != certificate_hash
                || certificate.root_key_hash != root_hash
                || certificate.post_root_generation != root.publication.post_root_generation
                || certificate.transaction_id != intent.transaction_id
            {
                bail!("CoreMeta publication evidence does not match its durable intent");
            }
            let mut metadata_replica_node_ids = receipts
                .iter()
                .map(|receipt| receipt.replica_node_id.clone())
                .collect::<Vec<_>>();
            metadata_replica_node_ids.sort();
            metadata_replica_node_ids.dedup();
            outcomes.push(CoreMetaQuorumCommitOutcome {
                root_key_hash: root_hash,
                post_root_generation: root.publication.post_root_generation,
                certificate_hash: certificate_hash.to_string(),
                committed_batch_hash: evidence.committed_batch_hash,
                certificate_bytes: evidence.certificate_bytes,
                certificate_persist_receipt_hashes: evidence.certificate_persist_receipt_hashes,
                certificate_persist_receipts: receipts,
                metadata_replica_node_ids,
            });
        }
        outcomes.sort_by(|left, right| left.root_key_hash.cmp(&right.root_key_hash));
        Ok(outcomes)
    }

    pub(super) async fn publish_root_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        intent.ensure_pending()?;
        let outcomes = self.root_publication_outcomes(intent)?;
        #[cfg(test)]
        super::local_root_publication_test_control::pause_before_coordinator(
            &intent.transaction_id,
        )
        .await;
        #[cfg(test)]
        if super::local_root_publication_test_control::take_publication_failure(
            &intent.transaction_id,
        ) {
            bail!("injected CoreMeta publication failure");
        }

        // Root-register Q2 preparation is the durable commit decision. Hold
        // every observed row and publication root lock from the last mutable
        // guard check through that decision; materialisation after Q2 must not
        // reject a value which the register has already committed.
        let (publication_guards, guard_context) =
            self.acquire_publication_intent_locks(intent).await?;
        let Some(current_intent) = self.read_root_publication_intent(&intent.transaction_id)?
        else {
            // A foreground retry can finish and clear the intent while a
            // recovery worker waits for the same publication locks. Treat
            // disappearance as success only when every durable root proves
            // that this exact transaction committed.
            if self
                .completed_publication_matches_intent(intent, &outcomes)
                .await?
            {
                return Ok(outcomes);
            }
            bail!("CoreMeta publication intent disappeared before commit");
        };
        if !publication_intent_retry_matches(&current_intent, intent)?
            || current_intent.state != intent.state
            || current_intent.terminal_reason != intent.terminal_reason
        {
            bail!("CoreMeta publication intent changed before commit");
        }
        if self
            .completed_publication_matches_intent(&current_intent, &outcomes)
            .await?
        {
            let delete_rows = self.root_publication_intent_delete_rows(&current_intent)?;
            self.write_coremeta_encoded_rows(&borrow_encoded_rows(&delete_rows))?;
            return Ok(outcomes);
        }
        if self
            .materialize_locally_committed_publication_intent(&current_intent, &outcomes)
            .await?
        {
            return Ok(outcomes);
        }
        self.ensure_publication_intent_active_locked(&current_intent)?;
        self.validate_publication_guards_at_linearization(&current_intent, guard_context.as_ref())
            .await?;
        self.validate_publication_predecessors_at_linearization(&current_intent)
            .await?;

        let anchors = self.publication_anchors(&current_intent, &outcomes)?;
        let coordinator_index = effective_coordinator_index(&current_intent)?;
        let participant_evidence = root_publication_evidence(&anchors, &outcomes)?;
        self.publish_root_anchor_generation_with_participants(
            &anchors[coordinator_index],
            &participant_evidence,
            Some(&current_intent),
        )
        .await?;
        drop(publication_guards);

        for root in &current_intent.roots {
            if !self.root_generation_is_published(
                &root.publication.descriptor.root_key_hash(),
                root.publication.post_root_generation,
                &current_intent.transaction_id,
            )? {
                bail!("CoreMeta publication recovery did not publish every participant root");
            }
        }
        if self
            .read_root_publication_intent(&current_intent.transaction_id)?
            .is_some()
        {
            bail!("CoreMeta publication recovery did not clear its durable intent");
        }
        Ok(outcomes)
    }

    /// Resolves a single-root intent staged by a publisher which is no longer
    /// the fenced owner. The immutable intent remains attributed to its
    /// original publisher; only the root ownership terms change after a
    /// quorum-backed failover grant.
    pub(super) async fn publish_foreign_single_root_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        intent.ensure_pending()?;
        if intent.publisher_node_id == self.node_identity.node_id {
            return self.publish_root_publication_intent(intent).await;
        }
        if intent.roots.len() != 1 {
            bail!(
                "CoreMeta successor recovery currently requires a single-root publication intent"
            );
        }
        if !intent.all_outcomes_recorded() {
            bail!("CoreMeta successor recovery requires complete quorum outcomes");
        }

        let outcomes = self.root_publication_outcomes(intent)?;
        let original_anchors = self.publication_anchors(intent, &outcomes)?;
        let original_anchor = original_anchors
            .first()
            .ok_or_else(|| anyhow!("CoreMeta successor recovery has no publication anchor"))?;
        let original_anchor_bytes = encode_root_anchor_record(original_anchor)?;
        let peers = self.coremeta_recovery_peers()?;
        match self
            .resolve_root_register_quorum(
                &peers,
                None,
                &original_anchor.root_key_hash,
                original_anchor.root_generation,
                &original_anchor_bytes,
            )
            .await?
        {
            super::local_coremeta_recovery::RootRegisterQuorumResolution::Committed => {
                self.materialize_committed_publication_intent(intent, &original_anchors)
                    .await?;
                return Ok(outcomes);
            }
            super::local_coremeta_recovery::RootRegisterQuorumResolution::CommittedConflict {
                anchor_record,
            } => {
                let committed = decode_root_anchor_record(&anchor_record)?;
                if committed.root_key_hash != original_anchor.root_key_hash
                    || committed.root_generation != original_anchor.root_generation
                {
                    bail!("CoreMeta conflicting root-register decision escaped its scope");
                }
                // Q2 has made the competing anchor irrevocable. Install that
                // publication before terminalising this intent so the caller
                // can retry its optimistic mutation against the winning rows
                // immediately instead of waiting for periodic anti-entropy.
                self.catch_up_committed_publication(&peers, &anchor_record)
                    .await?;
                self.mark_superseded_publication_if_still_current(intent)?;
                let publication = intent
                    .roots
                    .first()
                    .ok_or_else(|| anyhow!("CoreMeta successor publication has no root"))?;
                let actual_hash = hash_root_anchor_record(&committed)?;
                return Err(CoreStoreCommitError::RootChangedBeforeDurableStaging {
                    root_key_hash: committed.root_key_hash,
                    expected_generation: publication.expected_root_generation,
                    expected_hash: publication.publication.previous_root_hash.clone(),
                    actual_generation: committed.root_generation,
                    actual_hash,
                }
                .into());
            }
            super::local_coremeta_recovery::RootRegisterQuorumResolution::Indeterminate => {
                return Err(CoreStoreAvailabilityError::QuorumUnavailable {
                    operation: "successor_publication_resolution",
                    required: self.default_coremeta_quorum_profile()?.prepare_quorum,
                    received: 0,
                    details: format!(
                        "root={} generation={}",
                        original_anchor.root_key_hash, original_anchor.root_generation
                    ),
                }
                .into());
            }
            super::local_coremeta_recovery::RootRegisterQuorumResolution::DefinitivelyAbsent => {}
        }

        self.publish_single_root_intent_as_fenced_successor(intent, &outcomes)
            .await?;
        Ok(outcomes)
    }

    async fn publish_single_root_intent_as_fenced_successor(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<()> {
        self.ensure_publication_intent_active(intent).await?;
        let (publication_guards, guard_context) =
            self.acquire_publication_intent_locks(intent).await?;
        let current_intent = self
            .read_root_publication_intent(&intent.transaction_id)?
            .ok_or_else(|| anyhow!("CoreMeta publication intent disappeared before takeover"))?;
        if !publication_intent_retry_matches(&current_intent, intent)?
            || current_intent.state != intent.state
            || current_intent.terminal_reason != intent.terminal_reason
        {
            bail!("CoreMeta publication intent changed before takeover");
        }
        self.validate_publication_guards_at_linearization(&current_intent, guard_context.as_ref())
            .await?;

        let root = current_intent
            .roots
            .first()
            .ok_or_else(|| anyhow!("CoreMeta successor publication has no root"))?;
        let current_anchor = self
            .read_latest_root_anchor(&root.publication.descriptor.root_anchor_key)
            .await?;
        let current_generation = current_anchor
            .as_ref()
            .map_or(0, |anchor| anchor.root_generation);
        let current_hash = current_anchor
            .as_ref()
            .map(hash_root_anchor_record)
            .transpose()?
            .unwrap_or_else(|| ZERO_HASH.to_string());
        if current_generation != root.expected_root_generation
            || current_hash != root.publication.previous_root_hash
        {
            return Err(CoreStoreCommitError::RootChangedBeforeDurableStaging {
                root_key_hash: root.publication.descriptor.root_key_hash(),
                expected_generation: root.expected_root_generation,
                expected_hash: root.publication.previous_root_hash.clone(),
                actual_generation: current_generation,
                actual_hash: current_hash,
            }
            .into());
        }
        self.ensure_root_publication_owner(current_anchor.as_ref())
            .await?;

        let outcome = outcomes
            .first()
            .ok_or_else(|| anyhow!("CoreMeta successor publication has no quorum outcome"))?;
        let anchor = self.prepared_root_anchor_for_publisher(
            &root.publication,
            outcome,
            &current_intent.transaction_id,
            &self.node_identity.node_id,
        )?;
        let evidence = root_publication_evidence(std::slice::from_ref(&anchor), outcomes)?;
        self.publish_root_anchor_generation_with_participants(
            &anchor,
            &evidence,
            Some(&current_intent),
        )
        .await?;
        drop(publication_guards);

        if !self.root_generation_is_published(
            &anchor.root_key_hash,
            anchor.root_generation,
            &current_intent.transaction_id,
        )? {
            bail!("CoreMeta successor publication did not become visible");
        }
        if self
            .read_root_publication_intent(&current_intent.transaction_id)?
            .is_some()
        {
            bail!("CoreMeta successor publication did not clear its durable intent");
        }
        Ok(())
    }

    async fn materialize_locally_committed_publication_intent(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<bool> {
        let coordinator_index = effective_coordinator_index(intent)?;
        let coordinator_root = &intent.roots[coordinator_index];
        let coordinator_root_hash = coordinator_root.publication.descriptor.root_key_hash();
        let coordinator_generation = coordinator_root.publication.post_root_generation;
        let Some(inspection) = self
            .inspect_root_register_generation(&coordinator_root_hash, coordinator_generation)
            .await?
        else {
            return Ok(false);
        };
        let profile = self.default_coremeta_quorum_profile()?;
        if inspection.shard_indexes.len() < profile.prepare_quorum {
            return Ok(false);
        }
        let committed = decode_root_anchor_record(&inspection.root_anchor_record)?;
        validate_root_anchor_record(&committed)?;
        if committed.root_key_hash != coordinator_root_hash
            || committed.root_generation != coordinator_generation
        {
            bail!("committed local root-register quorum escaped its requested scope");
        }
        if publication_transaction_id(&committed)? != intent.transaction_id {
            return self.terminal_publication_guard_failure(
                intent,
                &format!(
                    "PublicationRootChanged: physical generation {} for root {} belongs to another transaction",
                    coordinator_generation, coordinator_root_hash
                ),
            );
        }

        // Only derive owner terms after physical Q2 has proved that this exact
        // intent owns the generation. A stale, uncommitted intent must never
        // reconstruct an anchor against a newer local head.
        let anchors = self.publication_anchors(intent, outcomes)?;
        let coordinator = &anchors[coordinator_index];
        let coordinator_record = encode_root_anchor_record(coordinator)?;
        if inspection.root_anchor_record != coordinator_record
            || inspection.root_anchor_hash != hash_root_anchor_record(coordinator)?
            || inspection.register_cohort_hash
                != super::local_root_register::root_register_cohort_hash(
                    &coordinator.root_key_hash,
                    coordinator.root_generation,
                    &inspection.register_cohort_nodes,
                )
        {
            bail!("committed local root-register quorum conflicts with publication intent");
        }
        self.materialize_committed_publication_intent(intent, &anchors)
            .await?;
        Ok(true)
    }

    async fn validate_publication_predecessors_at_linearization(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        for root in &intent.roots {
            let current = self
                .read_latest_root_anchor(&root.publication.descriptor.root_anchor_key)
                .await?;
            let actual_generation = current.as_ref().map_or(0, |anchor| anchor.root_generation);
            let actual_hash = current
                .as_ref()
                .map(hash_root_anchor_record)
                .transpose()?
                .unwrap_or_else(|| ZERO_HASH.to_string());
            if actual_generation < root.expected_root_generation {
                bail!(
                    "CoreMeta publication predecessor is not locally visible: root={} expected_generation={} actual_generation={}",
                    root.publication.descriptor.root_key_hash(),
                    root.expected_root_generation,
                    actual_generation
                );
            }
            if actual_generation != root.expected_root_generation
                || actual_hash != root.publication.previous_root_hash
            {
                return self.terminal_publication_guard_failure(
                    intent,
                    &format!(
                        "PublicationRootChanged: root={} expected_generation={} actual_generation={} expected_hash={} actual_hash={}",
                        root.publication.descriptor.root_key_hash(),
                        root.expected_root_generation,
                        actual_generation,
                        root.publication.previous_root_hash,
                        actual_hash
                    ),
                );
            }
        }
        Ok(())
    }

    async fn materialize_committed_publication_intent(
        &self,
        intent: &RootPublicationIntent,
        anchors: &[CoreRootAnchorRecord],
    ) -> Result<()> {
        let coordinator_index = effective_coordinator_index(intent)?;
        let coordinator = &anchors[coordinator_index];
        let coordinator_record = encode_root_anchor_record(coordinator)?;
        let participant_records = anchors
            .iter()
            .map(encode_root_anchor_record)
            .collect::<Result<Vec<_>>>()?;
        let expected_root_hash = if coordinator.previous_root_hash == ZERO_HASH {
            ""
        } else {
            coordinator.previous_root_hash.as_str()
        };
        self.compare_and_swap_publication_group_locally(
            &coordinator.root_key_hash,
            coordinator.root_generation.saturating_sub(1),
            expected_root_hash,
            &coordinator_record,
            &participant_records,
            Some(intent),
            RootPublicationAuthority::RegisterQuorum,
        )
        .await?;
        Ok(())
    }

    async fn completed_publication_matches_intent(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<bool> {
        let mut published_roots = Vec::with_capacity(intent.roots.len());
        for root in &intent.roots {
            published_roots.push(self.root_generation_is_published(
                &root.publication.descriptor.root_key_hash(),
                root.publication.post_root_generation,
                &intent.transaction_id,
            )?);
        }
        if published_roots.iter().all(|published| !published) {
            return Ok(false);
        }
        if published_roots.iter().any(|published| !published) {
            bail!("CoreMeta publication intent is only partially visible");
        }

        let outcomes = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        for root in &intent.roots {
            let root_key_hash = root.publication.descriptor.root_key_hash();
            let outcome = outcomes
                .get(root_key_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta completed publication outcome is missing"))?;
            let anchor = self
                .read_internal_root_anchor_by_hash(
                    &root_key_hash,
                    root.publication.post_root_generation,
                )
                .await?;
            let anchor = decode_root_anchor_record(&anchor.root_anchor_record)?;
            if anchor.root_anchor_key != root.publication.descriptor.root_anchor_key
                || anchor.root_key_hash != root_key_hash
                || anchor.root_generation != root.publication.post_root_generation
                || anchor.core_meta_commit_certificate_hash.as_deref()
                    != Some(outcome.certificate_hash.as_str())
                || publication_transaction_id(&anchor)? != intent.transaction_id
            {
                bail!("CoreMeta completed publication changed its durable intent");
            }
            self.validate_root_anchor_coremeta_commit_evidence(&anchor)?;
        }
        Ok(true)
    }

    pub(super) async fn recover_root_publication_intents(&self) -> Result<()> {
        let transaction_ids = self.root_publication_intent_ids()?;
        for transaction_id in transaction_ids {
            let Some(intent) = self.read_root_publication_intent(&transaction_id)? else {
                continue;
            };
            if intent.state == RootPublicationIntentState::Terminal {
                continue;
            }
            if intent.publisher_node_id != self.node_identity.node_id {
                continue;
            }
            self.resume_root_publication_intent_for_recovery(intent)
                .await?;
        }
        Ok(())
    }

    pub(in crate::core_store::local) async fn recover_distributed_root_publication_intents(
        &self,
        peers: &[super::local_coremeta_recovery::RecoveryPeer],
    ) -> Result<BTreeSet<String>> {
        let mut unresolved = BTreeSet::new();
        for transaction_id in self.root_publication_intent_ids()? {
            let Some(intent) = self.read_root_publication_intent(&transaction_id)? else {
                continue;
            };
            if intent.state == RootPublicationIntentState::Terminal
                || intent.publisher_node_id != self.node_identity.node_id
            {
                continue;
            }
            // Foreground publication holds this same lock set from its final
            // mutable checks through local materialization. Waiting here
            // prevents recovery from observing physical Q2 while participant
            // root-cache writes are still in flight.
            let (publication_guards, _) = self.acquire_publication_intent_locks(&intent).await?;
            let Some(current_intent) = self.read_root_publication_intent(&transaction_id)? else {
                drop(publication_guards);
                continue;
            };
            if !publication_intent_retry_matches(&current_intent, &intent)?
                || current_intent.state != intent.state
                || current_intent.terminal_reason != intent.terminal_reason
            {
                drop(publication_guards);
                bail!("CoreMeta publication intent changed during distributed recovery");
            }
            let intent = current_intent;
            let coordinator = &intent.roots[effective_coordinator_index(&intent)?];
            let root_key_hash = coordinator.publication.descriptor.root_key_hash();
            let generation = coordinator.publication.post_root_generation;
            match self
                .resolve_root_register_generation(
                    peers,
                    None,
                    &root_key_hash,
                    generation,
                )
                .await?
            {
                super::local_coremeta_recovery::RootRegisterGenerationResolution::Committed {
                    anchor_record,
                } => {
                    let committed = decode_root_anchor_record(&anchor_record)?;
                    let committed_transaction_id = publication_transaction_id(&committed)?;
                    if committed_transaction_id == intent.transaction_id {
                        self.materialize_own_committed_publication(
                            peers,
                            &intent,
                            &anchor_record,
                        )
                        .await?;
                    } else {
                        drop(publication_guards);
                        self.catch_up_committed_publication(peers, &anchor_record)
                            .await?;
                    }
                    if committed_transaction_id == intent.transaction_id {
                        if self
                            .read_root_publication_intent(&intent.transaction_id)?
                            .is_some()
                        {
                            bail!(
                                "CoreMeta committed publication catch-up did not clear its publisher intent"
                            );
                        }
                    } else {
                        self.mark_superseded_publication_if_still_current(&intent)?;
                    }
                }
                super::local_coremeta_recovery::RootRegisterGenerationResolution::DefinitivelyAbsent => {
                    drop(publication_guards);
                    self.resume_root_publication_intent_for_recovery(intent)
                        .await?;
                }
                super::local_coremeta_recovery::RootRegisterGenerationResolution::Indeterminate => {
                    drop(publication_guards);
                    unresolved.insert(transaction_id);
                }
            }
        }
        Ok(unresolved)
    }

    pub(super) async fn compare_and_swap_publication_group_locally(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        coordinator_record: &[u8],
        participant_records: &[Vec<u8>],
        publication_intent: Option<&RootPublicationIntent>,
        authority: RootPublicationAuthority,
    ) -> Result<CoreInternalRootAnchorRead> {
        let coordinator = decode_root_anchor_record(coordinator_record)?;
        let transaction_id = publication_transaction_id(&coordinator)?;
        let mut supplied = decode_participant_anchors(participant_records)?;
        let mut intent = match publication_intent {
            Some(intent) => {
                if !self.validate_persisted_root_publication_intent_summary(intent)? {
                    return self
                        .validate_completed_publication_retry(
                            root_key_hash_value,
                            expected_generation,
                            expected_root_hash,
                            coordinator_record,
                            participant_records,
                        )
                        .await;
                }
                intent.clone()
            }
            None => {
                let Some(intent) = self.read_root_publication_intent(transaction_id)? else {
                    return self
                        .validate_completed_publication_retry(
                            root_key_hash_value,
                            expected_generation,
                            expected_root_hash,
                            coordinator_record,
                            participant_records,
                        )
                        .await;
                };
                intent
            }
        };
        intent.ensure_pending()?;
        let Some(current_intent) = self.read_root_publication_intent(transaction_id)? else {
            return self
                .validate_completed_publication_retry(
                    root_key_hash_value,
                    expected_generation,
                    expected_root_hash,
                    coordinator_record,
                    participant_records,
                )
                .await;
        };
        if !publication_intent_retry_matches(&current_intent, &intent)?
            || current_intent.state != intent.state
            || current_intent.terminal_reason != intent.terminal_reason
        {
            bail!("CoreMeta publication intent changed before linearization");
        }
        intent = current_intent;
        if supplied.len() != intent.roots.len() {
            bail!("CoreMeta publication participant anchor cardinality mismatch");
        }
        for root in &mut intent.roots {
            let root_hash = root.publication.descriptor.root_key_hash();
            let anchor = supplied
                .get(&root_hash)
                .ok_or_else(|| anyhow!("CoreMeta publication participant anchor is missing"))?;
            let certificate_hash = anchor
                .core_meta_commit_certificate_hash
                .clone()
                .ok_or_else(|| anyhow!("CoreMeta publication participant has no certificate"))?;
            if let Some(expected) = &root.certificate_hash {
                if expected != &certificate_hash {
                    bail!("CoreMeta publication participant certificate mismatch");
                }
            }
            root.certificate_hash = Some(certificate_hash);
        }
        let outcomes = self.root_publication_outcomes(&intent)?;
        let expected_anchors =
            self.publication_anchors_with_authority(&intent, &outcomes, &supplied, authority)?;
        let coordinator_index = effective_coordinator_index(&intent)?;
        let expected_coordinator = &expected_anchors[coordinator_index];
        if expected_coordinator.root_key_hash != root_key_hash_value
            || expected_coordinator.root_generation != expected_generation.saturating_add(1)
            || encode_root_anchor_record(expected_coordinator)? != coordinator_record
        {
            bail!("CoreMeta publication coordinator anchor mismatch");
        }
        for anchor in &expected_anchors {
            let supplied_anchor = supplied
                .remove(&anchor.root_key_hash)
                .ok_or_else(|| anyhow!("CoreMeta publication participant anchor is missing"))?;
            if encode_root_anchor_record(anchor)? != encode_root_anchor_record(&supplied_anchor)? {
                bail!("CoreMeta publication participant anchor bytes changed");
            }
            let expected_hash = if anchor.previous_root_hash == ZERO_HASH {
                ""
            } else {
                anchor.previous_root_hash.as_str()
            };
            if let Err(error) = self
                .validate_root_cas_precondition(
                    &anchor.root_key_hash,
                    anchor.root_generation.saturating_sub(1),
                    expected_hash,
                    anchor,
                )
                .await
            {
                return self.terminal_publication_guard_failure(
                    &intent,
                    &format!("PublicationRootChanged: {error:#}"),
                );
            }
            self.validate_root_anchor_coremeta_commit_evidence(anchor)?;
        }
        if !supplied.is_empty() {
            bail!("CoreMeta publication includes unexpected participant anchors");
        }
        self.apply_publication_group_atomically(&intent, &expected_anchors)
            .await?;
        internal_root_read_from_anchor(expected_coordinator)
    }

    pub(super) fn publication_anchors(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<Vec<CoreRootAnchorRecord>> {
        let outcomes = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        intent
            .roots
            .iter()
            .map(|root| {
                let root_hash = root.publication.descriptor.root_key_hash();
                let outcome = outcomes
                    .get(root_hash.as_str())
                    .ok_or_else(|| anyhow!("CoreMeta publication outcome is missing"))?;
                self.prepared_root_anchor_for_publisher(
                    &root.publication,
                    outcome,
                    &intent.transaction_id,
                    &intent.publisher_node_id,
                )
            })
            .collect()
    }

    fn publication_anchors_with_authority(
        &self,
        intent: &RootPublicationIntent,
        outcomes: &[CoreMetaQuorumCommitOutcome],
        supplied: &BTreeMap<String, CoreRootAnchorRecord>,
        authority: RootPublicationAuthority,
    ) -> Result<Vec<CoreRootAnchorRecord>> {
        if authority == RootPublicationAuthority::LocalOwnerState {
            return self.publication_anchors(intent, outcomes);
        }
        let outcomes = outcomes
            .iter()
            .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
            .collect::<BTreeMap<_, _>>();
        intent
            .roots
            .iter()
            .map(|root| {
                let root_hash = root.publication.descriptor.root_key_hash();
                let outcome = outcomes
                    .get(root_hash.as_str())
                    .ok_or_else(|| anyhow!("CoreMeta publication outcome is missing"))?;
                let anchor = supplied
                    .get(&root_hash)
                    .ok_or_else(|| anyhow!("CoreMeta recovered publication anchor is missing"))?;
                self.prepared_root_anchor_with_owner_terms(
                    &root.publication,
                    outcome,
                    &intent.transaction_id,
                    RootOwnerTerms {
                        owner_node_id: anchor.publisher_node_id.clone(),
                        owner_epoch: anchor.publisher_epoch,
                        owner_fence: anchor.partition_owner_fence,
                    },
                )
            })
            .collect()
    }

    async fn apply_publication_group_atomically(
        &self,
        intent: &RootPublicationIntent,
        anchors: &[CoreRootAnchorRecord],
    ) -> Result<()> {
        let mut rows = intent.local_rows.clone();
        for root in &intent.roots {
            rows.extend(root.rows.clone());
        }
        rows.extend(self.coremeta_generation_history_rows_for_publication_intent(intent)?);
        rows.extend(self.root_cache_rows_for_publication(anchors)?);
        rows.extend(self.root_publication_intent_delete_rows(intent)?);
        ensure_unique_physical_rows(&rows)?;
        // Q2 root-register preparation already committed this exact anchor
        // while the publisher held all mutable guard locks. This WriteBatch is
        // deterministic materialisation of that decision, not a second point
        // at which later state may veto it.
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&rows))
    }

    fn root_cache_rows_for_publication(
        &self,
        anchors: &[CoreRootAnchorRecord],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut owned = Vec::with_capacity(anchors.len().saturating_mul(3));
        for anchor in anchors {
            let payload = encode_root_cache_row(anchor)?;
            let generation_key =
                root_anchor_generation_key(&anchor.root_key_hash, anchor.root_generation);
            let latest_key = root_cache_key(&anchor.root_anchor_key);
            let hash_key = root_cache_hash_key(&anchor.root_key_hash);
            let ops = [
                CoreMetaBatchOp {
                    cf: CF_ROOT_CACHE,
                    table_id: TABLE_ROOT_CACHE_ROW,
                    tuple_key: generation_key.as_slice(),
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(payload.as_slice()),
                },
                CoreMetaBatchOp {
                    cf: CF_ROOT_CACHE,
                    table_id: TABLE_ROOT_CACHE_ROW,
                    tuple_key: latest_key.as_slice(),
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(payload.as_slice()),
                },
                CoreMetaBatchOp {
                    cf: CF_ROOT_CACHE,
                    table_id: TABLE_ROOT_CACHE_ROW,
                    tuple_key: hash_key.as_slice(),
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(payload.as_slice()),
                },
            ];
            owned.extend(self.meta.encode_batch_ops(&ops)?);
        }
        Ok(owned)
    }

    fn root_publication_intent_delete_rows(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let stored = encode_intent_rows(intent)?;
        let ops = stored
            .iter()
            .map(|row| CoreMetaBatchOp {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
                tuple_key: row.tuple_key.as_slice(),
                common: None,
                kind: CoreMetaBatchOpKind::Delete,
            })
            .collect::<Vec<_>>();
        self.meta.encode_batch_ops(&ops)
    }

    pub(in crate::core_store::local) fn root_publication_intent_ids(&self) -> Result<Vec<String>> {
        let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("root-publication-intent")])?;
        let mut after = None;
        let mut ids = Vec::new();
        loop {
            let page = self.meta.scan_prefix_page(
                CF_TRANSACTIONS,
                TABLE_ROOT_PUBLICATION_INTENT_ROW,
                &prefix,
                after.as_deref(),
                CORE_META_MAX_SCAN_PAGE_ROWS,
            )?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                let header = decode_canonical::<PublicationIntentHeaderProto>(
                    &record.payload,
                    "CoreMeta root publication intent",
                )?;
                validate_header(&header)?;
                ids.push(header.transaction_id);
            }
            after = page
                .last()
                .map(|record| core_meta_record_tuple_key(&record.key).map(ToOwned::to_owned))
                .transpose()?;
            if page.len() < CORE_META_MAX_SCAN_PAGE_ROWS {
                break;
            }
        }
        ids.sort();
        ids.dedup();
        Ok(ids)
    }

    pub(in crate::core_store::local) fn has_owned_pending_root_publication_intents(
        &self,
    ) -> Result<bool> {
        for transaction_id in self.root_publication_intent_ids()? {
            let Some(intent) = self.read_root_publication_intent(&transaction_id)? else {
                continue;
            };
            if intent.state == RootPublicationIntentState::Pending
                && intent.publisher_node_id == self.node_identity.node_id
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn validate_completed_publication_retry(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        coordinator_record: &[u8],
        participant_records: &[Vec<u8>],
    ) -> Result<CoreInternalRootAnchorRead> {
        let coordinator = decode_root_anchor_record(coordinator_record)?;
        self.validate_root_cas_precondition(
            root_key_hash_value,
            expected_generation,
            expected_root_hash,
            &coordinator,
        )
        .await?;
        for anchor in decode_participant_anchors(participant_records)?.into_values() {
            let current = self
                .read_internal_root_anchor_by_hash(&anchor.root_key_hash, anchor.root_generation)
                .await?;
            if current.root_anchor_record != encode_root_anchor_record(&anchor)? {
                bail!("CoreMeta completed publication retry changed a participant anchor");
            }
        }
        internal_root_read_from_anchor(&coordinator)
    }
}

fn effective_coordinator_index(intent: &RootPublicationIntent) -> Result<usize> {
    let coordinators = intent
        .roots
        .iter()
        .enumerate()
        .filter_map(|(index, root)| {
            root.publication
                .descriptor
                .transaction_coordinator
                .then_some(index)
        })
        .collect::<Vec<_>>();
    match coordinators.as_slice() {
        [index] => Ok(*index),
        [] if intent.roots.len() == 1 => Ok(0),
        [] => bail!("CoreMeta multi-root publication has no coordinator"),
        _ => bail!("CoreMeta publication has multiple coordinators"),
    }
}

pub(super) fn root_publication_evidence(
    anchors: &[CoreRootAnchorRecord],
    outcomes: &[CoreMetaQuorumCommitOutcome],
) -> Result<Vec<crate::anvil_api::CoreMetaRootPublicationEvidence>> {
    let outcomes = outcomes
        .iter()
        .map(|outcome| (outcome.root_key_hash.as_str(), outcome))
        .collect::<BTreeMap<_, _>>();
    let mut evidence = Vec::with_capacity(anchors.len());
    for anchor in anchors {
        let outcome = outcomes
            .get(anchor.root_key_hash.as_str())
            .ok_or_else(|| anyhow!("CoreMeta publication participant outcome is missing"))?;
        let certificate = decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
            &outcome.certificate_bytes,
            "CoreMeta publication certificate",
        )?;
        if certificate.root_key_hash != anchor.root_key_hash
            || certificate.post_root_generation != anchor.root_generation
            || anchor.core_meta_commit_certificate_hash.as_deref()
                != Some(certificate.certificate_hash.as_str())
        {
            bail!("CoreMeta publication participant evidence does not match its anchor");
        }
        evidence.push(crate::anvil_api::CoreMetaRootPublicationEvidence {
            root_anchor_record: encode_root_anchor_record(anchor)?,
            commit_certificate: Some(certificate),
            committed_batch_hash: outcome.committed_batch_hash.clone(),
            certificate_persist_receipts: outcome
                .certificate_persist_receipts
                .iter()
                .map(core_persist_receipt_to_api)
                .collect(),
        });
    }
    Ok(evidence)
}

pub(super) fn publication_intent_retry_matches(
    existing: &RootPublicationIntent,
    candidate: &RootPublicationIntent,
) -> Result<bool> {
    if existing.transaction_id != candidate.transaction_id
        || existing.plan_hash != candidate.plan_hash
        || existing.publisher_node_id != candidate.publisher_node_id
        || existing.created_at_unix_nanos != candidate.created_at_unix_nanos
        || existing.guard != candidate.guard
        || existing.state != candidate.state
        || existing.terminal_reason != candidate.terminal_reason
        || existing.roots.len() != candidate.roots.len()
        || rows_hash(&existing.local_rows) != rows_hash(&candidate.local_rows)
    {
        return Ok(false);
    }
    for (existing_root, candidate_root) in existing.roots.iter().zip(&candidate.roots) {
        let mut existing_proto = root_to_proto(existing, existing_root)?;
        let mut candidate_proto = root_to_proto(candidate, candidate_root)?;
        existing_proto.certificate_hash = None;
        candidate_proto.certificate_hash = None;
        if encode_deterministic_proto(&existing_proto)
            != encode_deterministic_proto(&candidate_proto)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn publication_intent_local_rows_match(
    intent: &RootPublicationIntent,
    local_rows: &[CoreMetaEncodedOwnedRow],
) -> bool {
    rows_hash(&intent.local_rows) == rows_hash(local_rows)
}

pub(super) fn publication_transaction_id(anchor: &CoreRootAnchorRecord) -> Result<&str> {
    match (
        anchor.mutation_first.as_deref(),
        anchor.mutation_last.as_deref(),
    ) {
        (Some(first), Some(last)) if first == last && !first.is_empty() => Ok(first),
        _ => bail!("CoreMeta publication anchor has no single transaction identity"),
    }
}

fn decode_participant_anchors(
    records: &[Vec<u8>],
) -> Result<BTreeMap<String, CoreRootAnchorRecord>> {
    if records.is_empty() || records.len() > MAX_PUBLICATION_ROOTS {
        bail!("CoreMeta publication participant anchor count is outside the bounded range");
    }
    let mut anchors = BTreeMap::new();
    let mut anchor_keys = BTreeSet::new();
    for record in records {
        let anchor = decode_root_anchor_record(record)?;
        if !anchor_keys.insert(anchor.root_anchor_key.clone())
            || anchors
                .insert(anchor.root_key_hash.clone(), anchor)
                .is_some()
        {
            bail!("CoreMeta publication contains duplicate participant anchors");
        }
    }
    Ok(anchors)
}

fn internal_root_read_from_anchor(
    anchor: &CoreRootAnchorRecord,
) -> Result<CoreInternalRootAnchorRead> {
    let bytes = encode_root_anchor_record(anchor)?;
    Ok(CoreInternalRootAnchorRead {
        root_key_hash: anchor.root_key_hash.clone(),
        generation: anchor.root_generation,
        root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        root_anchor_record: bytes,
    })
}

fn ensure_unique_physical_rows(rows: &[CoreMetaEncodedOwnedRow]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for row in rows {
        if !seen.insert((row.cf.as_str(), row.core_meta_key.as_slice())) {
            bail!("CoreMeta publication atomic batch contains a duplicate physical row");
        }
    }
    Ok(())
}

pub(in crate::core_store) fn validate_root_publication_intent_row(payload: &[u8]) -> Result<()> {
    let probe = PublicationSchemaProbe::decode(payload)?;
    match probe.schema.as_str() {
        PUBLICATION_INTENT_SCHEMA => {
            let row = decode_canonical::<PublicationIntentHeaderProto>(
                payload,
                "CoreMeta root publication intent",
            )?;
            validate_header(&row)
        }
        PUBLICATION_ROOT_SCHEMA => {
            let row = decode_canonical::<PublicationRootProto>(
                payload,
                "CoreMeta root publication root row",
            )?;
            validate_common(
                row.common.as_ref(),
                &row.transaction_id,
                row.created_at_unix_nanos,
            )?;
            if row.schema != PUBLICATION_ROOT_SCHEMA || row.row_count == 0 {
                bail!("CoreMeta root publication root row is invalid");
            }
            Ok(())
        }
        PUBLICATION_ROW_SCHEMA => {
            let row = decode_canonical::<PublicationRowProto>(
                payload,
                "CoreMeta root publication row header",
            )?;
            let common = row
                .common
                .as_ref()
                .ok_or_else(|| anyhow!("CoreMeta publication row common metadata is missing"))?;
            validate_common(
                Some(common),
                &row.transaction_id,
                common.created_at_unix_nanos,
            )?;
            if row.schema != PUBLICATION_ROW_SCHEMA || row.chunk_count == 0 {
                bail!("CoreMeta root publication row header is invalid");
            }
            Ok(())
        }
        PUBLICATION_CHUNK_SCHEMA => {
            let row = decode_canonical::<PublicationChunkProto>(
                payload,
                "CoreMeta root publication row chunk",
            )?;
            let common = row
                .common
                .as_ref()
                .ok_or_else(|| anyhow!("CoreMeta publication chunk common metadata is missing"))?;
            validate_common(
                Some(common),
                &row.transaction_id,
                common.created_at_unix_nanos,
            )?;
            if row.schema != PUBLICATION_CHUNK_SCHEMA
                || row.chunk_count == 0
                || row.chunk_ordinal >= row.chunk_count
                || row.bytes.len() > PUBLICATION_ROW_CHUNK_BYTES
            {
                bail!("CoreMeta root publication row chunk is invalid");
            }
            Ok(())
        }
        _ => bail!("CoreMeta root publication intent row has unknown schema"),
    }
}
