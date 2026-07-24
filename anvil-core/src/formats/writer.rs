use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use crate::core_store::{
    CORE_META_MAX_INLINE_PAYLOAD_BYTES, CoreBoundaryValue, CoreLogicalRangeHint, CoreMetaBatchOp,
    CoreMetaBatchOpKind, CoreMetaRowCommonProto, CoreMetaVisibilityState, CoreObjectRef,
    CorePipelinePolicy, CoreSharedRangeMarker, CoreTraceContext, WriteLogicalFilePathRequest,
    WriteLogicalFileRequest, core_meta_root_key_hash, sha256_hex,
};

use super::{EncodedWriterSegment, FileFamily, Hash32, RangeIndexEntry, encode_writer_segment};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriterFamily {
    ObjectBlob,
    Stream,
    FullText,
    Vector,
    TypedMetadata,
    Authz,
    PersonalDb,
    GitSource,
    Registry,
    MeshControl,
    CoreControl,
}

impl WriterFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ObjectBlob => "object_blob",
            Self::Stream => "stream",
            Self::FullText => "full_text",
            Self::Vector => "vector",
            Self::TypedMetadata => "typed_index",
            Self::Authz => "authz",
            Self::PersonalDb => "personaldb",
            Self::GitSource => "git_source",
            Self::Registry => "registry",
            Self::MeshControl => "mesh_control",
            Self::CoreControl => "core_control",
        }
    }

    pub fn all() -> [Self; 11] {
        [
            Self::ObjectBlob,
            Self::Stream,
            Self::FullText,
            Self::Vector,
            Self::TypedMetadata,
            Self::Authz,
            Self::PersonalDb,
            Self::GitSource,
            Self::Registry,
            Self::MeshControl,
            Self::CoreControl,
        ]
    }

    pub fn from_name(value: &str) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|family| family.as_str() == value)
    }
}

pub fn canonical_logical_file_id(
    writer_family: WriterFamily,
    writer_generation: u64,
    stable_name: &str,
    content_hash: &[u8],
) -> String {
    let mut bytes = Vec::new();
    for part in [
        "anvil.logical_file_id.v1",
        writer_family.as_str(),
        &writer_generation.to_string(),
        stable_name,
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes.extend_from_slice(&(content_hash.len() as u64).to_le_bytes());
    bytes.extend_from_slice(content_hash);
    format!("lf_{}", sha256_hex(&bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteSource {
    InlineBytes(Vec<u8>),
    LandedBytes {
        landing_id: String,
        hash: String,
        length: u64,
        relative_path: String,
    },
    TempFile {
        path: std::path::PathBuf,
        hash: String,
        length: u64,
    },
    ExistingCoreObject {
        object_ref: CoreObjectRef,
        byte_start: u64,
        byte_end: u64,
    },
}

impl ByteSource {
    pub fn inline(bytes: Vec<u8>) -> Self {
        Self::InlineBytes(bytes)
    }

    pub fn logical_len(&self) -> u64 {
        match self {
            Self::InlineBytes(bytes) => bytes.len() as u64,
            Self::LandedBytes { length, .. } | Self::TempFile { length, .. } => *length,
            Self::ExistingCoreObject {
                byte_start,
                byte_end,
                ..
            } => byte_end.saturating_sub(*byte_start),
        }
    }

    pub fn into_inline_bytes(self) -> Result<Vec<u8>> {
        match self {
            Self::InlineBytes(bytes) => Ok(bytes),
            Self::LandedBytes { .. } | Self::TempFile { .. } | Self::ExistingCoreObject { .. } => {
                bail!(
                    "ByteSource must be materialised through CoreStore admission before this call"
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeStatistics {
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryStrength {
    Required,
    Preferred,
    DiagnosticOnly,
}

impl BoundaryStrength {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Preferred => "preferred",
            Self::DiagnosticOnly => "diagnostic_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRangeMarker {
    pub record_kind: String,
    pub reason: String,
    pub boundary_dimension_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalRangeHint {
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub statistics: RangeStatistics,
    pub preferred_block_boundary: BoundaryStrength,
    pub prefetch_group: Option<String>,
    pub shared_range: Option<SharedRangeMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    InlineMetadata,
    ErasureCodedBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub policy_id: String,
    pub target_segment_bytes: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            policy_id: "corestore-default".to_string(),
            target_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadProfile {
    pub profile_id: String,
    pub prefetch_group: Option<String>,
}

impl Default for ReadProfile {
    fn default() -> Self {
        Self {
            profile_id: "range-read-default".to_string(),
            prefetch_group: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalFileWrite {
    pub logical_file_id: String,
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub bytes: ByteSource,
    pub ranges: Vec<LogicalRangeHint>,
    pub durability_class: DurabilityClass,
    pub compaction_policy: CompactionPolicy,
    pub read_profile: ReadProfile,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
}

impl LogicalFileWrite {
    pub fn into_write_logical_file_request(self) -> Result<WriteLogicalFileRequest> {
        let source = self.bytes.into_inline_bytes()?;
        Ok(WriteLogicalFileRequest {
            writer_family: self.writer_family.as_str().to_string(),
            generation: self.writer_generation,
            logical_file_id: self.logical_file_id,
            source,
            range_hints: writer_ranges_to_core_hints(self.ranges),
            pipeline_policy: self.pipeline_policy,
            trace_context: self.trace_context,
            boundary_values: self.boundary_values,
            mutation_id: self.mutation_id,
            region_id: self.region_id,
        })
    }

    pub fn into_write_logical_file_path_request(
        self,
        source_path: std::path::PathBuf,
        source_hash: String,
        source_len: u64,
    ) -> WriteLogicalFilePathRequest {
        WriteLogicalFilePathRequest {
            writer_family: self.writer_family.as_str().to_string(),
            generation: self.writer_generation,
            logical_file_id: self.logical_file_id,
            source_path,
            source_len,
            source_hash,
            range_hints: writer_ranges_to_core_hints(self.ranges),
            pipeline_policy: self.pipeline_policy,
            trace_context: self.trace_context,
            boundary_values: self.boundary_values,
            mutation_id: self.mutation_id,
            region_id: self.region_id,
        }
    }
}

fn writer_ranges_to_core_hints(ranges: Vec<LogicalRangeHint>) -> Vec<CoreLogicalRangeHint> {
    ranges
        .into_iter()
        .map(|range| CoreLogicalRangeHint {
            range_id: format!("{}-{}", range.byte_start, range.byte_end),
            byte_start: range.byte_start,
            byte_end: range.byte_end,
            writer_record_kind: range.writer_record_kind,
            boundary_values: range.boundary_values,
            writer_statistics: range.statistics.encoded_bytes,
            preferred_block_boundary: range.preferred_block_boundary.as_str().to_string(),
            boundary_dimension_ids: Vec::new(),
            prefetch_next_range_ids: range.prefetch_group.into_iter().collect(),
            shared_range: range.shared_range.map(|marker| CoreSharedRangeMarker {
                record_kind: marker.record_kind,
                reason: marker.reason,
                boundary_dimension_ids: marker.boundary_dimension_ids,
            }),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoreMetaMutationKind {
    Put(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMetaMutationScope {
    root_anchor_key: Option<String>,
    post_root_generation: u64,
}

impl CoreMetaMutationScope {
    pub fn local() -> Self {
        Self {
            root_anchor_key: None,
            post_root_generation: 0,
        }
    }

    pub fn rooted(root_anchor_key: impl Into<String>, post_root_generation: u64) -> Result<Self> {
        let root_anchor_key = root_anchor_key.into();
        validate_canonical_root_anchor_key(&root_anchor_key)?;
        if post_root_generation == 0 {
            bail!("CoreFormatWriter rooted mutation generation must be nonzero");
        }
        Ok(Self {
            root_anchor_key: Some(root_anchor_key),
            post_root_generation,
        })
    }

    pub fn root_anchor_key(&self) -> Option<&str> {
        self.root_anchor_key.as_deref()
    }

    pub fn post_root_generation(&self) -> Option<u64> {
        self.root_anchor_key
            .as_ref()
            .map(|_| self.post_root_generation)
    }

    pub fn root_key_hash(&self) -> Option<String> {
        self.root_anchor_key().map(core_meta_root_key_hash)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterRootPublication {
    root_anchor_key: String,
    writer_families: Vec<WriterFamily>,
    transaction_coordinator: bool,
}

impl WriterRootPublication {
    pub fn new(
        root_anchor_key: impl Into<String>,
        writer_families: Vec<WriterFamily>,
    ) -> Result<Self> {
        let root_anchor_key = root_anchor_key.into();
        validate_canonical_root_anchor_key(&root_anchor_key)?;
        validate_sorted_writer_families(&writer_families)?;
        Ok(Self {
            root_anchor_key,
            writer_families,
            transaction_coordinator: false,
        })
    }

    pub fn coordinator(mut self) -> Self {
        self.transaction_coordinator = true;
        self
    }

    pub fn root_anchor_key(&self) -> &str {
        &self.root_anchor_key
    }

    pub fn writer_families(&self) -> &[WriterFamily] {
        &self.writer_families
    }

    pub fn is_transaction_coordinator(&self) -> bool {
        self.transaction_coordinator
    }
}

fn validate_canonical_root_anchor_key(root_anchor_key: &str) -> Result<()> {
    if root_anchor_key.is_empty()
        || root_anchor_key.len() > 1024
        || root_anchor_key.starts_with('/')
        || root_anchor_key.ends_with('/')
        || root_anchor_key
            .split('/')
            .any(|part| part.is_empty() || part.chars().any(char::is_control))
    {
        bail!("CoreFormatWriter root anchor key is not canonical");
    }
    Ok(())
}

fn validate_sorted_writer_families(writer_families: &[WriterFamily]) -> Result<()> {
    if writer_families.is_empty() {
        bail!("CoreFormatWriter root publication must name at least one writer family");
    }
    let mut canonical = writer_families.to_vec();
    canonical.sort_by_key(|family| family.as_str());
    canonical.dedup();
    if canonical != writer_families {
        bail!("CoreFormatWriter root publication writer families must be sorted and unique");
    }
    Ok(())
}

fn validate_format_logical_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("CoreFormatWriter {label} must not be empty");
    }
    if value.contains('\0') || value.contains("..") {
        bail!("CoreFormatWriter {label} contains an invalid component");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetaMutation {
    pub column_family: &'static str,
    pub table_id: u16,
    pub tuple_key: Vec<u8>,
    pub kind: CoreMetaMutationKind,
    pub common: Option<CoreMetaRowCommonProto>,
    pub transaction_id: String,
    scope: CoreMetaMutationScope,
}

impl CoreMetaMutation {
    pub fn put(
        column_family: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
        scope: CoreMetaMutationScope,
        transaction_id: String,
    ) -> Result<Self> {
        let mutation = Self {
            column_family,
            table_id,
            tuple_key,
            kind: CoreMetaMutationKind::Put(payload),
            common,
            transaction_id,
            scope,
        };
        mutation.validate()?;
        Ok(mutation)
    }

    pub fn delete(
        column_family: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
        scope: CoreMetaMutationScope,
        transaction_id: String,
    ) -> Result<Self> {
        let mutation = Self {
            column_family,
            table_id,
            tuple_key,
            kind: CoreMetaMutationKind::Delete,
            common,
            transaction_id,
            scope,
        };
        mutation.validate()?;
        Ok(mutation)
    }

    pub fn scope(&self) -> &CoreMetaMutationScope {
        &self.scope
    }

    pub fn validate(&self) -> Result<()> {
        validate_format_logical_id(&self.transaction_id, "mutation transaction id")?;
        match self.scope.root_anchor_key() {
            Some(root_anchor_key) => {
                let common = self.common.as_ref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "CoreFormatWriter rooted mutation must carry explicit common metadata"
                    )
                })?;
                if common.root_key_hash != core_meta_root_key_hash(root_anchor_key) {
                    bail!(
                        "CoreFormatWriter mutation common root hash does not match its canonical root anchor key"
                    );
                }
                if Some(common.root_generation) != self.scope.post_root_generation() {
                    bail!(
                        "CoreFormatWriter mutation common generation does not match its declared root generation"
                    );
                }
                if common.transaction_id != self.transaction_id {
                    bail!(
                        "CoreFormatWriter mutation common transaction id does not match its mutation transaction id"
                    );
                }
                if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
                    bail!("CoreFormatWriter rooted mutation must be committed");
                }
            }
            None => {
                if let Some(common) = self.common.as_ref() {
                    if !common.root_key_hash.is_empty() || common.root_generation != 0 {
                        bail!(
                            "CoreFormatWriter local mutation must not carry rooted common metadata"
                        );
                    }
                    if !common.transaction_id.is_empty()
                        && common.transaction_id != self.transaction_id
                    {
                        bail!(
                            "CoreFormatWriter local mutation common transaction id does not match its mutation transaction id"
                        );
                    }
                    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
                        bail!("CoreFormatWriter local mutation must be committed");
                    }
                }
            }
        }
        Ok(())
    }

    pub fn as_batch_op(&self) -> CoreMetaBatchOp<'_> {
        let kind = match &self.kind {
            CoreMetaMutationKind::Put(payload) => CoreMetaBatchOpKind::Put(payload),
            CoreMetaMutationKind::Delete => CoreMetaBatchOpKind::Delete,
        };
        CoreMetaBatchOp {
            cf: self.column_family,
            table_id: self.table_id,
            tuple_key: &self.tuple_key,
            common: self.common.clone(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterPlanInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub estimated_bytes: u64,
    pub estimated_records: u64,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterBuildInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub source_bytes: Vec<u8>,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterRecoveryInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub observed_coremeta_rows: Vec<Vec<u8>>,
    pub observed_logical_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterPlan {
    pub logical_files: Vec<LogicalFileWrite>,
    pub core_meta_mutations: Vec<CoreMetaMutation>,
    pub core_meta_root_publications: Vec<WriterRootPublication>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterBuildOutput {
    pub logical_files: Vec<LogicalFileWrite>,
    pub core_meta_mutations: Vec<CoreMetaMutation>,
    pub core_meta_root_publications: Vec<WriterRootPublication>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterRecoveryPlan {
    pub mutations_to_commit: Vec<CoreMetaMutation>,
    pub core_meta_root_publications: Vec<WriterRootPublication>,
    pub logical_files_to_rewrite: Vec<LogicalFileWrite>,
    pub orphaned_logical_files: Vec<String>,
}

pub trait CoreFormatWriter {
    fn family(&self) -> WriterFamily;

    fn plan(&self, input: WriterPlanInput) -> Result<WriterPlan>;

    fn build(&self, input: WriterBuildInput) -> Result<WriterBuildOutput>;

    fn recover(&self, input: WriterRecoveryInput) -> Result<WriterRecoveryPlan>;
}

#[derive(Default)]
pub struct CoreFormatWriterRegistry {
    writers: BTreeMap<WriterFamily, Box<dyn CoreFormatWriter + Send + Sync>>,
}

impl CoreFormatWriterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default_opaque_writers() -> Self {
        let mut registry = Self::new();
        for family in WriterFamily::all() {
            registry.register(Box::new(OpaqueLogicalFileWriter::new(family)));
        }
        registry
    }

    pub fn register(&mut self, writer: Box<dyn CoreFormatWriter + Send + Sync>) {
        self.writers.insert(writer.family(), writer);
    }

    pub fn get(&self, family: WriterFamily) -> Option<&(dyn CoreFormatWriter + Send + Sync)> {
        self.writers.get(&family).map(|writer| writer.as_ref())
    }

    pub fn require(&self, family: WriterFamily) -> Result<&(dyn CoreFormatWriter + Send + Sync)> {
        self.get(family).ok_or_else(|| {
            anyhow::anyhow!("CoreFormatWriter is not registered for {}", family.as_str())
        })
    }

    pub fn registered_families(&self) -> BTreeSet<WriterFamily> {
        self.writers.keys().copied().collect()
    }
}

#[derive(Debug, Clone)]
pub struct OpaqueLogicalFileWriter {
    family: WriterFamily,
}

impl OpaqueLogicalFileWriter {
    pub fn new(family: WriterFamily) -> Self {
        Self { family }
    }
}

impl CoreFormatWriter for OpaqueLogicalFileWriter {
    fn family(&self) -> WriterFamily {
        self.family
    }

    fn plan(&self, input: WriterPlanInput) -> Result<WriterPlan> {
        let bytes = Vec::with_capacity(input.estimated_bytes.min(usize::MAX as u64) as usize);
        let plan_hash = sha256_hex(
            format!(
                "{}\0{}\0{}\0{}",
                input.writer_family.as_str(),
                input.writer_generation,
                input.estimated_bytes,
                input.estimated_records
            )
            .as_bytes(),
        );
        Ok(WriterPlan {
            logical_files: vec![LogicalFileWrite {
                logical_file_id: canonical_logical_file_id(
                    input.writer_family,
                    input.writer_generation,
                    &input.logical_file_id,
                    plan_hash.as_bytes(),
                ),
                writer_family: input.writer_family,
                writer_generation: input.writer_generation,
                bytes: ByteSource::InlineBytes(bytes),
                ranges: Vec::new(),
                durability_class: DurabilityClass::ErasureCodedBytes,
                compaction_policy: CompactionPolicy::default(),
                read_profile: ReadProfile::default(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: input.boundary_values,
                mutation_id: format!(
                    "writer-plan-{}",
                    sha256_hex(
                        format!(
                            "{}\0{}\0{}",
                            input.writer_family.as_str(),
                            input.writer_generation,
                            plan_hash
                        )
                        .as_bytes()
                    )
                ),
                region_id: input
                    .options
                    .get("region_id")
                    .cloned()
                    .unwrap_or_else(|| "local".to_string()),
            }],
            core_meta_mutations: Vec::new(),
            core_meta_root_publications: Vec::new(),
        })
    }

    fn build(&self, input: WriterBuildInput) -> Result<WriterBuildOutput> {
        let source_hash = sha256_hex(&input.source_bytes);
        Ok(WriterBuildOutput {
            logical_files: vec![LogicalFileWrite {
                logical_file_id: canonical_logical_file_id(
                    input.writer_family,
                    input.writer_generation,
                    &input.logical_file_id,
                    source_hash.as_bytes(),
                ),
                writer_family: input.writer_family,
                writer_generation: input.writer_generation,
                bytes: ByteSource::InlineBytes(input.source_bytes),
                ranges: Vec::new(),
                durability_class: DurabilityClass::ErasureCodedBytes,
                compaction_policy: CompactionPolicy::default(),
                read_profile: ReadProfile::default(),
                pipeline_policy: input.pipeline_policy,
                trace_context: input.trace_context,
                boundary_values: input.boundary_values,
                mutation_id: input.mutation_id,
                region_id: input.region_id,
            }],
            core_meta_mutations: Vec::new(),
            core_meta_root_publications: Vec::new(),
        })
    }

    fn recover(&self, _input: WriterRecoveryInput) -> Result<WriterRecoveryPlan> {
        Ok(WriterRecoveryPlan {
            mutations_to_commit: Vec::new(),
            core_meta_root_publications: Vec::new(),
            logical_files_to_rewrite: Vec::new(),
            orphaned_logical_files: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriterSegmentBuildInput {
    pub file_family: FileFamily,
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub header_proto: Vec<u8>,
    pub body: Vec<u8>,
    pub range_index: Vec<u8>,
    pub record_count: u64,
    pub first_record_hash: Hash32,
    pub last_record_hash: Hash32,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone)]
pub struct WriterSegmentBuildOutput {
    pub encoded: EncodedWriterSegment,
    pub logical_file: LogicalFileWrite,
    pub range_index_entries: Vec<RangeIndexEntry>,
}

pub fn build_writer_segment_logical_file(
    input: WriterSegmentBuildInput,
) -> Result<WriterSegmentBuildOutput> {
    let range_index_entries = super::decode_range_index(&input.range_index)?;
    let encoded = encode_writer_segment(
        input.file_family,
        0,
        input.header_proto,
        &input.body,
        &input.range_index,
        input.record_count,
        input.first_record_hash,
        input.last_record_hash,
    )?;
    let logical_file = LogicalFileWrite {
        logical_file_id: input.logical_file_id,
        writer_family: input.writer_family,
        writer_generation: input.writer_generation,
        bytes: ByteSource::InlineBytes(encoded.bytes.clone()),
        ranges: range_index_entries
            .iter()
            .map(|entry| LogicalRangeHint {
                byte_start: entry.logical_start,
                byte_end: entry.logical_end,
                writer_record_kind: input.file_family.writer_family_name().to_string(),
                boundary_values: input.boundary_values.clone(),
                statistics: RangeStatistics {
                    encoded_bytes: entry.stats_ref.clone(),
                },
                preferred_block_boundary: BoundaryStrength::Preferred,
                prefetch_group: None,
                shared_range: None,
            })
            .collect(),
        durability_class: if encoded.bytes.len() <= CORE_META_MAX_INLINE_PAYLOAD_BYTES {
            DurabilityClass::InlineMetadata
        } else {
            DurabilityClass::ErasureCodedBytes
        },
        compaction_policy: CompactionPolicy::default(),
        read_profile: ReadProfile::default(),
        pipeline_policy: input.pipeline_policy,
        trace_context: input.trace_context,
        boundary_values: input.boundary_values,
        mutation_id: input.mutation_id,
        region_id: input.region_id,
    };
    Ok(WriterSegmentBuildOutput {
        encoded,
        logical_file,
        range_index_entries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rooted_common(
        root_anchor_key: &str,
        generation: u64,
        transaction_id: &str,
    ) -> CoreMetaRowCommonProto {
        CoreMetaRowCommonProto {
            realm_id: "test".to_string(),
            root_key_hash: core_meta_root_key_hash(root_anchor_key),
            root_generation: generation,
            transaction_id: transaction_id.to_string(),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: 1,
            payload_schema_version: 1,
        }
    }

    #[test]
    fn writer_root_publication_requires_canonical_sorted_families() {
        let publication = WriterRootPublication::new(
            "bucket/acme/index/orders",
            vec![WriterFamily::CoreControl, WriterFamily::TypedMetadata],
        )
        .expect("canonical declaration");
        assert_eq!(
            publication.writer_families(),
            &[WriterFamily::CoreControl, WriterFamily::TypedMetadata]
        );

        assert!(
            WriterRootPublication::new("/bucket/acme", vec![WriterFamily::CoreControl]).is_err()
        );
        assert!(
            WriterRootPublication::new(
                "bucket/acme",
                vec![WriterFamily::TypedMetadata, WriterFamily::CoreControl]
            )
            .is_err()
        );
        assert!(
            WriterRootPublication::new(
                "bucket/acme",
                vec![WriterFamily::CoreControl, WriterFamily::CoreControl]
            )
            .is_err()
        );
    }

    #[test]
    fn rooted_mutation_validates_canonical_scope_against_common_metadata() {
        let root_anchor_key = "bucket/acme/index/orders";
        let transaction_id = "format-write-1";
        let mutation = CoreMetaMutation::put(
            "cf_index_rows",
            1,
            b"orders/current".to_vec(),
            b"payload".to_vec(),
            Some(rooted_common(root_anchor_key, 7, transaction_id)),
            CoreMetaMutationScope::rooted(root_anchor_key, 7).expect("rooted scope"),
            transaction_id.to_string(),
        )
        .expect("rooted mutation");
        assert_eq!(mutation.scope().root_anchor_key(), Some(root_anchor_key));
        assert_eq!(mutation.scope().post_root_generation(), Some(7));

        let mismatched = CoreMetaMutation::put(
            "cf_index_rows",
            1,
            b"orders/current".to_vec(),
            b"payload".to_vec(),
            Some(rooted_common("bucket/other", 7, transaction_id)),
            CoreMetaMutationScope::rooted(root_anchor_key, 7).expect("rooted scope"),
            transaction_id.to_string(),
        );
        assert!(mismatched.is_err());
    }

    #[test]
    fn local_mutation_rejects_rooted_common_metadata() {
        let result = CoreMetaMutation::delete(
            "cf_index_rows",
            1,
            b"orders/current".to_vec(),
            Some(rooted_common(
                "bucket/acme/index/orders",
                1,
                "format-write-2",
            )),
            CoreMetaMutationScope::local(),
            "format-write-2".to_string(),
        );
        assert!(result.is_err());
    }
}
