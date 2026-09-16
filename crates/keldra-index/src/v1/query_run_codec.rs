//! Published run metadata and shared segment document tables.

use super::*;

impl ProjectionQueryRunDescriptor {
    pub fn has_memory_lease(&self) -> bool {
        self.memory_lease.is_attached()
    }
    pub fn attach_memory_lease(
        &self,
        lease: std::sync::Arc<dyn Send + Sync + std::fmt::Debug>,
    ) -> bool {
        self.memory_lease.attach(lease)
    }
    pub fn validate(&self, limits: QueryBlockLimits) -> Result<(), IndexError> {
        limits.validate()?;
        self.partition.validate()?;
        self.pack_table.validate()?;
        for (valid, invariant) in [
            (
                self.physical_catalog_generation != [0; 32],
                "physical_catalog_generation",
            ),
            (self.sequence != 0, "sequence"),
            (
                self.source_start_offset < self.next_offset,
                "source_offset_range",
            ),
        ] {
            if !valid {
                return Err(query_run_integrity(self, invariant));
            }
        }
        let mut total_descriptor_key_bytes = 0usize;
        let mut previous = None::<(&QueryBlockKind, &RecipeIdentity, &[u8], &[u8], &[u8; 32])>;
        for block in &self.blocks {
            if block.hash == [0; 32]
                || block.encoded_bytes == 0
                || usize::try_from(block.encoded_bytes)
                    .map_or(true, |bytes| bytes > limits.maximum_block_bytes)
                || block.records == 0
                || block.records as usize > limits.maximum_records
                || block.minimum_key.is_empty()
                || block.minimum_key > block.maximum_key
                || block.minimum_key.len() > limits.maximum_key_bytes
                || block.maximum_key.len() > limits.maximum_key_bytes
                || block.locator.encoded_bytes != block.encoded_bytes
                || block.locator.logical_bytes != block.encoded_bytes
                || block.locator.checksum != block.hash
                || block.kind != QueryBlockKind::TermDictionary
                    && block.documents.documents().is_empty()
                || !block.documents.is_version_bound()
            {
                return Err(IndexError::InvalidDefinition(
                    "v1 query block descriptor is invalid".into(),
                ));
            }
            if block.pack_table.as_ref() != self.pack_table.as_ref() {
                return Err(IndexError::Integrity);
            }
            block.locator.resolve(&self.pack_table)?;
            total_descriptor_key_bytes = total_descriptor_key_bytes
                .checked_add(block.minimum_key.len())
                .and_then(|bytes| bytes.checked_add(block.maximum_key.len()))
                .ok_or(IndexError::OffsetOverflow)?;
            let current = (
                &block.kind,
                &block.recipe,
                block.minimum_key.as_slice(),
                block.maximum_key.as_slice(),
                &block.hash,
            );
            if previous.is_some_and(|previous| previous >= current) {
                return Err(IndexError::InvalidDefinition(
                    "v1 query block descriptors are not canonical order".into(),
                ));
            }
            previous = Some(current);
        }
        if total_descriptor_key_bytes > limits.maximum_run_descriptor_bytes {
            return Err(IndexError::ResourceLimit {
                needed: total_descriptor_key_bytes,
                limit: limits.maximum_run_descriptor_bytes,
            });
        }
        Ok(())
    }

    pub fn matching_blocks<'a>(
        &'a self,
        kind: QueryBlockKind,
        recipe: RecipeIdentity,
        lower: &[u8],
        upper: &[u8],
    ) -> impl Iterator<Item = &'a QueryBlockDescriptor> {
        self.blocks.iter().filter(move |block| {
            block.kind == kind
                && block.recipe == recipe
                && block.maximum_key.as_slice() >= lower
                && block.minimum_key.as_slice() <= upper
        })
    }
}

fn query_run_integrity(run: &ProjectionQueryRunDescriptor, invariant: &str) -> IndexError {
    let generation = run
        .physical_catalog_generation
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    IndexError::IntegrityViolation(format!(
        "projection query run {invariant}; partition={:?}, catalog_generation={generation}, sequence={}, source_range={}..{}, blocks={}",
        run.partition,
        run.sequence,
        run.source_start_offset,
        run.next_offset,
        run.blocks.len()
    ))
}

pub fn encode_projection_query_run(
    descriptor: &ProjectionQueryRunDescriptor,
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<EncodedProjectionQueryRun, IndexError> {
    descriptor.validate(limits)?;
    let tables = descriptor
        .blocks
        .iter()
        .map(|block| (block.documents.identity(), block.documents.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut required: usize = 8 + 2 + 96 + 32 + 8 * 4 + 4 + 4 + 4;
    for table in tables.values() {
        required = required
            .checked_add(32 + 4)
            .and_then(|bytes| bytes.checked_add(table.documents().len().checked_mul(40)?))
            .ok_or(IndexError::OffsetOverflow)?;
    }
    for pack in descriptor.pack_table.entries() {
        required = required
            .checked_add(4 + 4 + pack.canonical_path.len() + 8 + 32 + 8)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    for block in &descriptor.blocks {
        required = required
            .checked_add(
                1 + 32
                    + 32
                    + 8
                    + 4
                    + 4
                    + block.minimum_key.len()
                    + 4
                    + block.maximum_key.len()
                    + 32
                    + 4
                    + 8
                    + 8
                    + 8
                    + 32,
            )
            .ok_or(IndexError::OffsetOverflow)?;
    }
    if required > limits.maximum_run_descriptor_bytes {
        return Err(IndexError::ResourceLimit {
            needed: required,
            limit: limits.maximum_run_descriptor_bytes,
        });
    }
    credits.reserve(required)?;
    let mut bytes = Vec::with_capacity(required);
    bytes.extend_from_slice(RUN_MAGIC);
    bytes.extend_from_slice(&RUN_FORMAT.to_be_bytes());
    put_partition(&mut bytes, descriptor.partition);
    bytes.extend_from_slice(&descriptor.physical_catalog_generation);
    put_u64(&mut bytes, descriptor.sequence);
    put_u64(&mut bytes, descriptor.source_start_offset);
    put_u64(&mut bytes, descriptor.next_offset);
    put_u64(&mut bytes, descriptor.through_atomic_position);
    put_u32(&mut bytes, descriptor.pack_table.entries().len())?;
    for pack in descriptor.pack_table.entries() {
        put_u32(&mut bytes, pack.ordinal as usize)?;
        put_bytes(&mut bytes, pack.canonical_path.as_bytes())?;
        put_u64(&mut bytes, pack.object_version);
        bytes.extend_from_slice(&pack.hash);
        put_u64(&mut bytes, pack.length);
    }
    put_u32(&mut bytes, tables.len())?;
    for (identity, table) in tables {
        bytes.extend_from_slice(&identity);
        put_u32(&mut bytes, table.documents().len())?;
        for (index, document) in table.documents().iter().enumerate() {
            bytes.extend_from_slice(document);
            put_u64(
                &mut bytes,
                table.material_version(SegmentDocumentId(index as u32))?,
            );
        }
    }
    put_u32(&mut bytes, descriptor.blocks.len())?;
    for block in &descriptor.blocks {
        bytes.push(block.kind as u8);
        bytes.extend_from_slice(&block.recipe.bytes());
        bytes.extend_from_slice(&block.documents.identity());
        put_u64(&mut bytes, block.encoded_bytes);
        put_u32(&mut bytes, block.records as usize)?;
        put_bytes(&mut bytes, &block.minimum_key)?;
        put_bytes(&mut bytes, &block.maximum_key)?;
        bytes.extend_from_slice(&block.hash);
        put_u32(&mut bytes, block.locator.ordinal as usize)?;
        put_u64(&mut bytes, block.locator.offset);
        put_u64(&mut bytes, block.locator.encoded_bytes);
        put_u64(&mut bytes, block.locator.logical_bytes);
        bytes.extend_from_slice(&block.locator.checksum);
    }
    if bytes.len() != required {
        return Err(IndexError::Integrity);
    }
    Ok(EncodedProjectionQueryRun {
        hash: *crate::profiled_blake3_hash!(&bytes).as_bytes(),
        bytes,
    })
}

pub fn decode_projection_query_run(
    bytes: &[u8],
    limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
) -> Result<ProjectionQueryRunDescriptor, IndexError> {
    let limits = limits.validate()?;
    if bytes.len() > limits.maximum_run_descriptor_bytes {
        return Err(IndexError::ResourceLimit {
            needed: bytes.len(),
            limit: limits.maximum_run_descriptor_bytes,
        });
    }
    let mut input = BlockInput::new(bytes);
    input.expect(RUN_MAGIC)?;
    if input.u16()? != RUN_FORMAT {
        return Err(IndexError::InvalidFormat("v1 query run format"));
    }
    let partition = read_partition(&mut input)?;
    let physical_catalog_generation = input.array_32()?;
    let sequence = input.u64()?;
    let source_start_offset = input.u64()?;
    let next_offset = input.u64()?;
    let through_atomic_position = input.u64()?;
    let pack_count = input.u32()? as usize;
    const MINIMUM_PACK_REFERENCE_BYTES: usize = 4 + 4 + 1 + 8 + 32 + 8;
    if pack_count > input.remaining() / MINIMUM_PACK_REFERENCE_BYTES {
        return Err(IndexError::UnexpectedEof {
            expected: pack_count
                .checked_mul(MINIMUM_PACK_REFERENCE_BYTES)
                .and_then(|size| size.checked_add(input.offset))
                .ok_or(IndexError::OffsetOverflow)? as u64,
            actual: input.bytes.len() as u64,
        });
    }
    let mut packs = Vec::with_capacity(pack_count);
    for _ in 0..pack_count {
        let ordinal = input.u32()?;
        let canonical_path = std::str::from_utf8(input.bytes()?)
            .map_err(|_| IndexError::InvalidFormat("v1 artifact pack path"))?
            .to_owned();
        let object_version = input.u64()?;
        let hash = input.array_32()?;
        let length = input.u64()?;
        packs.push(super::super::ArtifactPackReference {
            ordinal,
            canonical_path: std::sync::Arc::from(canonical_path),
            object_version,
            hash,
            length,
        });
    }
    let pack_table = std::sync::Arc::new(ArtifactPackTable::new(packs)?);
    let table_count = input.u32()? as usize;
    if table_count > input.remaining() / 36 {
        return Err(IndexError::Integrity);
    }
    let mut tables = BTreeMap::new();
    for _ in 0..table_count {
        let identity = input.array_32()?;
        let document_count = input.u32()? as usize;
        if document_count > input.remaining() / 40 {
            return Err(IndexError::Integrity);
        }
        credits.reserve(
            document_count
                .checked_mul(40)
                .and_then(|bytes| {
                    bytes.checked_add(std::mem::size_of::<SegmentDocumentTable>() + 512)
                })
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut documents = Vec::with_capacity(document_count);
        let mut versions = Vec::with_capacity(document_count);
        for _ in 0..document_count {
            let document = stable_key(&input.array_32()?)?;
            documents.push(document.bytes());
            versions.push(input.u64()?);
        }
        let table = std::sync::Arc::new(SegmentDocumentTable::from_sorted_columns(
            documents, versions,
        )?);
        if table.identity() != identity || tables.insert(identity, table).is_some() {
            return Err(IndexError::Integrity);
        }
    }
    let count = input.u32()? as usize;
    const MINIMUM_DESCRIPTOR_BLOCK_BYTES: usize =
        1 + 32 + 32 + 8 + 4 + 4 + 4 + 32 + 4 + 8 + 8 + 8 + 32;
    if count > input.remaining() / MINIMUM_DESCRIPTOR_BLOCK_BYTES {
        return Err(IndexError::UnexpectedEof {
            expected: count
                .checked_mul(MINIMUM_DESCRIPTOR_BLOCK_BYTES)
                .and_then(|size| size.checked_add(input.offset))
                .ok_or(IndexError::OffsetOverflow)? as u64,
            actual: input.bytes.len() as u64,
        });
    }
    credits.reserve(bytes.len())?;
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = QueryBlockKind::decode(input.byte()?)?;
        let recipe = RecipeIdentity::new(input.array_32()?)?;
        let documents = tables
            .get(&input.array_32()?)
            .cloned()
            .ok_or(IndexError::Integrity)?;
        let encoded_bytes = input.u64()?;
        let records = input.u32()?;
        let minimum_key = input.bytes()?.to_vec();
        let maximum_key = input.bytes()?.to_vec();
        let hash = input.array_32()?;
        let locator = ArtifactPackLocator {
            ordinal: input.u32()?,
            offset: input.u64()?,
            encoded_bytes: input.u64()?,
            logical_bytes: input.u64()?,
            checksum: input.array_32()?,
        };
        blocks.push(QueryBlockDescriptor {
            kind,
            recipe,
            minimum_key,
            maximum_key,
            hash,
            encoded_bytes,
            records,
            locator,
            pack_table: pack_table.clone(),
            documents,
        });
    }
    input.finish()?;
    let descriptor = ProjectionQueryRunDescriptor {
        partition,
        physical_catalog_generation,
        sequence,
        source_start_offset,
        next_offset,
        through_atomic_position,
        pack_table,
        blocks,
        memory_lease: SegmentMemoryLease::default(),
    };
    descriptor.validate(limits)?;
    Ok(descriptor)
}
