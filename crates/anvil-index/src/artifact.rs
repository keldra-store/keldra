use crate::{
    ComponentCodec, IndexError, IndexKind, MAX_INDEX_BLOCK_BYTES, MAX_INDEX_ROUTING_KEY_BYTES,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockDescriptor {
    pub kind: IndexKind,
    pub component_tag: u8,
    pub codec: ComponentCodec,
    /// Zero is a data leaf; larger values are recursive routing pages.
    pub routing_height: u8,
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub encoded_bytes: u64,
    pub hash: [u8; 32],
    /// Deterministic writer-lane/local-pack ID containing this logical block.
    ///
    /// Builders create blocks before a storage sink assigns their physical
    /// location. Only descriptors returned by [`crate::IndexBlockSink`] may be
    /// embedded in another block.
    pub pack_id: u32,
    /// Byte offset of this logical block within `pack_id`.
    pub pack_offset: u64,
}

impl BlockDescriptor {
    pub fn logical_name(&self) -> String {
        hex_hash(&self.hash)
    }

    pub(crate) fn place(&mut self, pack_id: u32, pack_offset: u64) {
        self.pack_id = pack_id;
        self.pack_offset = pack_offset;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct GeneratedBlock {
    descriptor: BlockDescriptor,
    bytes: Vec<u8>,
}

impl GeneratedBlock {
    pub(crate) fn new(
        kind: IndexKind,
        component_tag: u8,
        codec: ComponentCodec,
        routing_height: u8,
        minimum_key: Vec<u8>,
        maximum_key: Vec<u8>,
        element_count: u64,
        bytes: Vec<u8>,
    ) -> Result<Self, IndexError> {
        if bytes.is_empty() || element_count == 0 || minimum_key > maximum_key {
            return Err(IndexError::InvalidDefinition(
                "index blocks require a non-empty canonical key range".into(),
            ));
        }
        if minimum_key.len() > MAX_INDEX_ROUTING_KEY_BYTES
            || maximum_key.len() > MAX_INDEX_ROUTING_KEY_BYTES
        {
            return Err(IndexError::ResourceLimit {
                needed: minimum_key.len().max(maximum_key.len()),
                limit: MAX_INDEX_ROUTING_KEY_BYTES,
            });
        }
        if bytes.len() > MAX_INDEX_BLOCK_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: bytes.len(),
                limit: MAX_INDEX_BLOCK_BYTES,
            });
        }
        let hash = *blake3::hash(&bytes).as_bytes();
        Ok(Self {
            descriptor: BlockDescriptor {
                kind,
                component_tag,
                codec,
                routing_height,
                minimum_key,
                maximum_key,
                element_count,
                encoded_bytes: u64::try_from(bytes.len())
                    .map_err(|_| IndexError::OffsetOverflow)?,
                hash,
                pack_id: u32::MAX,
                pack_offset: 0,
            },
            bytes,
        })
    }

    pub fn descriptor(&self) -> &BlockDescriptor {
        &self.descriptor
    }

    pub fn logical_name(&self) -> String {
        self.descriptor.logical_name()
    }

    pub fn into_parts(self) -> (BlockDescriptor, Vec<u8>) {
        (self.descriptor, self.bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunDescriptor {
    pub format_version: u16,
    pub kind: IndexKind,
    pub level: u8,
    pub mutation_count: u64,
    pub live_document_count: u64,
    pub minimum_version: u64,
    pub maximum_version: u64,
    pub encoded_bytes: u64,
    pub hash: [u8; 32],
}

#[derive(Debug, PartialEq, Eq)]
pub struct SealedRun {
    descriptor: RunDescriptor,
    root: GeneratedBlock,
}

impl SealedRun {
    pub(crate) fn new(
        kind: IndexKind,
        level: u8,
        mutation_count: u64,
        live_document_count: u64,
        minimum_version: u64,
        maximum_version: u64,
        encoded_bytes: u64,
        root: GeneratedBlock,
    ) -> Self {
        Self {
            descriptor: RunDescriptor {
                format_version: crate::INDEX_FORMAT_VERSION,
                kind,
                level,
                mutation_count,
                live_document_count,
                minimum_version,
                maximum_version,
                encoded_bytes,
                hash: root.descriptor.hash,
            },
            root,
        }
    }

    pub fn descriptor(&self) -> &RunDescriptor {
        &self.descriptor
    }

    pub fn into_root(self) -> GeneratedBlock {
        self.root
    }
}

pub(crate) fn hex_hash(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in hash {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_size_and_routing_key_limits_fail_closed() {
        assert!(matches!(
            GeneratedBlock::new(
                IndexKind::Path,
                1,
                ComponentCodec::FixedRows,
                0,
                b"a".to_vec(),
                b"a".to_vec(),
                1,
                vec![0; MAX_INDEX_BLOCK_BYTES + 1],
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
        assert!(matches!(
            GeneratedBlock::new(
                IndexKind::Path,
                1,
                ComponentCodec::FixedRows,
                0,
                vec![b'a'; MAX_INDEX_ROUTING_KEY_BYTES + 1],
                vec![b'a'; MAX_INDEX_ROUTING_KEY_BYTES + 1],
                1,
                vec![1],
            ),
            Err(IndexError::ResourceLimit { .. })
        ));
    }
}
