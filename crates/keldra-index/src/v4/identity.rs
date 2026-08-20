use crate::IndexError;

use super::codec::{COMPONENT_HEADER_BYTES, Decoder, Encoder};
use super::model::{DocId, INDEX_COMPONENT_BYTES, INDEX_ROUTING_KEY_BYTES};

const IDENTITY_CODEC_VERSION: u16 = 2;
const MAX_PAYLOAD_BYTES: usize = INDEX_COMPONENT_BYTES - COMPONENT_HEADER_BYTES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectIdentity {
    pub path: String,
    pub version: u64,
}

impl ObjectIdentity {
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.path.is_empty()
            || self.path.len() > INDEX_ROUTING_KEY_BYTES
            || self.path.contains('\0')
            || self.version == 0
        {
            return Err(IndexError::InvalidDefinition(
                "object identity requires a 1..=4096-byte path and non-zero version".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentIdentity {
    /// Source object whose overwrite/delete controls this record's liveness.
    pub source: ObjectIdentity,
    /// Stable ordinal of this record inside `source`. Unlike a segment DocId,
    /// this survives deterministic merges and can complete a public cursor's
    /// total order when several records return the same result object.
    pub source_record: u32,
    /// Optional object authorized and returned for this projected record.
    /// Ordinary indexes leave this absent and return `source`.
    pub result: Option<ObjectIdentity>,
}

impl DocumentIdentity {
    pub fn result_or_source(&self) -> &ObjectIdentity {
        self.result.as_ref().unwrap_or(&self.source)
    }

    fn validate(&self) -> Result<(), IndexError> {
        self.source.validate()?;
        if let Some(result) = &self.result {
            result.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityBlock {
    pub first_doc_id: DocId,
    entries: Vec<DocumentIdentity>,
}

impl IdentityBlock {
    pub fn new(first_doc_id: DocId, entries: Vec<DocumentIdentity>) -> Result<Self, IndexError> {
        if entries.is_empty() {
            return Err(IndexError::InvalidDefinition(
                "identity block must not be empty".into(),
            ));
        }
        for entry in &entries {
            entry.validate()?;
        }
        let last_offset =
            u32::try_from(entries.len() - 1).map_err(|_| IndexError::OffsetOverflow)?;
        first_doc_id
            .get()
            .checked_add(last_offset)
            .ok_or(IndexError::OffsetOverflow)?;
        let block = Self {
            first_doc_id,
            entries,
        };
        if block.encode_payload()?.len() > MAX_PAYLOAD_BYTES {
            return Err(IndexError::ResourceLimit {
                needed: block.encode_payload()?.len() + COMPONENT_HEADER_BYTES,
                limit: INDEX_COMPONENT_BYTES,
            });
        }
        Ok(block)
    }

    pub fn entries(&self) -> &[DocumentIdentity] {
        &self.entries
    }

    pub fn get(&self, doc_id: DocId) -> Option<&DocumentIdentity> {
        let offset = doc_id.get().checked_sub(self.first_doc_id.get())?;
        self.entries.get(offset as usize)
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, IndexError> {
        let mut out = Encoder::default();
        out.u16(IDENTITY_CODEC_VERSION);
        out.u32(self.first_doc_id.get());
        out.usize_u32(self.entries.len())?;
        for entry in &self.entries {
            entry.validate()?;
            out.string(&entry.source.path)?;
            out.u64(entry.source.version);
            out.u32(entry.source_record);
            out.bool(entry.result.is_some());
            if let Some(result) = &entry.result {
                out.string(&result.path)?;
                out.u64(result.version);
            }
        }
        Ok(out.finish())
    }

    pub fn decode_payload(bytes: &[u8]) -> Result<Self, IndexError> {
        let mut input = Decoder::new(bytes)?;
        if input.u16()? != IDENTITY_CODEC_VERSION {
            return Err(IndexError::InvalidFormat("identity codec version"));
        }
        let first = DocId::new(input.u32()?);
        let count = usize::try_from(input.u32()?).map_err(|_| IndexError::OffsetOverflow)?;
        input.claim(
            count
                .checked_mul(std::mem::size_of::<DocumentIdentity>())
                .ok_or(IndexError::OffsetOverflow)?,
        )?;
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            entries.push(DocumentIdentity {
                source: ObjectIdentity {
                    path: input.string()?,
                    version: input.u64()?,
                },
                source_record: input.u32()?,
                result: input
                    .bool()?
                    .then(|| {
                        Ok::<ObjectIdentity, IndexError>(ObjectIdentity {
                            path: input.string()?,
                            version: input.u64()?,
                        })
                    })
                    .transpose()?,
            });
        }
        input.finish()?;
        Self::new(first, entries)
    }

    pub fn split(entries: Vec<DocumentIdentity>) -> Result<Vec<Self>, IndexError> {
        let mut blocks = Vec::new();
        let mut pending = Vec::new();
        let mut first = 0u32;
        let mut payload_bytes = 2usize + 4 + 4;
        for entry in entries {
            entry.validate()?;
            let row = 4usize
                .checked_add(entry.source.path.len())
                .and_then(|value| value.checked_add(8 + 4 + 1))
                .and_then(|value| {
                    value.checked_add(
                        entry
                            .result
                            .as_ref()
                            .map_or(0, |result| 4 + result.path.len() + 8),
                    )
                })
                .ok_or(IndexError::OffsetOverflow)?;
            if !pending.is_empty() && payload_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                let count = u32::try_from(pending.len()).map_err(|_| IndexError::OffsetOverflow)?;
                blocks.push(Self::new(DocId::new(first), std::mem::take(&mut pending))?);
                first = first.checked_add(count).ok_or(IndexError::OffsetOverflow)?;
                payload_bytes = 10;
            }
            if payload_bytes.saturating_add(row) > MAX_PAYLOAD_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: payload_bytes.saturating_add(row) + COMPONENT_HEADER_BYTES,
                    limit: INDEX_COMPONENT_BYTES,
                });
            }
            payload_bytes += row;
            pending.push(entry);
        }
        if !pending.is_empty() {
            blocks.push(Self::new(DocId::new(first), pending)?);
        }
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_doc_ids_round_trip() {
        let block = IdentityBlock::new(
            DocId::new(41),
            vec![
                DocumentIdentity {
                    source: ObjectIdentity {
                        path: "a".into(),
                        version: 7,
                    },
                    source_record: 0,
                    result: None,
                },
                DocumentIdentity {
                    source: ObjectIdentity {
                        path: "b".into(),
                        version: 8,
                    },
                    source_record: 3,
                    result: Some(ObjectIdentity {
                        path: "result/b".into(),
                        version: 3,
                    }),
                },
            ],
        )
        .unwrap();
        let decoded = IdentityBlock::decode_payload(&block.encode_payload().unwrap()).unwrap();
        assert_eq!(decoded, block);
        assert_eq!(decoded.get(DocId::new(42)).unwrap().source_record, 3);
        assert_eq!(
            decoded.get(DocId::new(42)).unwrap().result_or_source().path,
            "result/b"
        );
        assert!(decoded.get(DocId::new(43)).is_none());
    }

    #[test]
    fn large_tables_split_without_doc_id_gaps() {
        let path = "x".repeat(4096);
        let entries = (0..300)
            .map(|version| DocumentIdentity {
                source: ObjectIdentity {
                    path: format!("{path}{version}"),
                    version: version + 1,
                },
                source_record: version as u32,
                result: None,
            })
            .collect();
        // The deliberately oversized path suffix is rejected before an
        // allocation can create a malformed identity block.
        assert!(IdentityBlock::split(entries).is_err());
        let entries = (0..20_000)
            .map(|version| DocumentIdentity {
                source: ObjectIdentity {
                    path: format!("object/{version:08}"),
                    version: version + 1,
                },
                source_record: version as u32,
                result: None,
            })
            .collect();
        let blocks = IdentityBlock::split(entries).unwrap();
        assert!(blocks.len() > 1);
        for pair in blocks.windows(2) {
            assert_eq!(
                pair[0].first_doc_id.get() + pair[0].entries.len() as u32,
                pair[1].first_doc_id.get()
            );
        }
    }
}
