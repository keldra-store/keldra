//! Deterministic sealing admission from native record sizes and block packing.
use super::{
    ArtifactPackReference, EncodedQueryBlock, PreparedQueryMutationBatch,
    ProjectionQueryRunDescriptor, QueryBlockDescriptor, QueryBlockKind, QueryBlockLimits,
    QueryBlockRecord, RecipeIdentity,
};
use crate::{IndexError, typed_json::ScalarValue};
use std::{collections::BTreeMap, mem::size_of};

const NATIVE_BLOCK_HEADER_BYTES: usize = 119;
const RESTART_INTERVAL: usize = 64;
const PACK_BYTES: usize = super::ARTIFACT_PACK_MAX_BYTES;

/// Peak of charged builder representations, excluding already-admitted input.
/// Counts the same recipe/term groups as the encoder without encoding values.
/// Nonfinal greedy blocks occupy at least (capacity-largest_record), unless
/// their record limit caused a split. This bounds both reasons for splitting.
pub fn query_run_seal_peak_bytes(
    batch: &PreparedQueryMutationBatch,
    limits: QueryBlockLimits,
) -> Result<usize, IndexError> {
    query_batches_seal_peak_bytes(std::iter::once(batch), limits)
}

/// Candidate batches are inspected together without cloning or encoding their
/// fields. Duplicate/coalesced entries conservatively contribute to the bound.
pub fn query_batches_seal_peak_bytes<'a>(
    batches: impl IntoIterator<Item = &'a PreparedQueryMutationBatch>,
    limits: QueryBlockLimits,
) -> Result<usize, IndexError> {
    let limits = limits.validate()?;
    let mut ordinary = BTreeMap::<(QueryBlockKind, RecipeIdentity), Group>::new();
    let mut terms = BTreeMap::<(RecipeIdentity, &ScalarValue), TermGroup>::new();
    let mut charged = size_of::<ProjectionQueryRunDescriptor>();
    let mut document_slots = 0usize;
    for batch in batches {
        if let Some(membership) = &batch.membership {
            for gate in &membership.gates {
                let paths = add(
                    add(
                        gate.source_path.as_ref().map_or(0, String::len),
                        gate.canonical_source_path.as_ref().map_or(0, String::len),
                    )?,
                    gate.result_path.as_ref().map_or(0, String::len),
                )?;
                ordinary
                    .entry((QueryBlockKind::Gate, membership.recipe))
                    .or_default()
                    .record(32, add(37, paths)?, false)?;
                document_slots = add(document_slots, 1)?;
            }
        }
        for field in &batch.fields {
            ordinary
                .entry((QueryBlockKind::Presence, field.recipe))
                .or_default()
                .record(32, 37, false)?;
            document_slots = add(document_slots, 1)?;
            if let Some(value) = &field.delta.doc_value {
                let bytes = value
                    .value
                    .iter()
                    .flatten()
                    .try_fold(13usize, |bytes, scalar| {
                        add(bytes, add(value_bytes(scalar)?, 4)?)
                    })?;
                ordinary
                    .entry((QueryBlockKind::DocValue, field.recipe))
                    .or_default()
                    .record(32, bytes, false)?;
                document_slots = add(document_slots, 1)?;
            }
            for point in &field.delta.points {
                ordinary
                    .entry((QueryBlockKind::Point, field.recipe))
                    .or_default()
                    .record(add(sort_bytes(&point.value)?, 32)?, 9, false)?;
                document_slots = add(document_slots, 1)?;
            }
            for term in &field.delta.terms {
                let group = terms.entry((field.recipe, &term.term)).or_default();
                group.postings.record(32, 46, false)?;
                if !term.positions.is_empty() {
                    group
                        .positions
                        .record(32, add(4, mul(term.positions.len(), 4)?)?, false)?;
                }
                // Term preparation charges 192-byte ordered-map residency, then
                // allocates a posting-record array while consuming those entries.
                charged = add(
                    charged,
                    add(
                        192 + size_of::<QueryBlockRecord>(),
                        add(value_bytes(&term.term)?, mul(term.positions.len(), 4)?)?,
                    )?,
                )?;
                document_slots = add(document_slots, 1)?;
            }
        }
    }
    let mut encoded = 0usize;
    let mut blocks = 0usize;
    let mut descriptors = 0usize;
    for ((recipe, term), mut group) in terms {
        // Posting and position windows split together whenever either kind
        // reaches its bound. Their split causes can interleave, so sum both
        // independent upper bounds, rather than treating postings alone.
        let posting_blocks = add(
            group.postings.block_count(limits)?,
            group.positions.block_count(limits)?,
        )?
        .saturating_sub(1)
        .max(group.postings.block_count(limits)?)
        .min(group.postings.records);
        group.postings.minimum_blocks = posting_blocks;
        group.positions.minimum_blocks = posting_blocks.min(group.positions.records);
        ordinary
            .entry((QueryBlockKind::TermDictionary, recipe))
            .or_default()
            .record(sort_bytes(term)?, add(4, mul(posting_blocks, 100)?)?, true)?;
        for group in [group.postings, group.positions] {
            account(
                group,
                limits,
                &mut charged,
                &mut encoded,
                &mut blocks,
                &mut descriptors,
            )?;
        }
    }
    for group in ordinary.into_values() {
        account(
            group,
            limits,
            &mut charged,
            &mut encoded,
            &mut blocks,
            &mut descriptors,
        )?;
    }
    // Whole encoded blocks are greedily packed. All but the final pack contain
    // at least pack capacity minus the largest admitted block.
    let pack_fill = PACK_BYTES.saturating_sub(limits.maximum_block_bytes).max(1);
    let packs = if blocks == 0 {
        0
    } else {
        encoded.div_ceil(pack_fill).min(blocks).max(1)
    };
    charged = add(
        charged,
        224 + size_of::<super::query_blocks::SegmentDocumentTable>()
            + size_of::<super::ArtifactPackTable>(),
    )?;
    charged = add(charged, mul(document_slots, 160)?)?; // sorting workspace + retained/wire identity tables
    charged = add(charged, mul(encoded, 2)?)?; // encoded blocks + transient pack copy
    charged = add(charged, mul(descriptors, 2)?)?; // logical and finalized descriptor arrays
    // Production paths have fixed family/hash widths; account both retained
    // and serialized pack-table paths, not an arbitrary runtime path cap.
    let pack_path = "_keldra/index-projections/v1/".len() + 64 + "/artifacts/packs/".len() + 64;
    charged = add(
        charged,
        mul(
            packs,
            add(size_of::<ArtifactPackReference>() + 68, mul(pack_path, 2)?)?,
        )?,
    )?;
    Ok(charged)
}

#[derive(Default)]
struct Group {
    records: usize,
    payload: usize,
    largest: usize,
    maximum_key: usize,
    grouped: usize,
    minimum_blocks: usize,
}
#[derive(Default)]
struct TermGroup {
    postings: Group,
    positions: Group,
}
impl Group {
    fn record(&mut self, key: usize, value: usize, dictionary: bool) -> Result<(), IndexError> {
        let native_key = if dictionary {
            key
        } else {
            key.checked_sub(28).ok_or(IndexError::Integrity)?
        };
        let payload = add(8, add(native_key, value)?)?;
        self.records = add(self.records, 1)?;
        self.payload = add(self.payload, payload)?;
        self.largest = self.largest.max(payload);
        self.maximum_key = self.maximum_key.max(key);
        self.grouped = add(
            self.grouped,
            add(size_of::<QueryBlockRecord>(), add(mul(key, 2)?, value)?)?,
        )?;
        Ok(())
    }
    fn block_count(&self, limits: QueryBlockLimits) -> Result<usize, IndexError> {
        if self.records == 0 {
            return Ok(0);
        }
        let one = add(
            NATIVE_BLOCK_HEADER_BYTES,
            add(
                self.payload,
                mul(self.records.div_ceil(RESTART_INTERVAL), 4)?,
            )?,
        )?;
        if self.records <= limits.maximum_records && one <= limits.maximum_block_bytes {
            return Ok(self.minimum_blocks.max(1));
        }
        let capacity = limits
            .maximum_block_bytes
            .saturating_sub(NATIVE_BLOCK_HEADER_BYTES + 4);
        if self.largest > capacity {
            // Values and posting-shard counts are conservative upper bounds,
            // not encoded record sizes. Do not turn overestimation into a
            // second format validator: actual records still pass the native
            // encoder's unchanged single-record limits.
            return Ok(self.records);
        }
        // Four restart bytes per record is a conservative contribution to each
        // split's largest record; the retained-size calculation below uses the
        // real restart interval rather than charging that worst case everywhere.
        let fill = capacity.saturating_sub(add(self.largest, 4)?).max(1);
        let split_payload = add(self.payload, mul(self.records, 4)?)?;
        Ok(add(
            split_payload.div_ceil(fill),
            self.records.div_ceil(limits.maximum_records),
        )?
        .saturating_sub(1)
        .max(self.minimum_blocks)
        .min(self.records)
        .max(1))
    }
}
fn account(
    group: Group,
    limits: QueryBlockLimits,
    charged: &mut usize,
    encoded: &mut usize,
    blocks: &mut usize,
    descriptors: &mut usize,
) -> Result<(), IndexError> {
    let count = group.block_count(limits)?;
    *charged = add(*charged, group.grouped)?;
    *encoded = add(
        *encoded,
        add(
            group.payload,
            add(
                mul(count, NATIVE_BLOCK_HEADER_BYTES)?,
                mul(add(group.records.div_ceil(RESTART_INTERVAL), count)?, 4)?,
            )?,
        )?,
    )?;
    *blocks = add(*blocks, count)?;
    let descriptor = add(
        size_of::<QueryBlockDescriptor>(),
        add(size_of::<EncodedQueryBlock>(), mul(group.maximum_key, 4)?)?,
    )?;
    *descriptors = add(*descriptors, mul(count, descriptor)?)?;
    Ok(())
}
fn add(a: usize, b: usize) -> Result<usize, IndexError> {
    a.checked_add(b).ok_or(IndexError::OffsetOverflow)
}
fn mul(a: usize, b: usize) -> Result<usize, IndexError> {
    a.checked_mul(b).ok_or(IndexError::OffsetOverflow)
}
fn value_bytes(value: &ScalarValue) -> Result<usize, IndexError> {
    match value {
        ScalarValue::String(value) => add(value.len(), 8),
        _ => Ok(16),
    }
}
fn sort_bytes(value: &ScalarValue) -> Result<usize, IndexError> {
    match value {
        ScalarValue::String(value) => add(mul(value.len(), 2)?, 3),
        _ => Ok(16),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{
        ArtifactPackTable, IndexingMemoryCredits, IndexingMemoryLimits, IndexingMemoryStage,
        PreparedQueryFieldDelta, PreparedQueryRecipeDelta, PreparedQueryTermDelta,
        ProjectionPartitionIdentity, QueryBlockCredits, QueryDocValue, QueryDocumentGate,
        QueryPoint, StableDocumentKey, prepare_projection_query_run, projection_pack_path,
    };

    fn shape_batch(
        recipes: usize,
        rows: usize,
        term_count: usize,
        positions: usize,
        string_bytes: usize,
    ) -> PreparedQueryMutationBatch {
        let mut batch = PreparedQueryMutationBatch::default();
        for recipe in 0..recipes {
            for row in 0..rows {
                let mut identity = [1; 32];
                identity[..8].copy_from_slice(&(row as u64 + 1).to_be_bytes());
                let document = StableDocumentKey::from_bytes(identity).unwrap();
                let value = ScalarValue::String("v".repeat(string_bytes));
                batch.fields.push(PreparedQueryRecipeDelta {
                    recipe: RecipeIdentity::new([recipe as u8 + 1; 32]).unwrap(),
                    delta: PreparedQueryFieldDelta {
                        presence: QueryDocumentGate {
                            document,
                            material_source_version: 1,
                            current_source_version: 1,
                            live: true,
                            source_path: None,
                            canonical_source_path: None,
                            result_path: None,
                            result_version: 0,
                        },
                        doc_value: Some(QueryDocValue {
                            document,
                            material_source_version: 1,
                            value: Some(vec![value.clone()]),
                        }),
                        points: vec![QueryPoint {
                            document,
                            material_source_version: 1,
                            value,
                            live: true,
                        }],
                        terms: (0..term_count)
                            .map(|term| PreparedQueryTermDelta {
                                term: ScalarValue::String(format!("term-{term:04}")),
                                document,
                                material_source_version: 1,
                                live: true,
                                positions: (0..positions as u32).collect(),
                            })
                            .collect(),
                    },
                });
            }
        }
        batch
    }

    fn assert_encoder_fits(
        recipes: usize,
        rows: usize,
        term_count: usize,
        positions: usize,
        string_bytes: usize,
        maximum_records: usize,
    ) {
        let batch = shape_batch(recipes, rows, term_count, positions, string_bytes);
        let mut limits = QueryBlockLimits::default_for_memory();
        limits.maximum_records = maximum_records;
        // Position-heavy term windows split postings in tandem. Their one
        // dictionary entry contains every posting-shard reference, so the
        // 128-document/256-position shape needs an 8 KiB block to be genuinely
        // encoder-capable; it still exercises byte-driven position splitting.
        limits.maximum_block_bytes = if positions >= 256 { 8192 } else { 4096 };
        limits.maximum_key_bytes = limits.maximum_block_bytes;
        limits.maximum_value_bytes = limits.maximum_block_bytes;
        let peak = query_run_seal_peak_bytes(&batch, limits).unwrap();
        let memory = IndexingMemoryCredits::new(
            peak,
            IndexingMemoryLimits {
                hot_payload_bytes: peak,
                worker_scratch_bytes: peak,
                prepared_rows_bytes: peak,
                replay_input_bytes: peak,
                projection_accumulator_bytes: peak,
                seal_scratch_bytes: peak,
                ordering_catalog_bytes: peak,
            },
        )
        .unwrap();
        let credits = QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::SealScratch, peak)
                .unwrap(),
        );
        let partition = ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap();
        // Fixed-size credits make every encoder admission enforce the bound,
        // including grouping, table sort, block copies and final descriptor.
        let prepared =
            prepare_projection_query_run(partition, [2; 32], 1, 0, 1, 0, batch, limits, credits)
                .unwrap();
        let table = ArtifactPackTable::new(
            prepared
                .packs()
                .iter()
                .map(|pack| ArtifactPackReference {
                    ordinal: pack.ordinal,
                    canonical_path: projection_pack_path(partition, pack.hash).into(),
                    object_version: u64::from(pack.ordinal) + 1,
                    hash: pack.hash,
                    length: pack.bytes.len() as u64,
                })
                .collect(),
        )
        .unwrap();
        let artifacts = prepared.finalize(table).unwrap();
        let retained = artifacts
            .artifacts()
            .packs
            .iter()
            .map(|pack| pack.bytes.len())
            .sum::<usize>()
            + artifacts.artifacts().run.bytes.len();
        assert!(peak >= retained);
        drop(artifacts);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn actual_encoder_and_finalization_fit_admitted_native_peak_shapes() {
        // Many independent recipes; one frequent term with positions; many
        // small term dictionaries; large string/range columns; split edges.
        for shape in [
            (12, 8, 1, 0, 8, 64),
            (1, 128, 1, 8, 8, 64),
            // Actual positional windows contain four documents; the greedy
            // admission upper bound predicts more shards and an oversized
            // dictionary record. Native encoding remains valid at 4 KiB.
            (1, 128, 1, 220, 8, 64),
            (1, 128, 1, 256, 8, 64),
            (1, 8, 64, 2, 8, 64),
            (2, 32, 1, 0, 512, 64),
            (2, 65, 2, 3, 32, 64),
        ] {
            assert_encoder_fits(shape.0, shape.1, shape.2, shape.3, shape.4, shape.5);
        }
    }

    #[test]
    fn estimated_dictionary_oversize_is_not_a_second_format_validator() {
        let mut limits = QueryBlockLimits::default_for_memory();
        limits.maximum_block_bytes = 4096;
        limits.maximum_key_bytes = 4096;
        limits.maximum_value_bytes = 4096;
        let mut projected = Group::default();
        projected.record(16, 5156, true).unwrap();
        assert_eq!(projected.block_count(limits).unwrap(), 1);

        // Truly oversized term-shard lists are still rejected by the actual
        // encoder, even with enough memory and a successful admission estimate.
        let batch = shape_batch(1, 128, 1, 256, 8);
        limits.maximum_records = 64;
        let peak = query_run_seal_peak_bytes(&batch, limits).unwrap();
        let memory = IndexingMemoryCredits::new(
            peak,
            IndexingMemoryLimits {
                hot_payload_bytes: peak,
                worker_scratch_bytes: peak,
                prepared_rows_bytes: peak,
                replay_input_bytes: peak,
                projection_accumulator_bytes: peak,
                seal_scratch_bytes: peak,
                ordering_catalog_bytes: peak,
            },
        )
        .unwrap();
        let credits = QueryBlockCredits::from_pipeline_permit(
            memory
                .acquire(IndexingMemoryStage::SealScratch, peak)
                .unwrap(),
        );
        let partition = ProjectionPartitionIdentity::new([1; 32], 2, [3; 32], 2, 4, 5).unwrap();
        assert!(
            matches!(prepare_projection_query_run(partition, [2; 32], 1, 0, 1, 0, batch, limits, credits), Err(IndexError::ResourceLimit { needed, limit: 4096 }) if needed > 4096)
        );
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn grouped_records_share_blocks_and_planning_is_monotonic() {
        let limits = QueryBlockLimits::default_for_memory();
        let mut group = Group::default();
        for _ in 0..1_000 {
            group.record(32, 37, false).unwrap();
        }
        assert_eq!(group.block_count(limits).unwrap(), 1);
        let before = group.payload;
        group.record(32, 37, false).unwrap();
        assert!(group.payload > before);
        assert!(
            query_run_seal_peak_bytes(&PreparedQueryMutationBatch::default(), limits).unwrap()
                >= size_of::<ProjectionQueryRunDescriptor>() + 224
        );
    }
}
