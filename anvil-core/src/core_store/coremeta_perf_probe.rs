//! Redacted CoreMeta history evidence for the non-default performance gate.

use anyhow::Result;

use crate::anvil_api::{
    CoreMetaBatchFrame, CoreMetaHistoryCursor, CoreMetaInventory, CoreMetaInventoryCursor,
};

use super::CoreStore;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreMetaGenerationProbe {
    pub generation: u64,
    pub mutation_count: u64,
    pub mutation_bytes: u64,
    pub generation_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetaInventoryProbe {
    pub generations: Vec<CoreMetaGenerationProbe>,
    pub next_cursor: Option<CoreMetaInventoryCursor>,
    pub inventory_complete: bool,
    pub retention_floor_generation: u64,
    pub final_generation: u64,
    pub encoded_bytes: u64,
    pub page_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetaFrameProbe {
    pub generation: Option<u64>,
    pub generation_mutation_count: Option<u64>,
    pub delivered_mutation_count: usize,
    pub first_ordinal: Option<u64>,
    pub last_ordinal: Option<u64>,
    pub next_cursor: Option<CoreMetaHistoryCursor>,
    pub generation_complete: bool,
    pub history_complete: bool,
    pub encoded_bytes: u64,
    pub frame_hash: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetaCatchUpProbe {
    pub frames: Vec<CoreMetaFrameProbe>,
    pub next_cursor: Option<CoreMetaHistoryCursor>,
    pub history_complete: bool,
    pub retention_floor_generation: u64,
    pub final_generation: u64,
    pub delivered_mutation_count: usize,
    pub encoded_bytes: u64,
}

impl CoreStore {
    /// Executes one production inventory page and removes commit evidence.
    pub fn probe_coremeta_generation_inventory(
        &self,
        root_key_hash: &str,
        after: Option<&CoreMetaInventoryCursor>,
        through_generation: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<CoreMetaInventoryProbe> {
        let inventory = self.coremeta_generation_inventory(
            root_key_hash,
            after,
            through_generation,
            max_entries,
            max_bytes,
        )?;
        Ok(redact_inventory(inventory))
    }

    /// Executes one production catch-up page and removes rows and certificates.
    pub fn probe_coremeta_generation_catch_up(
        &self,
        root_key_hash: &str,
        after: Option<&CoreMetaHistoryCursor>,
        through_generation: u64,
        max_rows: usize,
        max_bytes: u64,
    ) -> Result<CoreMetaCatchUpProbe> {
        let frames = self.catch_up_coremeta_generation_history(
            root_key_hash,
            after,
            through_generation,
            max_rows,
            max_bytes,
        )?;
        Ok(redact_catch_up(frames))
    }
}

fn redact_inventory(inventory: CoreMetaInventory) -> CoreMetaInventoryProbe {
    CoreMetaInventoryProbe {
        generations: inventory
            .descriptors
            .into_iter()
            .map(|descriptor| CoreMetaGenerationProbe {
                generation: descriptor.generation,
                mutation_count: descriptor.mutation_count,
                mutation_bytes: descriptor.mutation_bytes,
                generation_hash: descriptor.generation_hash,
            })
            .collect(),
        next_cursor: inventory.next_cursor,
        inventory_complete: inventory.inventory_complete,
        retention_floor_generation: inventory.retention_floor_generation,
        final_generation: inventory.final_generation,
        encoded_bytes: inventory.encoded_bytes,
        page_hash: inventory.page_hash,
    }
}

fn redact_catch_up(frames: Vec<CoreMetaBatchFrame>) -> CoreMetaCatchUpProbe {
    let next_cursor = frames.last().and_then(|frame| frame.next_cursor.clone());
    let history_complete = frames.last().is_some_and(|frame| frame.history_complete);
    let final_generation = frames.first().map_or(0, |frame| frame.final_generation);
    let retention_floor_generation = frames
        .first()
        .map_or(0, |frame| frame.retention_floor_generation);
    let mut delivered_mutation_count = 0_usize;
    let mut encoded_bytes = 0_u64;
    let frames = frames
        .into_iter()
        .map(|frame| {
            delivered_mutation_count =
                delivered_mutation_count.saturating_add(frame.mutations.len());
            encoded_bytes = encoded_bytes.saturating_add(frame.encoded_bytes);
            redact_frame(frame)
        })
        .collect::<Vec<_>>();

    CoreMetaCatchUpProbe {
        frames,
        next_cursor,
        history_complete,
        retention_floor_generation,
        final_generation,
        delivered_mutation_count,
        encoded_bytes,
    }
}

fn redact_frame(frame: CoreMetaBatchFrame) -> CoreMetaFrameProbe {
    let generation = frame
        .descriptor
        .as_ref()
        .map(|descriptor| descriptor.generation)
        .or_else(|| frame.next_cursor.as_ref().map(|cursor| cursor.generation));
    let generation_mutation_count = frame
        .descriptor
        .as_ref()
        .map(|descriptor| descriptor.mutation_count);
    let first_ordinal = frame.mutations.first().map(|mutation| mutation.ordinal);
    let last_ordinal = frame.mutations.last().map(|mutation| mutation.ordinal);

    CoreMetaFrameProbe {
        generation,
        generation_mutation_count,
        delivered_mutation_count: frame.mutations.len(),
        first_ordinal,
        last_ordinal,
        next_cursor: frame.next_cursor,
        generation_complete: frame.generation_complete,
        history_complete: frame.history_complete,
        encoded_bytes: frame.encoded_bytes,
        frame_hash: frame.frame_hash,
    }
}

#[cfg(test)]
mod tests {
    use crate::anvil_api::{
        CoreMetaBatchFrame, CoreMetaCertificateEvidence, CoreMetaGenerationDescriptor,
        CoreMetaGenerationMutation, CoreMetaHistoryCursor, CoreMetaInventory,
        CoreMetaInventoryCursor, CoreMetaRowMutation,
    };

    use super::{redact_catch_up, redact_inventory};

    fn descriptor() -> CoreMetaGenerationDescriptor {
        CoreMetaGenerationDescriptor {
            root_key_hash: "root-hash".to_string(),
            generation: 7,
            transaction_id: "secret-transaction".to_string(),
            pending_batch_hash: "secret-pending".to_string(),
            committed_batch_hash: "secret-committed".to_string(),
            certificate_hash: "secret-certificate-hash".to_string(),
            commit_certificate: b"secret-certificate".to_vec(),
            certificate_persist_evidence: vec![CoreMetaCertificateEvidence {
                evidence_hash: "secret-evidence-hash".to_string(),
                evidence: b"secret-evidence".to_vec(),
            }],
            mutation_count: 12,
            mutation_bytes: 345,
            generation_hash: "generation-hash".to_string(),
            complete: true,
            created_at_unix_nanos: 99,
            coordinator_root_key_hash: None,
            coordinator_root_generation: None,
            column_families: Vec::new(),
            publication_bundle: b"coremeta-perf-publication-bundle".to_vec(),
        }
    }

    #[test]
    fn inventory_probe_redacts_descriptor_evidence() {
        let probe = redact_inventory(CoreMetaInventory {
            root_key_hash: "root-hash".to_string(),
            descriptors: vec![descriptor()],
            next_cursor: Some(CoreMetaInventoryCursor { generation: 7 }),
            inventory_complete: true,
            retention_floor_generation: 1,
            final_generation: 7,
            page_hash: "page-hash".to_string(),
            encoded_bytes: 456,
        });

        assert_eq!(probe.generations.len(), 1);
        assert_eq!(probe.generations[0].generation, 7);
        assert_eq!(probe.generations[0].mutation_count, 12);
        assert_eq!(probe.generations[0].generation_hash, "generation-hash");
        assert_eq!(probe.page_hash, "page-hash");
        assert!(!format!("{probe:?}").contains("secret"));
    }

    #[test]
    fn catch_up_probe_redacts_rows_and_certificates() {
        let probe = redact_catch_up(vec![CoreMetaBatchFrame {
            descriptor: Some(descriptor()),
            mutations: vec![CoreMetaGenerationMutation {
                root_key_hash: "root-hash".to_string(),
                generation: 7,
                ordinal: 4,
                mutation: Some(CoreMetaRowMutation {
                    column_family: "secret-column-family".to_string(),
                    core_meta_key: b"secret-key".to_vec(),
                    value_envelope: b"secret-value".to_vec(),
                    row_hash: "secret-row-hash".to_string(),
                    delete_marker: false,
                }),
            }],
            next_cursor: Some(CoreMetaHistoryCursor {
                generation: 7,
                ordinal: 4,
            }),
            generation_complete: false,
            history_complete: false,
            final_generation: 7,
            retention_floor_generation: 1,
            encoded_bytes: 789,
            frame_hash: "frame-hash".to_string(),
        }]);

        assert_eq!(probe.delivered_mutation_count, 1);
        assert_eq!(probe.frames[0].generation, Some(7));
        assert_eq!(probe.frames[0].first_ordinal, Some(4));
        assert_eq!(probe.frames[0].last_ordinal, Some(4));
        assert_eq!(probe.frames[0].frame_hash, "frame-hash");
        assert!(!format!("{probe:?}").contains("secret"));
    }
}
