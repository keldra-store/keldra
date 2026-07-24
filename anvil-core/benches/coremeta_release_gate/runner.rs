use std::hint::black_box;
use std::time::Instant;

use anvil_core::anvil_api::{CoreMetaHistoryCursor, CoreMetaInventoryCursor};
use anvil_core::core_store::{
    CF_INLINE_PAYLOADS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaCatchUpProbe,
    CoreMetaInventoryProbe, CoreMetaStore, CoreMutationBatch, CoreMutationOperation,
    CoreMutationRootPublication, CoreStore, CoreTransactionState, TABLE_INLINE_PAYLOAD_ROW,
    core_meta_root_key_hash, reset_coremeta_get_probe, take_coremeta_get_probe,
};
use anyhow::{Context, Result, bail};
use rocksdb::perf::{PerfContext, PerfStatsLevel, set_perf_stats};
use tokio::runtime::{Builder, Runtime};

use crate::config::{GateManifest, ProfileSpec};
use crate::dataset::{
    Dataset, LARGE_PREFIX, MULTI_PAGE_GENERATION_PREFIX, MUTATION_PREFIX, PUBLICATION_LARGE_PREFIX,
    PUBLICATION_SMALL_PREFIX, PublicationDataset, SMALL_PREFIX, encoded_payload, prefix_key,
    publication_payload, row_key,
};
use crate::report::{GateEvidence, ScenarioEvidence, WorkCounters};

pub struct RunResult {
    pub scenarios: Vec<ScenarioEvidence>,
    pub gates: Vec<GateEvidence>,
}

#[derive(Default)]
struct HistoryGateAssertions {
    inventory_snapshot_mismatches: u64,
    catch_up_cursor_mismatches: u64,
    traversal_cursor_mismatches: u64,
    traversal_pages: usize,
    expected_traversal_pages: usize,
    multi_page_publication_mutations: usize,
    expected_multi_page_publication_mutations: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraversalObservation {
    delivered_rows: usize,
    page_hashes: Vec<String>,
    cursor_mismatches: u64,
}

struct HistoryPageExpectation {
    first: CoreMetaHistoryCursor,
    next: CoreMetaHistoryCursor,
    complete: bool,
}

pub fn run(manifest: &GateManifest, profile: &ProfileSpec) -> Result<RunResult> {
    // RocksDB PerfContext is thread-local, so keep spawned publication work on this thread.
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let dataset = Dataset::create(manifest, profile, &runtime)?;
    let publication_small_dataset = PublicationDataset::create(manifest, profile, &runtime)?;
    set_perf_stats(PerfStatsLevel::EnableCount);

    let point_small = measure_point_get(
        &dataset.store,
        SMALL_PREFIX,
        profile.small_rows,
        profile,
        "point_get_small",
    )?;
    let point_large = measure_point_get(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        profile,
        "point_get_large",
    )?;
    let page_early_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        None,
        profile.page_size,
        profile,
        "prefix_page_early_large",
    )?;
    let page_deep_small = measure_prefix_page(
        &dataset.store,
        SMALL_PREFIX,
        profile.small_rows,
        Some(profile.small_rows - profile.page_size as u64 - 1),
        profile.page_size,
        profile,
        "prefix_page_deep_small",
    )?;
    let page_deep_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        Some(profile.large_rows - profile.page_size as u64 - 1),
        profile.page_size,
        profile,
        "prefix_page_deep_large",
    )?;
    let page_scaled_large = measure_prefix_page(
        &dataset.store,
        LARGE_PREFIX,
        profile.large_rows,
        Some(profile.large_rows - profile.scaling_page_size as u64 - 1),
        profile.scaling_page_size,
        profile,
        "bounded_list_scaled_page_large",
    )?;
    let durable_single = measure_durable_single_row(
        &dataset.store,
        &manifest.seed,
        profile,
        "durable_single_row",
    )?;
    let transactional_head = measure_transactional_head_batch(
        &dataset.store,
        &manifest.seed,
        profile,
        "transactional_head_read_and_batch",
    )?;
    let publication_small = measure_atomic_root_publication(
        &runtime,
        &publication_small_dataset.core_store,
        &publication_small_dataset.store,
        &manifest.seed,
        profile.small_rows,
        PUBLICATION_SMALL_PREFIX,
        "single-small",
        1,
        profile.payload_bytes,
        profile.root_publication_warmup_operations,
        profile.root_publication_samples,
        "atomic_root_publication_small_table",
    )?;
    let publication_large = measure_atomic_root_publication(
        &runtime,
        &dataset.core_store,
        &dataset.store,
        &manifest.seed,
        profile.large_rows,
        PUBLICATION_LARGE_PREFIX,
        "single-large",
        1,
        profile.payload_bytes,
        profile.root_publication_warmup_operations,
        profile.root_publication_samples,
        "atomic_root_publication_large_table",
    )?;
    let multi_page_generation = measure_atomic_root_publication(
        &runtime,
        &dataset.core_store,
        &dataset.store,
        &manifest.seed,
        profile.large_rows,
        MULTI_PAGE_GENERATION_PREFIX,
        "multi-page-generation",
        profile.multi_page_generation_rows,
        profile.payload_bytes,
        profile.multi_page_generation_warmup_operations,
        profile.multi_page_generation_samples,
        "atomic_root_publication_multi_page_generation",
    )?;

    let publication_generations = publication_operation_count(
        profile.root_publication_warmup_operations,
        profile.root_publication_samples,
    )?;
    let history_page_size_u64 = profile.history_page_size as u64;
    let inventory_after = CoreMetaInventoryCursor {
        generation: publication_generations - history_page_size_u64,
    };
    let small_root_anchor = publication_root_anchor_key("single-small");
    let large_root_anchor = publication_root_anchor_key("single-large");
    let small_root_hash = core_meta_root_key_hash(&small_root_anchor);
    let large_root_hash = core_meta_root_key_hash(&large_root_anchor);
    let mut history_assertions = HistoryGateAssertions::default();

    let inventory_small_expected = publication_small_dataset
        .core_store
        .probe_coremeta_generation_inventory(
            &small_root_hash,
            Some(&inventory_after),
            publication_generations,
            profile.history_page_size,
            profile.history_max_page_bytes,
        )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(inventory_probe_mismatches(
            &inventory_small_expected,
            &inventory_after,
            publication_generations,
            profile.history_page_size,
        ));
    let (inventory_small, inventory_small_mismatches) = measure_generation_inventory(
        &publication_small_dataset.core_store,
        &small_root_hash,
        &inventory_after,
        publication_generations,
        profile,
        profile.small_rows,
        &inventory_small_expected,
        "generation_inventory_small_table",
    )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(inventory_small_mismatches);

    let inventory_large_expected = dataset.core_store.probe_coremeta_generation_inventory(
        &large_root_hash,
        Some(&inventory_after),
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(inventory_probe_mismatches(
            &inventory_large_expected,
            &inventory_after,
            publication_generations,
            profile.history_page_size,
        ));
    let (inventory_large, inventory_large_mismatches) = measure_generation_inventory(
        &dataset.core_store,
        &large_root_hash,
        &inventory_after,
        publication_generations,
        profile,
        profile.large_rows,
        &inventory_large_expected,
        "generation_inventory_large_table",
    )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(inventory_large_mismatches);

    let grown_generation = publication_generations
        .checked_add(1)
        .context("CoreMeta history growth generation overflow")?;
    publish_atomic_root_generation(
        &runtime,
        &dataset.core_store,
        &manifest.seed,
        PUBLICATION_LARGE_PREFIX,
        "single-large",
        grown_generation,
        1,
        profile.payload_bytes,
    )?;
    let latest_inventory_after = CoreMetaInventoryCursor {
        generation: publication_generations,
    };
    let latest_inventory = dataset.core_store.probe_coremeta_generation_inventory(
        &large_root_hash,
        Some(&latest_inventory_after),
        0,
        1,
        profile.history_max_page_bytes,
    )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(inventory_probe_mismatches(
            &latest_inventory,
            &latest_inventory_after,
            grown_generation,
            1,
        ));
    let (inventory_after_growth, immutable_inventory_mismatches) = measure_generation_inventory(
        &dataset.core_store,
        &large_root_hash,
        &inventory_after,
        publication_generations,
        profile,
        profile.large_rows,
        &inventory_large_expected,
        "generation_inventory_captured_after_growth",
    )?;
    history_assertions.inventory_snapshot_mismatches = history_assertions
        .inventory_snapshot_mismatches
        .saturating_add(immutable_inventory_mismatches);

    let early_large_expectation = history_page_expectation(
        &dataset.core_store,
        &large_root_hash,
        None,
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let (catch_up_early_large, early_catch_up_mismatches) = measure_catch_up_page(
        &dataset.core_store,
        &large_root_hash,
        None,
        publication_generations,
        early_large_expectation.first,
        early_large_expectation.next,
        early_large_expectation.complete,
        profile,
        profile.large_rows,
        "generation_catch_up_early_large",
    )?;
    let small_deep_after = history_cursor_before_tail_rows(
        &publication_small_dataset.core_store,
        &small_root_hash,
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let small_deep_expectation = history_page_expectation(
        &publication_small_dataset.core_store,
        &small_root_hash,
        small_deep_after.as_ref(),
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let (catch_up_deep_small, deep_small_mismatches) = measure_catch_up_page(
        &publication_small_dataset.core_store,
        &small_root_hash,
        small_deep_after,
        publication_generations,
        small_deep_expectation.first,
        small_deep_expectation.next,
        small_deep_expectation.complete,
        profile,
        profile.small_rows,
        "generation_catch_up_deep_small",
    )?;
    let large_deep_after = history_cursor_before_tail_rows(
        &dataset.core_store,
        &large_root_hash,
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let large_deep_expectation = history_page_expectation(
        &dataset.core_store,
        &large_root_hash,
        large_deep_after.as_ref(),
        publication_generations,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let (catch_up_deep_large, deep_large_mismatches) = measure_catch_up_page(
        &dataset.core_store,
        &large_root_hash,
        large_deep_after,
        publication_generations,
        large_deep_expectation.first,
        large_deep_expectation.next,
        large_deep_expectation.complete,
        profile,
        profile.large_rows,
        "generation_catch_up_deep_large",
    )?;
    history_assertions.catch_up_cursor_mismatches = early_catch_up_mismatches
        .saturating_add(deep_small_mismatches)
        .saturating_add(deep_large_mismatches);

    let multi_page_generations = publication_operation_count(
        profile.multi_page_generation_warmup_operations,
        profile.multi_page_generation_samples,
    )?;
    let multi_page_root_anchor = publication_root_anchor_key("multi-page-generation");
    let multi_page_root_hash = core_meta_root_key_hash(&multi_page_root_anchor);
    let previous_multi_page_mutations = generation_mutation_count(
        &dataset.core_store,
        &multi_page_root_hash,
        multi_page_generations - 1,
        profile.history_max_page_bytes,
    )?;
    let multi_page_publication_mutations = generation_mutation_count(
        &dataset.core_store,
        &multi_page_root_hash,
        multi_page_generations,
        profile.history_max_page_bytes,
    )?;
    let expected_multi_page_publication_mutations =
        expected_publication_history_mutations(profile, profile.multi_page_generation_rows)?;
    let traversal_start = CoreMetaHistoryCursor {
        generation: multi_page_generations - 1,
        ordinal: previous_multi_page_mutations as u64 - 1,
    };
    let (multi_page_traversal, traversal, traversal_mismatches) =
        measure_multi_page_generation_traversal(
            &dataset.core_store,
            &multi_page_root_hash,
            traversal_start,
            multi_page_generations,
            multi_page_publication_mutations,
            profile,
            profile.large_rows,
            "generation_catch_up_multi_page_traversal",
        )?;
    history_assertions.traversal_cursor_mismatches = traversal_mismatches;
    history_assertions.traversal_pages = traversal.page_hashes.len();
    history_assertions.expected_traversal_pages =
        expected_multi_page_publication_mutations.div_ceil(profile.history_page_size);
    history_assertions.multi_page_publication_mutations = multi_page_publication_mutations;
    history_assertions.expected_multi_page_publication_mutations =
        expected_multi_page_publication_mutations;

    set_perf_stats(PerfStatsLevel::Disable);

    let scenarios = vec![
        point_small,
        point_large,
        page_early_large,
        page_deep_small,
        page_deep_large,
        page_scaled_large,
        durable_single,
        transactional_head,
        publication_small,
        publication_large,
        multi_page_generation,
        inventory_small,
        inventory_large,
        inventory_after_growth,
        catch_up_early_large,
        catch_up_deep_small,
        catch_up_deep_large,
        multi_page_traversal,
    ];
    let gates = evaluate_gates(&scenarios, profile, &history_assertions)?;
    Ok(RunResult { scenarios, gates })
}

fn measure_point_get(
    store: &CoreMetaStore,
    prefix: &str,
    dataset_rows: u64,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.point_samples;
    let keys = (0..operation_count)
        .map(|index| {
            let ordinal =
                ((index as u64).saturating_mul(7_919).saturating_add(104_729)) % dataset_rows;
            row_key(prefix, ordinal)
        })
        .collect::<Result<Vec<_>>>()?;

    measure(
        name,
        dataset_rows,
        1,
        profile.warmup_operations,
        profile.point_samples,
        |operation_index| {
            let value = store
                .get_inline_payload(&keys[operation_index])?
                .ok_or_else(|| anyhow::anyhow!("point benchmark row is missing"))?;
            black_box(value.len());
            Ok(1)
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn measure_prefix_page(
    store: &CoreMetaStore,
    prefix: &str,
    dataset_rows: u64,
    after_ordinal: Option<u64>,
    page_size: usize,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let tuple_prefix = prefix_key(prefix)?;
    let after_key = after_ordinal
        .map(|ordinal| row_key(prefix, ordinal))
        .transpose()?;

    measure(
        name,
        dataset_rows,
        page_size,
        profile.warmup_operations,
        profile.page_samples,
        |_| {
            let rows = store.scan_prefix_page(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &tuple_prefix,
                after_key.as_deref(),
                page_size,
            )?;
            black_box(rows.iter().map(|row| row.payload.len()).sum::<usize>());
            Ok(rows.len())
        },
    )
}

fn measure_durable_single_row(
    store: &CoreMetaStore,
    seed: &str,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.mutation_samples;
    let key = row_key(MUTATION_PREFIX, 0)?;
    let payloads = (0..operation_count)
        .map(|index| {
            encoded_payload(
                seed,
                "durable-single",
                index as u64,
                profile.payload_bytes,
                index as u64 + 1,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    measure(
        name,
        operation_count as u64,
        1,
        profile.warmup_operations,
        profile.mutation_samples,
        |operation_index| {
            let operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payloads[operation_index]),
            };
            store.write_batch(&[operation])?;
            Ok(1)
        },
    )
}

fn measure_transactional_head_batch(
    store: &CoreMetaStore,
    seed: &str,
    profile: &ProfileSpec,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = profile.warmup_operations + profile.mutation_samples;
    let head_key = row_key(MUTATION_PREFIX, 1)?;
    let version_keys = (0..operation_count)
        .map(|index| row_key(MUTATION_PREFIX, index as u64 + 2))
        .collect::<Result<Vec<_>>>()?;
    let head_payloads = (0..=operation_count)
        .map(|index| {
            encoded_payload(
                seed,
                "transactional-head",
                index as u64,
                profile.payload_bytes,
                index as u64 + 1,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    write_one(store, &head_key, &head_payloads[0])?;

    let evidence = measure(
        name,
        operation_count as u64,
        2,
        profile.warmup_operations,
        profile.mutation_samples,
        |operation_index| {
            let current = store
                .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &head_key)?
                .ok_or_else(|| anyhow::anyhow!("transactional head is missing"))?;
            if current != head_payloads[operation_index] {
                bail!("transactional head did not retain the preceding generation");
            }

            let version_operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &version_keys[operation_index],
                common: None,
                kind: CoreMetaBatchOpKind::Put(&head_payloads[operation_index + 1]),
            };
            let head_operation = CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: &head_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&head_payloads[operation_index + 1]),
            };
            store.write_batch(&[version_operation, head_operation])?;
            Ok(2)
        },
    )?;

    let final_head = store
        .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &head_key)?
        .ok_or_else(|| anyhow::anyhow!("transactional head disappeared"))?;
    if final_head != head_payloads[operation_count] {
        bail!("transactional head does not contain the final generation");
    }
    Ok(evidence)
}

#[allow(clippy::too_many_arguments)]
fn measure_atomic_root_publication(
    runtime: &Runtime,
    store: &CoreStore,
    meta: &CoreMetaStore,
    seed: &str,
    dataset_rows: u64,
    prefix: &str,
    fixture_id: &str,
    mutation_rows: usize,
    payload_bytes: usize,
    warmup_operations: usize,
    samples: usize,
    name: &str,
) -> Result<ScenarioEvidence> {
    let operation_count = warmup_operations
        .checked_add(samples)
        .context("CoreMeta publication operation count overflow")?;
    let root_anchor_key = publication_root_anchor_key(fixture_id);
    let keys = (0..mutation_rows)
        .map(|ordinal| row_key(prefix, ordinal as u64))
        .collect::<Result<Vec<_>>>()?;
    let mut batches = (0..operation_count)
        .map(|operation_index| {
            let generation = operation_index as u64 + 1;
            publication_batch(
                seed,
                prefix,
                fixture_id,
                &root_anchor_key,
                generation,
                &keys,
                payload_bytes,
            )
            .map(Some)
        })
        .collect::<Result<Vec<_>>>()?;

    let diagnostics = std::env::var_os("ANVIL_COREMETA_PERF_DIAGNOSTICS").is_some();
    if diagnostics {
        reset_coremeta_get_probe();
    }
    let evidence = measure(
        name,
        dataset_rows,
        mutation_rows,
        warmup_operations,
        samples,
        |operation_index| {
            let batch = batches
                .get_mut(operation_index)
                .and_then(Option::take)
                .context("CoreMeta publication benchmark reused an operation")?;
            let receipt = runtime.block_on(store.commit_mutation_batch(batch))?;
            if receipt.state != CoreTransactionState::Committed
                || receipt.finalisation_error.is_some()
            {
                bail!(
                    "CoreMeta publication {} did not commit cleanly: state={:?}, error={:?}",
                    receipt.transaction_id,
                    receipt.state,
                    receipt.finalisation_error
                );
            }
            Ok(receipt.visible_updates.len())
        },
    )?;
    if diagnostics {
        eprintln!(
            "[coremeta-perf-gate] scenario={name} coremeta_gets={:?}",
            take_coremeta_get_probe()
        );
    }

    let final_generation = operation_count as u64;
    let anchor =
        runtime.block_on(store.read_internal_root_anchor(&root_anchor_key, final_generation))?;
    if anchor.generation != final_generation {
        bail!(
            "CoreMeta publication root stopped at generation {}, expected {final_generation}",
            anchor.generation
        );
    }
    let final_transaction_id = publication_transaction_id(fixture_id, final_generation);
    for ordinal in [0, mutation_rows - 1] {
        let expected = publication_payload(
            seed,
            prefix,
            ordinal as u64,
            payload_bytes,
            &root_anchor_key,
            final_generation,
            &final_transaction_id,
        )?;
        let actual = meta
            .get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, &keys[ordinal])?
            .context("CoreMeta publication row is missing after root publication")?;
        if actual != expected {
            bail!(
                "CoreMeta publication row {ordinal} does not match root generation {final_generation}"
            );
        }
    }

    Ok(evidence)
}

#[allow(clippy::too_many_arguments)]
fn measure_generation_inventory(
    store: &CoreStore,
    root_key_hash: &str,
    after: &CoreMetaInventoryCursor,
    through_generation: u64,
    profile: &ProfileSpec,
    dataset_rows: u64,
    expected: &CoreMetaInventoryProbe,
    name: &str,
) -> Result<(ScenarioEvidence, u64)> {
    let mut probe_mismatches = 0_u64;
    let evidence = measure(
        name,
        dataset_rows,
        profile.history_page_size,
        profile.history_probe_warmup_operations,
        profile.history_probe_samples,
        |_| {
            let probe = store.probe_coremeta_generation_inventory(
                root_key_hash,
                Some(after),
                through_generation,
                profile.history_page_size,
                profile.history_max_page_bytes,
            )?;
            probe_mismatches = probe_mismatches.saturating_add(mismatch(&probe != expected));
            black_box(&probe.page_hash);
            Ok(probe.generations.len())
        },
    )?;
    Ok((evidence, probe_mismatches))
}

fn inventory_probe_mismatches(
    probe: &CoreMetaInventoryProbe,
    after: &CoreMetaInventoryCursor,
    through_generation: u64,
    expected_entries: usize,
) -> u64 {
    [
        probe.generations.len() != expected_entries,
        probe.generations.first().map(|item| item.generation)
            != Some(after.generation.saturating_add(1)),
        probe.generations.last().map(|item| item.generation) != Some(through_generation),
        probe.next_cursor.as_ref().map(|cursor| cursor.generation) != Some(through_generation),
        !probe.inventory_complete,
        probe.final_generation != through_generation,
        probe.encoded_bytes == 0,
        probe.page_hash.is_empty(),
    ]
    .into_iter()
    .map(mismatch)
    .sum()
}

fn generation_mutation_count(
    store: &CoreStore,
    root_key_hash: &str,
    generation: u64,
    max_bytes: u64,
) -> Result<usize> {
    let after = (generation > 1).then_some(CoreMetaInventoryCursor {
        generation: generation - 1,
    });
    let inventory = store.probe_coremeta_generation_inventory(
        root_key_hash,
        after.as_ref(),
        generation,
        1,
        max_bytes,
    )?;
    let [descriptor] = inventory.generations.as_slice() else {
        bail!("CoreMeta generation {generation} did not produce one inventory descriptor");
    };
    if descriptor.generation != generation || !inventory.inventory_complete {
        bail!("CoreMeta generation {generation} inventory did not stop at its captured boundary");
    }
    usize::try_from(descriptor.mutation_count)
        .context("CoreMeta generation mutation count exceeds usize")
}

fn expected_publication_history_mutations(
    profile: &ProfileSpec,
    logical_mutations: usize,
) -> Result<usize> {
    logical_mutations
        .checked_mul(profile.root_publication_history_rows_per_logical_mutation)
        .and_then(|rows| rows.checked_add(profile.root_publication_history_fixed_rows))
        .context("CoreMeta expected publication history mutation count overflow")
}

fn history_page_expectation(
    store: &CoreStore,
    root_key_hash: &str,
    after: Option<&CoreMetaHistoryCursor>,
    through_generation: u64,
    rows: usize,
    max_bytes: u64,
) -> Result<HistoryPageExpectation> {
    let mut generation = after.map_or(1, |cursor| cursor.generation);
    let mut ordinal = after.map_or(0, |cursor| cursor.ordinal.saturating_add(1));
    let mut generation_rows =
        generation_mutation_count(store, root_key_hash, generation, max_bytes)?;
    if ordinal as usize >= generation_rows {
        generation = generation.saturating_add(1);
        ordinal = 0;
        generation_rows = generation_mutation_count(store, root_key_hash, generation, max_bytes)?;
    }
    let first = CoreMetaHistoryCursor {
        generation,
        ordinal,
    };
    let mut remaining = rows;
    let next = loop {
        if generation > through_generation {
            bail!("CoreMeta history does not contain the requested expectation page");
        }
        let available = generation_rows.saturating_sub(ordinal as usize);
        if remaining <= available {
            break CoreMetaHistoryCursor {
                generation,
                ordinal: ordinal.saturating_add(remaining as u64 - 1),
            };
        }
        remaining -= available;
        generation = generation.saturating_add(1);
        ordinal = 0;
        generation_rows = generation_mutation_count(store, root_key_hash, generation, max_bytes)?;
    };
    let complete = next.generation == through_generation
        && next.ordinal.saturating_add(1)
            == generation_mutation_count(store, root_key_hash, through_generation, max_bytes)?
                as u64;
    Ok(HistoryPageExpectation {
        first,
        next,
        complete,
    })
}

fn history_cursor_before_tail_rows(
    store: &CoreStore,
    root_key_hash: &str,
    through_generation: u64,
    rows: usize,
    max_bytes: u64,
) -> Result<Option<CoreMetaHistoryCursor>> {
    if rows == 0 {
        bail!("CoreMeta history tail page must request at least one row");
    }
    let mut generation = through_generation;
    let mut remaining = rows;
    loop {
        let generation_rows =
            generation_mutation_count(store, root_key_hash, generation, max_bytes)?;
        if remaining < generation_rows {
            return Ok(Some(CoreMetaHistoryCursor {
                generation,
                ordinal: (generation_rows - remaining - 1) as u64,
            }));
        }
        remaining -= generation_rows;
        if generation == 1 {
            if remaining == 0 {
                return Ok(None);
            }
            bail!("CoreMeta history is shorter than the requested tail page");
        }
        generation -= 1;
        if remaining == 0 {
            let previous_rows =
                generation_mutation_count(store, root_key_hash, generation, max_bytes)?;
            return Ok(Some(CoreMetaHistoryCursor {
                generation,
                ordinal: previous_rows as u64 - 1,
            }));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn measure_catch_up_page(
    store: &CoreStore,
    root_key_hash: &str,
    after: Option<CoreMetaHistoryCursor>,
    through_generation: u64,
    expected_first_cursor: CoreMetaHistoryCursor,
    expected_next_cursor: CoreMetaHistoryCursor,
    expected_history_complete: bool,
    profile: &ProfileSpec,
    dataset_rows: u64,
    name: &str,
) -> Result<(ScenarioEvidence, u64)> {
    let expected = store.probe_coremeta_generation_catch_up(
        root_key_hash,
        after.as_ref(),
        through_generation,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let mut probe_mismatches = catch_up_page_mismatches(
        &expected,
        &expected_first_cursor,
        &expected_next_cursor,
        through_generation,
        profile.history_page_size,
        expected_history_complete,
    );
    let evidence = measure(
        name,
        dataset_rows,
        profile.history_page_size,
        profile.history_probe_warmup_operations,
        profile.history_probe_samples,
        |_| {
            let probe = store.probe_coremeta_generation_catch_up(
                root_key_hash,
                after.as_ref(),
                through_generation,
                profile.history_page_size,
                profile.history_max_page_bytes,
            )?;
            probe_mismatches = probe_mismatches.saturating_add(mismatch(probe != expected));
            black_box(probe.encoded_bytes);
            Ok(probe.delivered_mutation_count)
        },
    )?;
    Ok((evidence, probe_mismatches))
}

fn catch_up_page_mismatches(
    probe: &CoreMetaCatchUpProbe,
    expected_first_cursor: &CoreMetaHistoryCursor,
    expected_next_cursor: &CoreMetaHistoryCursor,
    through_generation: u64,
    expected_rows: usize,
    expected_history_complete: bool,
) -> u64 {
    let mut mismatches = [
        probe.delivered_mutation_count != expected_rows,
        probe.next_cursor.as_ref() != Some(expected_next_cursor),
        probe.history_complete != expected_history_complete,
        probe.final_generation != through_generation,
        probe.encoded_bytes == 0,
        probe.frames.is_empty(),
    ]
    .into_iter()
    .map(mismatch)
    .sum::<u64>();

    let mut expected_generation = expected_first_cursor.generation;
    let mut expected_ordinal = expected_first_cursor.ordinal;
    let mut delivered_rows = 0_usize;
    for frame in &probe.frames {
        let delivered = frame.delivered_mutation_count;
        let expected_last_ordinal =
            expected_ordinal.saturating_add((delivered as u64).saturating_sub(1));
        let generation_mutation_count = frame.generation_mutation_count.unwrap_or(0);
        let generation_complete =
            expected_last_ordinal.saturating_add(1) == generation_mutation_count;
        let history_complete = generation_complete && expected_generation == through_generation;
        mismatches = mismatches.saturating_add(
            [
                frame.generation != Some(expected_generation),
                generation_mutation_count == 0,
                delivered == 0,
                frame.first_ordinal != Some(expected_ordinal),
                frame.last_ordinal != Some(expected_last_ordinal),
                frame.next_cursor.as_ref()
                    != Some(&CoreMetaHistoryCursor {
                        generation: expected_generation,
                        ordinal: expected_last_ordinal,
                    }),
                frame.generation_complete != generation_complete,
                frame.history_complete != history_complete,
                frame.encoded_bytes == 0,
                frame.frame_hash.is_empty(),
            ]
            .into_iter()
            .map(mismatch)
            .sum::<u64>(),
        );
        delivered_rows = delivered_rows.saturating_add(delivered);
        if generation_complete {
            expected_generation = expected_generation.saturating_add(1);
            expected_ordinal = 0;
        } else {
            expected_ordinal = expected_last_ordinal.saturating_add(1);
        }
    }
    mismatches.saturating_add(mismatch(delivered_rows != expected_rows))
}

#[allow(clippy::too_many_arguments)]
fn measure_multi_page_generation_traversal(
    store: &CoreStore,
    root_key_hash: &str,
    start_cursor: CoreMetaHistoryCursor,
    through_generation: u64,
    generation_rows: usize,
    profile: &ProfileSpec,
    dataset_rows: u64,
    name: &str,
) -> Result<(ScenarioEvidence, TraversalObservation, u64)> {
    let expected = traverse_multi_page_generation(
        store,
        root_key_hash,
        &start_cursor,
        through_generation,
        generation_rows,
        profile.history_page_size,
        profile.history_max_page_bytes,
    )?;
    let mut traversal_mismatches = expected.cursor_mismatches;
    let evidence = measure(
        name,
        dataset_rows,
        generation_rows,
        profile.multi_page_generation_warmup_operations,
        profile.multi_page_generation_samples,
        |_| {
            let observation = traverse_multi_page_generation(
                store,
                root_key_hash,
                &start_cursor,
                through_generation,
                generation_rows,
                profile.history_page_size,
                profile.history_max_page_bytes,
            )?;
            traversal_mismatches = traversal_mismatches
                .saturating_add(observation.cursor_mismatches)
                .saturating_add(mismatch(
                    observation.page_hashes.as_slice() != expected.page_hashes.as_slice(),
                ));
            Ok(observation.delivered_rows)
        },
    )?;
    Ok((evidence, expected, traversal_mismatches))
}

fn traverse_multi_page_generation(
    store: &CoreStore,
    root_key_hash: &str,
    start_cursor: &CoreMetaHistoryCursor,
    through_generation: u64,
    generation_rows: usize,
    page_size: usize,
    max_bytes: u64,
) -> Result<TraversalObservation> {
    let expected_pages = generation_rows.div_ceil(page_size);
    let mut cursor = start_cursor.clone();
    let mut delivered_rows = 0_usize;
    let mut page_hashes = Vec::with_capacity(expected_pages);
    let mut cursor_mismatches = 0_u64;

    while delivered_rows < generation_rows {
        if page_hashes.len() >= expected_pages {
            bail!("CoreMeta multi-page traversal exceeded its bounded page count");
        }
        let probe = store.probe_coremeta_generation_catch_up(
            root_key_hash,
            Some(&cursor),
            through_generation,
            page_size,
            max_bytes,
        )?;
        if probe.delivered_mutation_count == 0 {
            bail!("CoreMeta multi-page traversal made no cursor progress");
        }
        let expected_first_ordinal = delivered_rows as u64;
        let next_delivered = delivered_rows.saturating_add(probe.delivered_mutation_count);
        let expected_last_ordinal = next_delivered.saturating_sub(1) as u64;
        let expected_complete = next_delivered == generation_rows;
        let expected_cursor = CoreMetaHistoryCursor {
            generation: through_generation,
            ordinal: expected_last_ordinal,
        };
        let page_mismatches = [
            probe.frames.len() != 1,
            probe.delivered_mutation_count > page_size,
            next_delivered > generation_rows,
            probe.next_cursor.as_ref() != Some(&expected_cursor),
            probe.history_complete != expected_complete,
            probe.final_generation != through_generation,
            probe.encoded_bytes == 0,
        ]
        .into_iter()
        .map(mismatch)
        .sum::<u64>();
        cursor_mismatches = cursor_mismatches.saturating_add(page_mismatches);

        if let [frame] = probe.frames.as_slice() {
            let frame_mismatches = [
                frame.generation != Some(through_generation),
                frame.generation_mutation_count != Some(generation_rows as u64),
                frame.delivered_mutation_count != probe.delivered_mutation_count,
                frame.first_ordinal != Some(expected_first_ordinal),
                frame.last_ordinal != Some(expected_last_ordinal),
                frame.next_cursor.as_ref() != Some(&expected_cursor),
                frame.generation_complete != expected_complete,
                frame.history_complete != expected_complete,
                frame.encoded_bytes == 0,
                frame.frame_hash.is_empty(),
            ]
            .into_iter()
            .map(mismatch)
            .sum::<u64>();
            cursor_mismatches = cursor_mismatches.saturating_add(frame_mismatches);
            if page_mismatches != 0 || frame_mismatches != 0 {
                eprintln!(
                    "[coremeta-perf-gate] traversal expectation mismatch delivered_before={delivered_rows} page_mismatches={page_mismatches} frame_mismatches={frame_mismatches} expected_cursor={expected_cursor:?} expected_complete={expected_complete} probe={probe:#?}"
                );
            }
            page_hashes.push(frame.frame_hash.clone());
        } else {
            page_hashes.push(String::new());
        }

        cursor = probe
            .next_cursor
            .context("CoreMeta multi-page traversal returned no next cursor")?;
        delivered_rows = next_delivered;
        if probe.history_complete {
            break;
        }
    }

    cursor_mismatches = cursor_mismatches.saturating_add(
        [
            delivered_rows != generation_rows,
            page_hashes.len() != expected_pages,
            cursor.generation != through_generation,
            cursor.ordinal != generation_rows as u64 - 1,
        ]
        .into_iter()
        .map(mismatch)
        .sum::<u64>(),
    );
    Ok(TraversalObservation {
        delivered_rows,
        page_hashes,
        cursor_mismatches,
    })
}

#[allow(clippy::too_many_arguments)]
fn publish_atomic_root_generation(
    runtime: &Runtime,
    store: &CoreStore,
    seed: &str,
    prefix: &str,
    fixture_id: &str,
    generation: u64,
    mutation_rows: usize,
    payload_bytes: usize,
) -> Result<()> {
    let root_anchor_key = publication_root_anchor_key(fixture_id);
    let keys = (0..mutation_rows)
        .map(|ordinal| row_key(prefix, ordinal as u64))
        .collect::<Result<Vec<_>>>()?;
    let batch = publication_batch(
        seed,
        prefix,
        fixture_id,
        &root_anchor_key,
        generation,
        &keys,
        payload_bytes,
    )?;
    let receipt = runtime.block_on(store.commit_mutation_batch(batch))?;
    if receipt.state != CoreTransactionState::Committed
        || receipt.finalisation_error.is_some()
        || receipt.visible_updates.len() != mutation_rows
    {
        bail!("CoreMeta history growth publication did not commit exactly {mutation_rows} rows");
    }
    let anchor = runtime.block_on(store.read_internal_root_anchor(&root_anchor_key, generation))?;
    if anchor.generation != generation {
        bail!(
            "CoreMeta history growth publication reached generation {}, expected {generation}",
            anchor.generation
        );
    }
    Ok(())
}

fn publication_operation_count(warmup_operations: usize, samples: usize) -> Result<u64> {
    let count = warmup_operations
        .checked_add(samples)
        .context("CoreMeta publication operation count overflow")?;
    u64::try_from(count).context("CoreMeta publication operation count exceeds u64")
}

fn publication_root_anchor_key(fixture_id: &str) -> String {
    format!("coremeta-release-gate/publication/{fixture_id}")
}

fn mismatch(condition: bool) -> u64 {
    if condition { 1 } else { 0 }
}

fn publication_batch(
    seed: &str,
    prefix: &str,
    fixture_id: &str,
    root_anchor_key: &str,
    generation: u64,
    keys: &[Vec<u8>],
    payload_bytes: usize,
) -> Result<CoreMutationBatch> {
    let transaction_id = publication_transaction_id(fixture_id, generation);
    let operations = keys
        .iter()
        .enumerate()
        .map(|(ordinal, tuple_key)| {
            Ok(CoreMutationOperation::CoreMetaPut {
                partition_id: root_anchor_key.to_string(),
                cf: CF_INLINE_PAYLOADS.to_string(),
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: tuple_key.clone(),
                payload: publication_payload(
                    seed,
                    prefix,
                    ordinal as u64,
                    payload_bytes,
                    root_anchor_key,
                    generation,
                    &transaction_id,
                )?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(CoreMutationBatch {
        transaction_id,
        scope_partition: root_anchor_key.to_string(),
        committed_by_principal: "coremeta-release-gate".to_string(),
        root_publications: vec![
            CoreMutationRootPublication::new(root_anchor_key, "core_control").coordinator(),
        ],
        preconditions: Vec::new(),
        operations,
    })
}

fn publication_transaction_id(fixture_id: &str, generation: u64) -> String {
    format!("coremeta-perf-{fixture_id}-{generation:020}")
}

fn write_one(store: &CoreMetaStore, key: &[u8], payload: &[u8]) -> Result<()> {
    store.write_batch(&[CoreMetaBatchOp {
        cf: CF_INLINE_PAYLOADS,
        table_id: TABLE_INLINE_PAYLOAD_ROW,
        tuple_key: key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(payload),
    }])
}

fn measure(
    name: &str,
    dataset_rows: u64,
    expected_items: usize,
    warmup_operations: usize,
    samples: usize,
    mut operation: impl FnMut(usize) -> Result<usize>,
) -> Result<ScenarioEvidence> {
    for operation_index in 0..warmup_operations {
        let returned = operation(operation_index)?;
        if returned != expected_items {
            bail!("{name} warmup returned {returned} items, expected {expected_items}");
        }
    }

    let mut perf_context = PerfContext::default();
    perf_context.reset();
    let mut latency_ns = Vec::with_capacity(samples);
    let mut returned_items = 0_u64;
    let mut item_count_mismatches = 0_u64;
    for sample in 0..samples {
        let operation_index = warmup_operations + sample;
        let started_at = Instant::now();
        let returned = operation(operation_index)?;
        latency_ns.push(started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        returned_items = returned_items.saturating_add(returned as u64);
        if returned != expected_items {
            item_count_mismatches = item_count_mismatches.saturating_add(1);
        }
    }
    let work = WorkCounters::capture(&perf_context);

    Ok(ScenarioEvidence::from_samples(
        name,
        dataset_rows,
        expected_items,
        returned_items,
        item_count_mismatches,
        latency_ns,
        work,
    ))
}

fn evaluate_gates(
    scenarios: &[ScenarioEvidence],
    profile: &ProfileSpec,
    history_assertions: &HistoryGateAssertions,
) -> Result<Vec<GateEvidence>> {
    let point_small = scenario(scenarios, "point_get_small")?;
    let point_large = scenario(scenarios, "point_get_large")?;
    let page_early = scenario(scenarios, "prefix_page_early_large")?;
    let page_deep_small = scenario(scenarios, "prefix_page_deep_small")?;
    let page_deep_large = scenario(scenarios, "prefix_page_deep_large")?;
    let page_scaled = scenario(scenarios, "bounded_list_scaled_page_large")?;
    let durable_single = scenario(scenarios, "durable_single_row")?;
    let transactional = scenario(scenarios, "transactional_head_read_and_batch")?;
    let publication_small = scenario(scenarios, "atomic_root_publication_small_table")?;
    let publication_large = scenario(scenarios, "atomic_root_publication_large_table")?;
    let multi_page_generation =
        scenario(scenarios, "atomic_root_publication_multi_page_generation")?;
    let inventory_small = scenario(scenarios, "generation_inventory_small_table")?;
    let inventory_large = scenario(scenarios, "generation_inventory_large_table")?;
    let inventory_after_growth = scenario(scenarios, "generation_inventory_captured_after_growth")?;
    let catch_up_early = scenario(scenarios, "generation_catch_up_early_large")?;
    let catch_up_deep_small = scenario(scenarios, "generation_catch_up_deep_small")?;
    let catch_up_deep_large = scenario(scenarios, "generation_catch_up_deep_large")?;
    let multi_page_traversal = scenario(scenarios, "generation_catch_up_multi_page_traversal")?;
    let thresholds = &profile.thresholds;
    let mut gates = Vec::new();

    for evidence in scenarios {
        gates.push(exact_gate(
            &format!("{}_bounded_result", evidence.name),
            "correctness",
            "items_per_operation",
            evidence.item_count_mismatches as f64,
            0.0,
            "operations",
            "Every operation must return or mutate exactly the declared bounded item count.",
        ));
    }

    let read_counter_total = [
        point_small,
        point_large,
        page_early,
        page_deep_small,
        page_deep_large,
        page_scaled,
        inventory_small,
        inventory_large,
        inventory_after_growth,
        catch_up_early,
        catch_up_deep_small,
        catch_up_deep_large,
        multi_page_traversal,
    ]
    .iter()
    .map(|evidence| evidence.work.logical_work())
    .sum::<u64>() as f64;
    gates.push(min_gate(
        "rocksdb_work_counters_present",
        "correctness",
        "logical_work",
        read_counter_total,
        1.0,
        "count",
        "A complexity gate cannot pass when RocksDB work counters are absent.",
    ));

    for point in [point_small, point_large] {
        gates.push(max_gate(
            &format!("{}_p95", point.name),
            "latency",
            "p95_ms",
            point.p95_ms,
            thresholds.point_get_p95_ms,
            thresholds.point_get_p95_ms,
            "ms",
            "Point-get p95 uses a deliberately conservative hard ceiling.",
        ));
    }

    let page_latency_limit = thresholds
        .page_p95_floor_ms
        .max(point_large.p95_ms * thresholds.page_p95_point_multiplier);
    for page in [page_early, page_deep_small, page_deep_large, page_scaled] {
        gates.push(max_gate(
            &format!("{}_p95", page.name),
            "latency",
            "p95_ms",
            page.p95_ms,
            thresholds.page_p95_floor_ms,
            page_latency_limit,
            "ms",
            "Page latency is calibrated to point-read p95 with a fixed lower ceiling.",
        ));
    }

    gates.push(max_gate(
        "durable_single_row_p95",
        "latency",
        "p95_ms",
        durable_single.p95_ms,
        thresholds.durable_single_p95_ms,
        thresholds.durable_single_p95_ms,
        "ms",
        "Single-row durable write is the local fsync calibration primitive.",
    ));
    let transactional_latency_limit = thresholds
        .transactional_head_p95_floor_ms
        .max(durable_single.p95_ms * thresholds.transactional_head_single_multiplier);
    gates.push(max_gate(
        "transactional_head_read_and_batch_p95",
        "latency",
        "p95_ms",
        transactional.p95_ms,
        thresholds.transactional_head_p95_floor_ms,
        transactional_latency_limit,
        "ms",
        "Head read plus one two-row atomic batch is calibrated to local durable-write p95.",
    ));

    gates.push(ratio_gate(
        "point_get_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        point_large.logical_work_per_operation,
        point_small.logical_work_per_operation,
        thresholds.point_work_growth_ratio,
        "ratio",
        "Increasing rows by at least 8x must not produce table-sized point-read work.",
    ));
    gates.push(ratio_gate(
        "deep_page_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        page_deep_large.logical_work_per_operation,
        page_deep_small.logical_work_per_operation,
        thresholds.deep_page_work_growth_ratio,
        "ratio",
        "A deep cursor must seek; it must not walk rows preceding the cursor.",
    ));
    gates.push(ratio_gate(
        "deep_page_work_is_comparable_to_early_page",
        "complexity",
        "logical_work_per_operation_ratio",
        page_deep_large.logical_work_per_operation,
        page_early.logical_work_per_operation,
        thresholds.deep_to_early_work_ratio,
        "ratio",
        "Deep and early pages of equal size must perform comparable bounded work.",
    ));
    let page_size_ratio = profile.scaling_page_size as f64 / profile.page_size as f64;
    gates.push(ratio_gate(
        "bounded_list_work_scales_with_page_size",
        "complexity",
        "logical_work_per_operation_ratio",
        page_scaled.logical_work_per_operation,
        page_deep_large.logical_work_per_operation,
        page_size_ratio * thresholds.page_size_work_ratio_multiplier,
        "ratio",
        "Increasing only page size may increase work proportionally, not with table rows.",
    ));

    for (page, page_size) in [
        (page_early, profile.page_size),
        (page_deep_small, profile.page_size),
        (page_deep_large, profile.page_size),
        (page_scaled, profile.scaling_page_size),
    ] {
        let work_limit =
            page_size as f64 * thresholds.page_work_per_item + thresholds.page_fixed_work;
        gates.push(max_gate(
            &format!("{}_bounded_work", page.name),
            "complexity",
            "logical_work_per_operation",
            page.logical_work_per_operation,
            work_limit,
            work_limit,
            "count/op",
            "Absolute iterator work is capped by page size plus fixed seek overhead.",
        ));
    }

    gates.push(ratio_gate(
        "atomic_root_publication_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        publication_large.logical_work_per_operation,
        publication_small.logical_work_per_operation,
        thresholds.root_publication_work_growth_ratio,
        "ratio",
        "Publishing one rooted mutation must not scan canonical or history tables as they grow.",
    ));
    let generation_row_ratio_limit = profile.multi_page_generation_rows as f64
        * thresholds.root_publication_row_work_ratio_multiplier;
    gates.push(ratio_gate(
        "atomic_root_publication_work_scales_with_generation_rows",
        "complexity",
        "logical_work_per_operation_ratio",
        multi_page_generation.logical_work_per_operation,
        publication_large.logical_work_per_operation,
        generation_row_ratio_limit,
        "ratio",
        "A page-spanning generation may add work per declared mutation, not per table row.",
    ));
    for publication in [publication_small, publication_large, multi_page_generation] {
        let requested_mutations = publication.expected_items_per_operation as f64;
        let work_limit = requested_mutations * thresholds.root_publication_work_per_mutation
            + thresholds.root_publication_fixed_work;
        gates.push(max_gate(
            &format!("{}_bounded_work", publication.name),
            "complexity",
            "logical_work_per_operation",
            publication.logical_work_per_operation,
            work_limit,
            work_limit,
            "count/op",
            "Root publication work is capped per requested logical mutation, including transaction evidence and R3/Q2 replication, plus fixed protocol overhead.",
        ));
    }

    gates.push(exact_gate(
        "generation_inventory_snapshot_is_immutable",
        "correctness",
        "probe_mismatches",
        history_assertions.inventory_snapshot_mismatches as f64,
        0.0,
        "probes",
        "A captured generation inventory must remain identical after a later root publication.",
    ));
    gates.push(exact_gate(
        "deep_catch_up_cursor_progress_is_exact",
        "correctness",
        "cursor_mismatches",
        history_assertions.catch_up_cursor_mismatches as f64,
        0.0,
        "checks",
        "Early and deep catch-up pages must resume at exactly the generation/ordinal cursor.",
    ));
    gates.push(exact_gate(
        "multi_page_generation_cursor_progress_is_exact",
        "correctness",
        "cursor_mismatches",
        history_assertions.traversal_cursor_mismatches as f64,
        0.0,
        "checks",
        "Every page of one generation must advance without gaps, duplicates, or early completion.",
    ));
    gates.push(exact_gate(
        "multi_page_generation_history_shape_is_exact",
        "correctness",
        "history_mutations",
        history_assertions.multi_page_publication_mutations as f64,
        history_assertions.expected_multi_page_publication_mutations as f64,
        "mutations",
        "Generation history must contain the protocol-defined canonical, transaction-update, header, and publication-manifest rows.",
    ));
    gates.push(exact_gate(
        "multi_page_generation_uses_expected_page_count",
        "correctness",
        "pages",
        history_assertions.traversal_pages as f64,
        history_assertions.expected_traversal_pages as f64,
        "pages",
        "A generation larger than the page bound must drain in the exact bounded page count.",
    ));

    gates.push(ratio_gate(
        "generation_inventory_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        inventory_large.logical_work_per_operation,
        inventory_small.logical_work_per_operation,
        thresholds.history_work_growth_ratio,
        "ratio",
        "A fixed inventory page must seek by root and generation rather than scan history tables.",
    ));
    gates.push(ratio_gate(
        "captured_generation_inventory_work_stays_bounded_after_growth",
        "complexity",
        "logical_work_per_operation_ratio",
        inventory_after_growth.logical_work_per_operation,
        inventory_large.logical_work_per_operation,
        thresholds.history_work_growth_ratio,
        "ratio",
        "Reading a captured inventory page must not inspect generations published afterward.",
    ));
    gates.push(ratio_gate(
        "deep_catch_up_work_is_table_size_independent",
        "complexity",
        "logical_work_per_operation_ratio",
        catch_up_deep_large.logical_work_per_operation,
        catch_up_deep_small.logical_work_per_operation,
        thresholds.history_work_growth_ratio,
        "ratio",
        "A deep generation cursor must not scan unrelated canonical or history rows.",
    ));
    gates.push(ratio_gate(
        "deep_catch_up_work_is_comparable_to_early_page",
        "complexity",
        "logical_work_per_operation_ratio",
        catch_up_deep_large.logical_work_per_operation,
        catch_up_early.logical_work_per_operation,
        thresholds.history_work_growth_ratio,
        "ratio",
        "Equal catch-up pages must do comparable work regardless of cursor depth.",
    ));
    let traversal_ratio_limit = history_assertions.expected_traversal_pages as f64
        * thresholds.history_page_work_ratio_multiplier;
    gates.push(ratio_gate(
        "multi_page_generation_work_scales_with_pages",
        "complexity",
        "logical_work_per_operation_ratio",
        multi_page_traversal.logical_work_per_operation,
        catch_up_deep_large.logical_work_per_operation,
        traversal_ratio_limit,
        "ratio",
        "Draining one generation may scale with delivered pages, not total history-table rows.",
    ));

    let history_page_work_limit = profile.history_page_size as f64
        * thresholds.history_work_per_item
        + thresholds.history_fixed_work;
    for history_page in [
        inventory_small,
        inventory_large,
        inventory_after_growth,
        catch_up_early,
        catch_up_deep_small,
        catch_up_deep_large,
    ] {
        gates.push(max_gate(
            &format!("{}_bounded_work", history_page.name),
            "complexity",
            "logical_work_per_operation",
            history_page.logical_work_per_operation,
            history_page_work_limit,
            history_page_work_limit,
            "count/op",
            "History-page work is capped by the requested page plus fixed seek and validation overhead.",
        ));
    }
    let traversal_work_limit = history_assertions.expected_multi_page_publication_mutations as f64
        * thresholds.history_work_per_item
        + history_assertions.expected_traversal_pages as f64 * thresholds.history_fixed_work;
    gates.push(max_gate(
        "generation_catch_up_multi_page_traversal_bounded_work",
        "complexity",
        "logical_work_per_operation",
        multi_page_traversal.logical_work_per_operation,
        traversal_work_limit,
        traversal_work_limit,
        "count/op",
        "Whole-generation traversal is capped by delivered mutations plus fixed work per page.",
    ));

    Ok(gates)
}

fn scenario<'a>(scenarios: &'a [ScenarioEvidence], name: &str) -> Result<&'a ScenarioEvidence> {
    scenarios
        .iter()
        .find(|scenario| scenario.name == name)
        .ok_or_else(|| anyhow::anyhow!("missing benchmark scenario {name}"))
}

#[allow(clippy::too_many_arguments)]
fn max_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    configured_threshold: f64,
    effective_threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "<=".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold,
        effective_threshold,
        unit: unit.to_string(),
        pass: observed.is_finite() && observed <= effective_threshold,
        detail: detail.to_string(),
    }
}

fn exact_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    expected: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "==".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold: expected,
        effective_threshold: expected,
        unit: unit.to_string(),
        pass: observed.is_finite() && (observed - expected).abs() < f64::EPSILON,
        detail: detail.to_string(),
    }
}

fn min_gate(
    name: &str,
    category: &str,
    metric: &str,
    observed: f64,
    threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: ">=".to_string(),
        observed: observed.is_finite().then_some(observed),
        configured_threshold: threshold,
        effective_threshold: threshold,
        unit: unit.to_string(),
        pass: observed.is_finite() && observed >= threshold,
        detail: detail.to_string(),
    }
}

fn ratio_gate(
    name: &str,
    category: &str,
    metric: &str,
    numerator: f64,
    denominator: f64,
    threshold: f64,
    unit: &str,
    detail: &str,
) -> GateEvidence {
    let observed = (denominator > 0.0).then(|| numerator / denominator);
    GateEvidence {
        name: name.to_string(),
        category: category.to_string(),
        metric: metric.to_string(),
        expectation: "<=".to_string(),
        observed: observed.filter(|value| value.is_finite()),
        configured_threshold: threshold,
        effective_threshold: threshold,
        unit: unit.to_string(),
        pass: observed.is_some_and(|value| value.is_finite() && value <= threshold),
        detail: detail.to_string(),
    }
}
