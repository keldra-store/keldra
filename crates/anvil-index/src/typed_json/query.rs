use super::*;
use crate::query_bounds::replace_retained_bytes;

const QUERY_FILTER_CHUNK_ROWS: usize = 128;
const ORDER_SCAN_CANDIDATES_PER_RESULT: usize = 32;
const MIN_ORDER_SCAN_CANDIDATES: usize = 256;

struct DriverPlan {
    predicate_index: usize,
    ranges: Vec<crate::compaction::KeyRange>,
    estimated_rows: u64,
}

struct OrderedPlan {
    field: String,
    scalar_tag: u8,
    descending: bool,
}

struct PendingTypedRow {
    row: TypedRow,
    routed_key: Option<(Vec<u8>, u32)>,
}

pub(super) async fn query_typed<D: IndexDirectoryRead>(
    runs: &[D],
    definition: &TypedJsonDefinition,
    query: &TypedQuery,
    kind: IndexKind,
    after: Option<&TypedQueryCursor>,
) -> Result<Vec<TypedHit>, IndexError> {
    validate_query(definition, query)?;
    validate_typed_cursor(after, query)?;
    if query.limit == 0 || runs.is_empty() {
        return Ok(Vec::new());
    }
    let views = open_views(runs, kind).await?;
    let driver = select_driver(runs, &views, &query.predicates).await?;
    if driver
        .as_ref()
        .is_some_and(|driver| driver.estimated_rows == 0)
    {
        return Ok(Vec::new());
    }
    if let Some(plan) = ordered_plan(runs, &views, query, after).await?
        && let Some(hits) = query_typed_ordered(
            runs,
            &views,
            query,
            after,
            &plan,
            driver.as_ref().map(|driver| driver.estimated_rows),
        )
        .await?
    {
        return Ok(hits);
    }
    query_typed_from_driver(runs, &views, query, after, driver.as_ref()).await
}

async fn query_typed_from_driver<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    query: &TypedQuery,
    after: Option<&TypedQueryCursor>,
    driver: Option<&DriverPlan>,
) -> Result<Vec<TypedHit>, IndexError> {
    let mut live_probe = LatestLiveProbe::new();
    let mut hits = Vec::with_capacity(query.limit.min(128));
    let mut retained_bytes = 0usize;
    for (run, view) in runs.iter().zip(views) {
        let mut pending = Vec::with_capacity(QUERY_FILTER_CHUNK_ROWS);
        if let Some(driver) = driver {
            let Some(root) = view.component_optional(KEYS_TAG) else {
                continue;
            };
            for range in &driver.ranges {
                let mut cursor = PostingCursor::in_range(run, root.clone(), range.clone());
                while let Some(key) = cursor.next().await? {
                    let row = typed_row(run, view, key.ordinal).await?;
                    pending.push(PendingTypedRow {
                        row,
                        routed_key: Some((key.primary, key.position)),
                    });
                    if pending.len() == QUERY_FILTER_CHUNK_ROWS {
                        process_typed_batch(
                            runs,
                            &views,
                            run,
                            view,
                            &mut pending,
                            query,
                            after,
                            &mut live_probe,
                            &mut hits,
                            &mut retained_bytes,
                            None,
                        )
                        .await?;
                    }
                }
            }
        } else {
            let Some(root) = view.component_optional(ROWS_TAG) else {
                continue;
            };
            let mut cursor = TypedCursor::new(run, root.clone());
            while let Some(row) = cursor.next().await? {
                pending.push(PendingTypedRow {
                    row,
                    routed_key: None,
                });
                if pending.len() == QUERY_FILTER_CHUNK_ROWS {
                    process_typed_batch(
                        runs,
                        &views,
                        run,
                        view,
                        &mut pending,
                        query,
                        after,
                        &mut live_probe,
                        &mut hits,
                        &mut retained_bytes,
                        None,
                    )
                    .await?;
                }
            }
        }
        if !pending.is_empty() {
            process_typed_batch(
                runs,
                &views,
                run,
                view,
                &mut pending,
                query,
                after,
                &mut live_probe,
                &mut hits,
                &mut retained_bytes,
                None,
            )
            .await?;
        }
    }
    Ok(hits)
}

async fn select_driver<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    predicates: &[TypedPredicate],
) -> Result<Option<DriverPlan>, IndexError> {
    let mut selected = None::<DriverPlan>;
    for (predicate_index, predicate) in predicates.iter().enumerate() {
        let ranges = predicate_ranges(predicate)?;
        let mut estimated_rows = 0u64;
        for (run, view) in runs.iter().zip(views) {
            let Some(root) = view.component_optional(KEYS_TAG) else {
                continue;
            };
            estimated_rows = estimated_rows
                .checked_add(estimate_posting_ranges(run, root.clone(), &ranges).await?)
                .ok_or(IndexError::OffsetOverflow)?;
        }
        if selected.as_ref().is_none_or(|current| {
            (estimated_rows, predicate_index) < (current.estimated_rows, current.predicate_index)
        }) {
            selected = Some(DriverPlan {
                predicate_index,
                ranges,
                estimated_rows,
            });
        }
    }
    Ok(selected)
}

async fn ordered_plan<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    query: &TypedQuery,
    after: Option<&TypedQueryCursor>,
) -> Result<Option<OrderedPlan>, IndexError> {
    let Some(order) = query.order.first() else {
        return Ok(None);
    };
    if after.is_some_and(|cursor| cursor.values.first().is_none_or(Option::is_none)) {
        return Ok(None);
    }

    let mut selected_tag = None;
    for (run, view) in runs.iter().zip(views) {
        let Some(rows) = view.component_optional(ROWS_TAG) else {
            continue;
        };
        let Some(keys) = view.component_optional(KEYS_TAG) else {
            return Ok(None);
        };
        let exists = count_posting_range(
            run,
            keys.clone(),
            query_prefix_range(typed_exists_primary(&order.field)?),
        )
        .await?;
        if exists != rows.element_count {
            return Ok(None);
        }
        for tag in [0, 1, 2, 3, UNSIGNED_VALUE_TAG] {
            let count = count_posting_range(
                run,
                keys.clone(),
                query_prefix_range(typed_scalar_tag_prefix(&order.field, tag)?),
            )
            .await?;
            if count == 0 {
                continue;
            }
            match selected_tag {
                None => selected_tag = Some(tag),
                Some(selected) if selected == tag => {}
                Some(_) => return Ok(None),
            }
        }
    }
    let Some(scalar_tag) = selected_tag else {
        return Ok(None);
    };
    if after
        .and_then(|cursor| cursor.values.first())
        .and_then(Option::as_ref)
        .is_some_and(|value| scalar_tag_for_value(value) != scalar_tag)
    {
        return Ok(None);
    }
    Ok(Some(OrderedPlan {
        field: order.field.clone(),
        scalar_tag,
        descending: order.descending,
    }))
}

async fn query_typed_ordered<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    query: &TypedQuery,
    after: Option<&TypedQueryCursor>,
    plan: &OrderedPlan,
    driver_estimate: Option<u64>,
) -> Result<Option<Vec<TypedHit>>, IndexError> {
    let base_cap = query
        .limit
        .saturating_mul(ORDER_SCAN_CANDIDATES_PER_RESULT)
        .max(MIN_ORDER_SCAN_CANDIDATES);
    let scan_cap = driver_estimate.map_or(base_cap, |estimate| {
        base_cap.min(usize::try_from(estimate).unwrap_or(usize::MAX))
    });
    let range = ordered_range(plan, after)?;
    let mut live_probe = LatestLiveProbe::new();
    let mut hits = Vec::with_capacity(query.limit.min(128));
    let mut retained_bytes = 0usize;

    for (run, view) in runs.iter().zip(views) {
        let Some(root) = view.component_optional(KEYS_TAG) else {
            continue;
        };
        let mut cursor = if plan.descending {
            PostingCursor::in_range_reverse(run, root.clone(), range.clone())
        } else {
            PostingCursor::in_range(run, root.clone(), range.clone())
        };
        let mut run_matches = 0usize;
        let mut run_scanned = 0usize;
        let mut pending = Vec::with_capacity(QUERY_FILTER_CHUNK_ROWS);
        let mut current_primary = None::<Vec<u8>>;
        while let Some(key) = cursor.next().await? {
            if current_primary
                .as_ref()
                .is_some_and(|primary| primary != &key.primary)
            {
                if !pending.is_empty() {
                    run_matches = run_matches.saturating_add(
                        process_typed_batch(
                            runs,
                            views,
                            run,
                            view,
                            &mut pending,
                            query,
                            after,
                            &mut live_probe,
                            &mut hits,
                            &mut retained_bytes,
                            Some(&plan.field),
                        )
                        .await?,
                    );
                }
                if run_matches >= query.limit {
                    break;
                }
                current_primary = Some(key.primary.clone());
            } else if current_primary.is_none() {
                current_primary = Some(key.primary.clone());
            }

            run_scanned = run_scanned.saturating_add(1);
            if run_scanned > scan_cap {
                return Ok(None);
            }
            let row = typed_row(run, view, key.ordinal).await?;
            pending.push(PendingTypedRow {
                row,
                routed_key: Some((key.primary, key.position)),
            });
            if pending.len() == QUERY_FILTER_CHUNK_ROWS {
                run_matches = run_matches.saturating_add(
                    process_typed_batch(
                        runs,
                        views,
                        run,
                        view,
                        &mut pending,
                        query,
                        after,
                        &mut live_probe,
                        &mut hits,
                        &mut retained_bytes,
                        Some(&plan.field),
                    )
                    .await?,
                );
            }
        }
        if !pending.is_empty() {
            let _ = process_typed_batch(
                runs,
                views,
                run,
                view,
                &mut pending,
                query,
                after,
                &mut live_probe,
                &mut hits,
                &mut retained_bytes,
                Some(&plan.field),
            )
            .await?;
        }
    }
    Ok(Some(hits))
}

fn ordered_range(
    plan: &OrderedPlan,
    after: Option<&TypedQueryCursor>,
) -> Result<crate::compaction::KeyRange, IndexError> {
    let type_prefix = typed_scalar_tag_prefix(&plan.field, plan.scalar_tag)?;
    let type_upper = crate::routed::prefix_successor(&type_prefix);
    let Some(value) = after
        .and_then(|cursor| cursor.values.first())
        .and_then(Option::as_ref)
    else {
        return Ok(crate::compaction::KeyRange {
            lower: Some(type_prefix),
            upper: type_upper,
        });
    };
    let primary = typed_query_primary(&plan.field, value)?;
    Ok(if plan.descending {
        crate::compaction::KeyRange {
            lower: Some(type_prefix),
            upper: crate::routed::prefix_successor(&primary),
        }
    } else {
        crate::compaction::KeyRange {
            lower: Some(primary),
            upper: type_upper,
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn process_typed_batch<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    run: &D,
    view: &RunView,
    pending: &mut Vec<PendingTypedRow>,
    query: &TypedQuery,
    after: Option<&TypedQueryCursor>,
    live_probe: &mut LatestLiveProbe,
    hits: &mut Vec<TypedHit>,
    retained_bytes: &mut usize,
    first_order_field: Option<&str>,
) -> Result<usize, IndexError> {
    let rows = std::mem::take(pending);
    let predicates = query.predicates.clone();
    let first_order_field = first_order_field.map(str::to_owned);
    let rows = run
        .run_query_cpu(move || {
            rows.into_iter()
                .filter_map(|pending| {
                    let routed_matches = pending
                        .routed_key
                        .as_ref()
                        .map(|(primary, position)| {
                            pending.row.payload.matches_key(primary, *position)
                        })
                        .transpose();
                    match routed_matches {
                        Err(error) => Some(Err(error)),
                        Ok(Some(false)) => {
                            Some(Err(IndexError::InvalidFormat("typed routed key mismatch")))
                        }
                        Ok(_) => {
                            let first_order_matches =
                                match (first_order_field.as_deref(), pending.routed_key.as_ref()) {
                                    (Some(field), Some((primary, _))) => pending
                                        .row
                                        .payload
                                        .fields
                                        .get(field)
                                        .and_then(|values| values.first())
                                        .map(|value| typed_primary(field, value))
                                        .transpose()
                                        .map(|first| first.as_ref() == Some(primary)),
                                    (Some(_), None) => Ok(false),
                                    (None, _) => Ok(true),
                                };
                            match first_order_matches {
                                Err(error) => Some(Err(error)),
                                Ok(false) => None,
                                Ok(true) => predicates
                                    .iter()
                                    .all(|predicate| {
                                        row_accepts(&pending.row.payload.fields, predicate)
                                    })
                                    .then_some(Ok(pending.row)),
                            }
                        }
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await?;
    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        let document = document_by_ordinal(run, view, row.ordinal).await?;
        if live_probe.is_latest_live(runs, views, &document).await? {
            candidates.push(TypedHit {
                document,
                fields: row.payload.fields,
            });
        }
    }
    if candidates.is_empty() {
        return Ok(0);
    }
    let mut retained = std::mem::take(hits);
    let mut retained_size = *retained_bytes;
    let after = after.cloned();
    let order = query.order.clone();
    let limit = query.limit;
    let merged = run
        .run_query_cpu(move || {
            let mut accepted = 0usize;
            for hit in candidates {
                if after.as_ref().is_some_and(|cursor| {
                    compare_hit_to_cursor(&hit, cursor, &order) != Ordering::Greater
                }) {
                    continue;
                }
                accepted = accepted.saturating_add(1);
                insert_bounded(&mut retained, &mut retained_size, hit, limit, &order)?;
            }
            Ok((retained, retained_size, accepted))
        })
        .await?;
    *hits = merged.0;
    *retained_bytes = merged.1;
    Ok(merged.2)
}

fn insert_bounded(
    hits: &mut Vec<TypedHit>,
    retained_bytes: &mut usize,
    hit: TypedHit,
    limit: usize,
    order: &[TypedOrder],
) -> Result<(), IndexError> {
    if hits
        .iter()
        .any(|existing| existing.document == hit.document)
    {
        return Ok(());
    }
    let added = typed_hit_resident_bytes(&hit);
    hits.push(hit);
    hits.sort_by(|left, right| compare_hits(left, right, order));
    let removed = (hits.len() > limit)
        .then(|| hits.pop().map_or(0, |hit| typed_hit_resident_bytes(&hit)))
        .unwrap_or(0);
    *retained_bytes = replace_retained_bytes(*retained_bytes, added, removed)?;
    Ok(())
}

fn typed_hit_resident_bytes(hit: &TypedHit) -> usize {
    std::mem::size_of::<TypedHit>()
        .saturating_add(hit.document.path.len())
        .saturating_add(estimate_selected_fields(&hit.fields))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    use crate::io::tests::{MemoryBlockSink, MemoryDirectory};

    use super::*;

    #[derive(Clone)]
    struct CountingDirectory {
        inner: MemoryDirectory,
        posting_leaf_opens: Arc<AtomicUsize>,
    }

    impl CountingDirectory {
        fn new(inner: MemoryDirectory) -> Self {
            Self {
                inner,
                posting_leaf_opens: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn posting_leaf_opens(&self) -> usize {
            self.posting_leaf_opens.load(AtomicOrdering::Relaxed)
        }

        fn reset(&self) {
            self.posting_leaf_opens.store(0, AtomicOrdering::Relaxed);
        }
    }

    impl IndexDirectoryRead for CountingDirectory {
        type File = <MemoryDirectory as IndexDirectoryRead>::File;

        async fn open_root(&self) -> Result<Self::File, IndexError> {
            self.inner.open_root().await
        }

        async fn open_block(
            &self,
            descriptor: &crate::BlockDescriptor,
        ) -> Result<Self::File, IndexError> {
            if descriptor.component_tag == KEYS_TAG && descriptor.routing_height == 0 {
                self.posting_leaf_opens
                    .fetch_add(1, AtomicOrdering::Relaxed);
            }
            self.inner.open_block(descriptor).await
        }
    }

    fn hit(path: &str, value: &str) -> TypedHit {
        TypedHit {
            document: DocumentRef {
                path: path.into(),
                version: 1,
            },
            fields: BTreeMap::from([("value".into(), vec![ScalarValue::String(value.into())])]),
        }
    }

    #[test]
    fn top_k_accounting_ignores_duplicates_and_releases_evictions() {
        let mut hits = Vec::new();
        let mut retained = 0;
        let original = hit("/z", &"x".repeat(100));
        insert_bounded(&mut hits, &mut retained, original.clone(), 1, &[]).unwrap();
        let original_bytes = retained;
        insert_bounded(&mut hits, &mut retained, original, 1, &[]).unwrap();
        assert_eq!(retained, original_bytes);
        assert_eq!(hits.len(), 1);

        let replacement = hit("/a", "small");
        let replacement_bytes = typed_hit_resident_bytes(&replacement);
        insert_bounded(&mut hits, &mut retained, replacement, 1, &[]).unwrap();
        assert_eq!(retained, replacement_bytes);
        assert_eq!(hits[0].document.path, "/a");
    }

    fn query_definition() -> TypedJsonDefinition {
        TypedJsonDefinition {
            fields: vec![
                TypedField {
                    name: "status".into(),
                    json_pointer: "/status".into(),
                },
                TypedField {
                    name: "amount".into(),
                    json_pointer: "/amount".into(),
                },
            ],
        }
    }

    async fn large_counted_run() -> (CountingDirectory, usize) {
        let mut builder = TypedJsonSegmentBuilder::new(
            query_definition(),
            SegmentBuildOptions::for_level(4 * 1024 * 1024, 1).unwrap(),
        )
        .unwrap();
        for index in 0..256u64 {
            assert!(matches!(
                builder
                    .try_push(IndexMutation::Upsert(TypedJsonDocument {
                        document: DocumentRef {
                            path: format!("/documents/{index:04}"),
                            version: 1,
                        },
                        fields: BTreeMap::from([
                            (
                                "status".into(),
                                vec![ScalarValue::String(format!("value-{index:04}"))],
                            ),
                            ("amount".into(), vec![ScalarValue::Number(index as f64)]),
                        ]),
                    }))
                    .unwrap(),
                SegmentPush::Accepted
            ));
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, 256)
            .await
            .unwrap()
            .unwrap();
        let directory = CountingDirectory::new(sink.directory_with_root(run.into_root()));
        let view = open_run(&directory, IndexKind::TypedJson).await.unwrap();
        let root = view.component(KEYS_TAG).unwrap().clone();
        directory.reset();
        let mut full = PostingCursor::new(&directory, root, None);
        while full.next().await.unwrap().is_some() {}
        let all_posting_leaves = directory.posting_leaf_opens();
        assert!(all_posting_leaves > 8);
        directory.reset();
        (directory, all_posting_leaves)
    }

    async fn assert_query_uses_fewer_posting_leaves(
        directory: &CountingDirectory,
        all_posting_leaves: usize,
        predicate: TypedPredicate,
        expected_hits: usize,
    ) {
        directory.reset();
        let hits = TypedJsonEngine::query(
            std::slice::from_ref(directory),
            &query_definition(),
            &TypedQuery {
                predicates: vec![predicate],
                order: Vec::new(),
                limit: 512,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), expected_hits);
        let opened = directory.posting_leaf_opens();
        assert!(opened > 0);
        assert!(
            opened < all_posting_leaves,
            "opened {opened}/{all_posting_leaves}"
        );
    }

    #[tokio::test]
    async fn set_range_and_exists_queries_skip_unrelated_posting_leaves() {
        let (directory, all_posting_leaves) = large_counted_run().await;
        assert_query_uses_fewer_posting_leaves(
            &directory,
            all_posting_leaves,
            TypedPredicate::In {
                field: "status".into(),
                values: vec![
                    ScalarValue::String("value-0001".into()),
                    ScalarValue::String("value-0254".into()),
                ],
            },
            2,
        )
        .await;
        assert_query_uses_fewer_posting_leaves(
            &directory,
            all_posting_leaves,
            TypedPredicate::LessThan {
                field: "amount".into(),
                value: ScalarValue::Number(4.0),
            },
            4,
        )
        .await;
        assert_query_uses_fewer_posting_leaves(
            &directory,
            all_posting_leaves,
            TypedPredicate::Exists {
                field: "status".into(),
            },
            256,
        )
        .await;
    }

    fn ordered_filter_definition() -> TypedJsonDefinition {
        TypedJsonDefinition {
            fields: [
                "broad_flag",
                "lifecycle",
                "excluded",
                "group",
                "rank",
                "tie_id",
            ]
            .into_iter()
            .map(|name| TypedField {
                name: name.into(),
                json_pointer: format!("/{name}"),
            })
            .collect(),
        }
    }

    fn ordered_filter_fields(index: u64, lifecycle: &str, group: &str) -> SelectedScalarFields {
        BTreeMap::from([
            ("broad_flag".into(), vec![ScalarValue::Boolean(true)]),
            (
                "lifecycle".into(),
                vec![ScalarValue::String(lifecycle.into())],
            ),
            ("excluded".into(), vec![ScalarValue::Boolean(false)]),
            ("group".into(), vec![ScalarValue::String(group.into())]),
            ("rank".into(), vec![ScalarValue::Number(index as f64)]),
            (
                "tie_id".into(),
                vec![ScalarValue::String(format!("item-{index:06}"))],
            ),
        ])
    }

    async fn ordered_filter_run(count: u64) -> CountingDirectory {
        let mut builder = TypedJsonSegmentBuilder::new(
            ordered_filter_definition(),
            SegmentBuildOptions::for_level(16 * 1024 * 1024, 1).unwrap(),
        )
        .unwrap();
        for index in 0..count {
            let group = match index % 4 {
                0 => "alpha",
                1 => "beta",
                2 => "gamma",
                _ => "other",
            };
            builder
                .try_push(IndexMutation::Upsert(TypedJsonDocument {
                    document: DocumentRef {
                        path: format!("/documents/{index:06}"),
                        version: 1,
                    },
                    fields: ordered_filter_fields(index, "enabled", group),
                }))
                .unwrap();
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, 1024)
            .await
            .unwrap()
            .unwrap();
        CountingDirectory::new(sink.directory_with_root(run.into_root()))
    }

    fn ordered_filter_query(limit: usize) -> TypedQuery {
        TypedQuery {
            predicates: vec![
                TypedPredicate::Equal {
                    field: "broad_flag".into(),
                    value: ScalarValue::Boolean(true),
                },
                TypedPredicate::Equal {
                    field: "lifecycle".into(),
                    value: ScalarValue::String("enabled".into()),
                },
                TypedPredicate::Equal {
                    field: "excluded".into(),
                    value: ScalarValue::Boolean(false),
                },
                TypedPredicate::In {
                    field: "group".into(),
                    values: ["alpha", "beta", "gamma"]
                        .into_iter()
                        .map(|value| ScalarValue::String(value.into()))
                        .collect(),
                },
            ],
            order: vec![
                TypedOrder {
                    field: "rank".into(),
                    descending: true,
                },
                TypedOrder {
                    field: "tie_id".into(),
                    descending: false,
                },
            ],
            limit,
        }
    }

    #[tokio::test]
    async fn planner_chooses_a_selective_later_predicate_over_the_first_equality() {
        let directory = ordered_filter_run(512).await;
        let views = open_views(std::slice::from_ref(&directory), IndexKind::TypedJson)
            .await
            .unwrap();
        let query = TypedQuery {
            predicates: vec![
                TypedPredicate::Equal {
                    field: "broad_flag".into(),
                    value: ScalarValue::Boolean(true),
                },
                TypedPredicate::Equal {
                    field: "tie_id".into(),
                    value: ScalarValue::String("item-000007".into()),
                },
            ],
            order: Vec::new(),
            limit: 4,
        };
        let plan = select_driver(std::slice::from_ref(&directory), &views, &query.predicates)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.predicate_index, 1);
    }

    #[tokio::test]
    async fn ordered_postings_bound_the_ordered_query_and_seek_the_second_page() {
        let directory = ordered_filter_run(2_048).await;
        let definition = ordered_filter_definition();
        let query = ordered_filter_query(4);

        let view = open_run(&directory, IndexKind::TypedJson).await.unwrap();
        let root = view.component(KEYS_TAG).unwrap().clone();
        directory.reset();
        let mut full = PostingCursor::new(&directory, root, None);
        while full.next().await.unwrap().is_some() {}
        let complete_posting_leaf_reads = directory.posting_leaf_opens();
        assert!(complete_posting_leaf_reads > 32);

        directory.reset();
        let first = TypedJsonEngine::query_after(
            std::slice::from_ref(&directory),
            &definition,
            &query,
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|hit| hit.document.path.as_str())
                .collect::<Vec<_>>(),
            [
                "/documents/002046",
                "/documents/002045",
                "/documents/002044",
                "/documents/002042",
            ]
        );
        assert!(directory.posting_leaf_opens() * 4 < complete_posting_leaf_reads);

        let cursor = TypedQueryCursor::from_hit(first.last().unwrap(), &query.order);
        directory.reset();
        let second = TypedJsonEngine::query_after(
            std::slice::from_ref(&directory),
            &definition,
            &query,
            Some(&cursor),
        )
        .await
        .unwrap();
        assert_eq!(
            second
                .iter()
                .map(|hit| hit.document.path.as_str())
                .collect::<Vec<_>>(),
            [
                "/documents/002041",
                "/documents/002040",
                "/documents/002038",
                "/documents/002037",
            ]
        );
        assert!(directory.posting_leaf_opens() * 4 < complete_posting_leaf_reads);
    }

    async fn mutation_run(mutations: Vec<IndexMutation<TypedJsonDocument>>) -> MemoryDirectory {
        let mut builder = TypedJsonSegmentBuilder::new(
            ordered_filter_definition(),
            SegmentBuildOptions::for_level(4 * 1024 * 1024, 1).unwrap(),
        )
        .unwrap();
        for mutation in mutations {
            builder.try_push(mutation).unwrap();
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder
            .seal_with_target(&mut sink, 512)
            .await
            .unwrap()
            .unwrap();
        sink.directory_with_root(run.into_root())
    }

    #[tokio::test]
    async fn ordered_query_preserves_newest_run_shadowing_and_tombstones() {
        let document = |path: &str, version: u64, modified: u64, lifecycle: &str| {
            IndexMutation::Upsert(TypedJsonDocument {
                document: DocumentRef {
                    path: path.into(),
                    version,
                },
                fields: ordered_filter_fields(modified, lifecycle, "alpha"),
            })
        };
        let older = mutation_run(vec![
            document("/documents/a", 1, 1, "enabled"),
            document("/documents/b", 1, 2, "enabled"),
            document("/documents/c", 1, 3, "enabled"),
            document("/documents/d", 1, 4, "enabled"),
        ])
        .await;
        let newer = mutation_run(vec![
            document("/documents/a", 2, 10, "disabled"),
            IndexMutation::Remove(DocumentRef {
                path: "/documents/c".into(),
                version: 2,
            }),
            // An exact duplicate cannot appear twice in the merged page even
            // when it is retained by two immutable runs.
            document("/documents/d", 1, 4, "enabled"),
        ])
        .await;
        let hits = TypedJsonEngine::query(
            &[newer, older],
            &ordered_filter_definition(),
            &TypedQuery {
                predicates: vec![TypedPredicate::Equal {
                    field: "lifecycle".into(),
                    value: ScalarValue::String("enabled".into()),
                }],
                order: vec![TypedOrder {
                    field: "rank".into(),
                    descending: true,
                }],
                limit: 2,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].document.path, "/documents/d");
        assert_eq!(hits[1].document.path, "/documents/b");
    }

    #[tokio::test]
    async fn ordered_arrays_use_the_first_canonical_value_in_both_directions() {
        let mut builder = TypedJsonSegmentBuilder::new(
            ordered_filter_definition(),
            SegmentBuildOptions::for_level(4 * 1024 * 1024, 1).unwrap(),
        )
        .unwrap();
        for (path, source, modified) in [
            ("a", "a", vec![1.0, 100.0]),
            ("b", "b", vec![2.0]),
            ("c", "c", vec![3.0]),
            ("d", "d", vec![1.0]),
        ] {
            let mut fields = ordered_filter_fields(0, "enabled", "alpha");
            fields.insert(
                "rank".into(),
                modified.into_iter().map(ScalarValue::Number).collect(),
            );
            fields.insert("tie_id".into(), vec![ScalarValue::String(source.into())]);
            builder
                .try_push(IndexMutation::Upsert(TypedJsonDocument {
                    document: DocumentRef {
                        path: format!("/documents/{path}"),
                        version: 1,
                    },
                    fields,
                }))
                .unwrap();
        }
        let mut sink = MemoryBlockSink::default();
        let run = builder.seal(&mut sink).await.unwrap().unwrap();
        let directory = sink.directory_with_root(run.into_root());
        for (descending, expected) in [(true, ["c", "b", "a", "d"]), (false, ["a", "d", "b", "c"])]
        {
            let hits = TypedJsonEngine::query(
                std::slice::from_ref(&directory),
                &ordered_filter_definition(),
                &TypedQuery {
                    predicates: Vec::new(),
                    order: vec![
                        TypedOrder {
                            field: "rank".into(),
                            descending,
                        },
                        TypedOrder {
                            field: "tie_id".into(),
                            descending: false,
                        },
                    ],
                    limit: 10,
                },
            )
            .await
            .unwrap();
            assert_eq!(
                hits.iter()
                    .map(|hit| hit.document.path.rsplit('/').next().unwrap())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn ordered_cursor_resumes_inside_a_tied_primary_group() {
        let documents = [
            ("a", 1, "a"),
            ("b", 1, "b"),
            ("c", 1, "c"),
            ("d", 1, "d"),
            ("e", 2, "e"),
            ("f", 0, "f"),
        ]
        .into_iter()
        .map(|(path, modified, source)| {
            let mut fields = ordered_filter_fields(modified, "enabled", "alpha");
            fields.insert("tie_id".into(), vec![ScalarValue::String(source.into())]);
            IndexMutation::Upsert(TypedJsonDocument {
                document: DocumentRef {
                    path: format!("/documents/{path}"),
                    version: 1,
                },
                fields,
            })
        })
        .collect();
        let directory = mutation_run(documents).await;

        for (descending, first_paths, second_paths) in [
            (false, ["f", "a"], ["b", "c"]),
            (true, ["e", "a"], ["b", "c"]),
        ] {
            let query = TypedQuery {
                predicates: vec![TypedPredicate::Equal {
                    field: "lifecycle".into(),
                    value: ScalarValue::String("enabled".into()),
                }],
                order: vec![
                    TypedOrder {
                        field: "rank".into(),
                        descending,
                    },
                    TypedOrder {
                        field: "tie_id".into(),
                        descending: false,
                    },
                ],
                limit: 2,
            };
            let first = TypedJsonEngine::query_after(
                std::slice::from_ref(&directory),
                &ordered_filter_definition(),
                &query,
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                first
                    .iter()
                    .map(|hit| hit.document.path.rsplit('/').next().unwrap())
                    .collect::<Vec<_>>(),
                first_paths
            );

            let cursor = TypedQueryCursor::from_hit(first.last().unwrap(), &query.order);
            let second = TypedJsonEngine::query_after(
                std::slice::from_ref(&directory),
                &ordered_filter_definition(),
                &query,
                Some(&cursor),
            )
            .await
            .unwrap();
            assert_eq!(
                second
                    .iter()
                    .map(|hit| hit.document.path.rsplit('/').next().unwrap())
                    .collect::<Vec<_>>(),
                second_paths
            );
        }
    }

    #[tokio::test]
    async fn ordered_plan_falls_back_for_missing_or_mixed_order_values() {
        let definition = TypedJsonDefinition {
            fields: vec![
                TypedField {
                    name: "state".into(),
                    json_pointer: "/state".into(),
                },
                TypedField {
                    name: "rank".into(),
                    json_pointer: "/rank".into(),
                },
            ],
        };
        let make_run = |fields: Vec<SelectedScalarFields>| {
            let definition = definition.clone();
            async move {
                let mut builder = TypedJsonSegmentBuilder::new(
                    definition,
                    SegmentBuildOptions::for_level(4 * 1024 * 1024, 1).unwrap(),
                )
                .unwrap();
                for (index, fields) in fields.into_iter().enumerate() {
                    builder
                        .try_push(IndexMutation::Upsert(TypedJsonDocument {
                            document: DocumentRef {
                                path: format!("/documents/{index}"),
                                version: 1,
                            },
                            fields,
                        }))
                        .unwrap();
                }
                let mut sink = MemoryBlockSink::default();
                let run = builder.seal(&mut sink).await.unwrap().unwrap();
                sink.directory_with_root(run.into_root())
            }
        };
        let query = TypedQuery {
            predicates: Vec::new(),
            order: vec![TypedOrder {
                field: "rank".into(),
                descending: false,
            }],
            limit: 4,
        };
        let missing = make_run(vec![
            BTreeMap::from([
                ("state".into(), vec![ScalarValue::String("active".into())]),
                ("rank".into(), vec![ScalarValue::Number(1.0)]),
            ]),
            BTreeMap::from([("state".into(), vec![ScalarValue::String("active".into())])]),
        ])
        .await;
        let views = open_views(std::slice::from_ref(&missing), IndexKind::TypedJson)
            .await
            .unwrap();
        assert!(
            ordered_plan(std::slice::from_ref(&missing), &views, &query, None)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            TypedJsonEngine::query(std::slice::from_ref(&missing), &definition, &query)
                .await
                .unwrap()
                .len(),
            2
        );

        let mixed = make_run(vec![
            BTreeMap::from([
                ("state".into(), vec![ScalarValue::String("active".into())]),
                ("rank".into(), vec![ScalarValue::Number(1.0)]),
            ]),
            BTreeMap::from([
                ("state".into(), vec![ScalarValue::String("active".into())]),
                ("rank".into(), vec![ScalarValue::String("two".into())]),
            ]),
        ])
        .await;
        let views = open_views(std::slice::from_ref(&mixed), IndexKind::TypedJson)
            .await
            .unwrap();
        assert!(
            ordered_plan(std::slice::from_ref(&mixed), &views, &query, None)
                .await
                .unwrap()
                .is_none()
        );
    }

    fn planned_ranges(predicate: TypedPredicate) -> Vec<crate::compaction::KeyRange> {
        predicate_ranges(&predicate).unwrap()
    }

    #[test]
    fn planner_uses_disjoint_exact_set_and_typed_inequality_ranges() {
        let number = |value| typed_primary("amount", &ScalarValue::Number(value)).unwrap();
        let string =
            |value: &str| typed_primary("amount", &ScalarValue::String(value.to_owned())).unwrap();
        let set = planned_ranges(TypedPredicate::In {
            field: "amount".into(),
            values: vec![
                ScalarValue::String("selected".into()),
                ScalarValue::Number(7.0),
                ScalarValue::Number(7.0),
            ],
        });
        assert_eq!(set.len(), 2);
        assert!(set.iter().any(|range| range.contains(&number(7.0))));
        assert!(set.iter().any(|range| range.contains(&string("selected"))));
        assert!(!set.iter().any(|range| range.contains(&number(8.0))));

        let less = planned_ranges(TypedPredicate::LessThan {
            field: "amount".into(),
            value: ScalarValue::Number(7.0),
        });
        assert!(less[0].contains(&number(6.0)));
        assert!(!less[0].contains(&number(7.0)));
        assert!(!less[0].contains(&string("6")));

        let greater_or_equal = planned_ranges(TypedPredicate::GreaterThanOrEqual {
            field: "amount".into(),
            value: ScalarValue::Number(7.0),
        });
        assert!(greater_or_equal[0].contains(&number(7.0)));
        assert!(greater_or_equal[0].contains(&number(8.0)));
        assert!(!greater_or_equal[0].contains(&string("8")));

        let exists = planned_ranges(TypedPredicate::Exists {
            field: "amount".into(),
        });
        assert!(exists[0].contains(&typed_exists_primary("amount").unwrap()));
        assert!(!exists[0].contains(&number(7.0)));
        assert!(
            planned_ranges(TypedPredicate::LessThan {
                field: "amount".into(),
                value: ScalarValue::Null,
            })
            .is_empty()
        );
    }
}
