use super::*;
use crate::query_bounds::replace_retained_bytes;

const QUERY_FILTER_CHUNK_ROWS: usize = 128;

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
    let mut live_probe = LatestLiveProbe::new();
    let mut hits = Vec::with_capacity(query.limit.min(128));
    let mut retained_bytes = 0usize;
    let driver = query_driver_ranges(&query.predicates)?;
    for (run, view) in runs.iter().zip(&views) {
        let mut pending = Vec::with_capacity(QUERY_FILTER_CHUNK_ROWS);
        if let Some(ranges) = &driver {
            let Some(root) = view.component_optional(KEYS_TAG) else {
                continue;
            };
            for range in ranges {
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
            )
            .await?;
        }
    }
    Ok(hits)
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
) -> Result<(), IndexError> {
    let rows = std::mem::take(pending);
    let predicates = query.predicates.clone();
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
                        Ok(_) => predicates
                            .iter()
                            .all(|predicate| row_accepts(&pending.row.payload.fields, predicate))
                            .then_some(Ok(pending.row)),
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
        return Ok(());
    }
    let mut retained = std::mem::take(hits);
    let mut retained_size = *retained_bytes;
    let after = after.cloned();
    let order = query.order.clone();
    let limit = query.limit;
    let merged = run
        .run_query_cpu(move || {
            for hit in candidates {
                if after.as_ref().is_some_and(|cursor| {
                    compare_hit_to_cursor(&hit, cursor, &order) != Ordering::Greater
                }) {
                    continue;
                }
                insert_bounded(&mut retained, &mut retained_size, hit, limit, &order)?;
            }
            Ok((retained, retained_size))
        })
        .await?;
    *hits = merged.0;
    *retained_bytes = merged.1;
    Ok(())
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

    fn planned_ranges(predicate: TypedPredicate) -> Vec<crate::compaction::KeyRange> {
        query_driver_ranges(&[predicate]).unwrap().unwrap()
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
