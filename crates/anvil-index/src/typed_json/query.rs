use super::*;
use crate::query_bounds::replace_retained_bytes;

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
    let mut hits = Vec::with_capacity(query.limit.min(128));
    let mut retained_bytes = 0usize;
    let driver = query_driver_prefix(&query.predicates)?;
    for (run, view) in runs.iter().zip(&views) {
        if let Some(prefix) = &driver {
            let Some(root) = view.component_optional(KEYS_TAG) else {
                continue;
            };
            let mut cursor = RoutedCursor::new(run, root.clone(), Some(prefix.clone()));
            while let Some(key) = cursor.next().await? {
                let row = typed_row(run, view, key.ordinal).await?;
                if !row.payload.matches_key(&key.primary, key.position)? {
                    return Err(IndexError::InvalidFormat("typed routed key mismatch"));
                }
                consider_typed_row(
                    runs,
                    &views,
                    run,
                    view,
                    row,
                    query,
                    after,
                    &mut hits,
                    &mut retained_bytes,
                )
                .await?;
            }
        } else {
            let Some(root) = view.component_optional(ROWS_TAG) else {
                continue;
            };
            let mut cursor = TypedCursor::new(run, root.clone());
            while let Some(row) = cursor.next().await? {
                consider_typed_row(
                    runs,
                    &views,
                    run,
                    view,
                    row,
                    query,
                    after,
                    &mut hits,
                    &mut retained_bytes,
                )
                .await?;
            }
        }
    }
    hits.sort_by(|left, right| compare_hits(left, right, &query.order));
    Ok(hits)
}

#[allow(clippy::too_many_arguments)]
async fn consider_typed_row<D: IndexDirectoryRead>(
    runs: &[D],
    views: &[RunView],
    run: &D,
    view: &RunView,
    row: TypedRow,
    query: &TypedQuery,
    after: Option<&TypedQueryCursor>,
    hits: &mut Vec<TypedHit>,
    retained_bytes: &mut usize,
) -> Result<(), IndexError> {
    if !query
        .predicates
        .iter()
        .all(|predicate| row_accepts(&row.payload.fields, predicate))
    {
        return Ok(());
    }
    let document = document_by_ordinal(run, view, row.ordinal).await?;
    if !is_latest_live(runs, views, &document).await? {
        return Ok(());
    }
    let hit = TypedHit {
        document,
        fields: row.payload.fields,
    };
    if after.is_some_and(|cursor| {
        compare_hit_to_cursor(&hit, cursor, &query.order) != Ordering::Greater
    }) {
        return Ok(());
    }
    insert_bounded(hits, retained_bytes, hit, query.limit, &query.order)
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
    use super::*;

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
}
