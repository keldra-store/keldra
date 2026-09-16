use super::super::super::{QueryMemoryPermit, SegmentDocumentId};
use super::*;

struct Permit;
impl QueryMemoryPermit for Permit {
    fn admitted_bytes(&self) -> usize {
        1024 * 1024
    }
}

fn key(id: u8) -> StableDocumentKey {
    StableDocumentKey::from_bytes([id; 32]).unwrap()
}

fn table(entries: &[(u8, u64)]) -> Arc<SegmentDocumentTable> {
    Arc::new(
        SegmentDocumentTable::new_with_versions(
            entries.iter().map(|(id, version)| (key(*id), *version)),
        )
        .unwrap(),
    )
}

fn posting(documents: Arc<SegmentDocumentTable>, key_id: u8) -> DenseQueryPosting {
    let document = documents.id(key(key_id)).unwrap();
    DenseQueryPosting {
        posting: DenseSegmentPosting {
            document,
            material_source_version: documents.material_version(document).unwrap(),
            live: true,
            position_block_hash: None,
            positions: 0,
        },
        documents,
    }
}

fn candidates(
    postings: &[DenseQueryPosting],
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> DenseCandidateSet {
    budget
        .reserve_heap(credits, candidate_set_bytes(postings.len()).unwrap())
        .unwrap();
    DenseCandidateSet {
        entries: postings
            .iter()
            .map(|posting| {
                (
                    posting.document().unwrap(),
                    QueryMaterialCandidate::from(posting.clone()),
                )
            })
            .collect(),
    }
}

#[test]
fn dense_boolean_intersection_joins_exact_versions_across_shared_recipe_tables() {
    let common = table(&[(1, 7), (2, 7)]);
    let alias = table(&[(1, 7), (9, 7)]);
    let stale = table(&[(1, 6), (9, 6)]);
    let current = posting(common.clone(), 1);
    let mut credits = QueryBlockCredits::from_query_permit(Box::new(Permit)).unwrap();
    let mut budget = Budget::new(QueryExecutionLimits::default_for_memory());
    let mut same = candidates(
        &[current.clone(), posting(common, 2)],
        &mut credits,
        &mut budget,
    );
    let next = candidates(&[current.clone()], &mut credits, &mut budget);
    same.intersect(next, &mut credits, &mut budget).unwrap();
    assert_eq!(
        same.entries.keys().copied().collect::<Vec<_>>(),
        vec![key(1)]
    );
    same.release(&mut credits, &mut budget).unwrap();
    let mut across = candidates(&[current.clone()], &mut credits, &mut budget);
    let next = candidates(&[posting(alias, 1)], &mut credits, &mut budget);
    across.intersect(next, &mut credits, &mut budget).unwrap();
    assert!(
        across.contains(&key(1)),
        "same exact object version in distinct recipe tables must join"
    );
    let next = candidates(&[posting(stale, 1)], &mut credits, &mut budget);
    across.intersect(next, &mut credits, &mut budget).unwrap();
    assert_eq!(
        across.len(),
        0,
        "different material versions cannot satisfy AND"
    );
    assert_eq!(
        budget.heap_bytes(),
        0,
        "candidate mask allocations are released"
    );
}

#[test]
fn dense_boolean_union_preserves_identity_and_prefers_exact_newer_material() {
    let first = table(&[(1, 7), (2, 7)]);
    let second = table(&[(1, 8), (3, 8)]);
    let mut credits = QueryBlockCredits::from_query_permit(Box::new(Permit)).unwrap();
    let mut budget = Budget::new(QueryExecutionLimits::default_for_memory());
    let mut left = candidates(
        &[posting(first.clone(), 1), posting(first, 2)],
        &mut credits,
        &mut budget,
    );
    let right = candidates(
        &[posting(second.clone(), 1), posting(second, 3)],
        &mut credits,
        &mut budget,
    );
    assert_eq!(left.union(right, &mut credits, &mut budget).unwrap(), 1);
    assert_eq!(left.len(), 3);
    assert_eq!(left.entries[&key(1)].material_source_version, 8);
    let keys = left.into_document_keys(&mut credits, &mut budget).unwrap();
    assert_eq!(budget.heap_bytes(), document_key_set_bytes(3).unwrap());
    drop(keys);
    budget
        .release_heap(&mut credits, document_key_set_bytes(3).unwrap())
        .unwrap();
    assert_eq!(budget.heap_bytes(), 0);
}

#[test]
fn full_text_never_combines_current_term_with_stale_term_of_same_object() {
    let current = posting(table(&[(1, 7)]), 1);
    let stale = posting(table(&[(1, 6)]), 1);
    let first = ScalarValue::String("first".into());
    let second = ScalarValue::String("second".into());
    let mut postings = TermPostingMap::new();
    postings.insert(first.clone(), [(key(1), (0, current))].into());
    postings.insert(second.clone(), [(key(1), (1, stale))].into());
    let mut credits = QueryBlockCredits::from_query_permit(Box::new(Permit)).unwrap();
    let mut budget = Budget::new(QueryExecutionLimits::default_for_memory());
    assert!(
        intersect_term_candidates(&postings, &[first, second], &mut credits, &mut budget)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn sparse_candidate_masks_allocate_only_populated_words() {
    let documents = Arc::new(
        SegmentDocumentTable::new_with_versions((1_u64..=65536).map(|ordinal| {
            let mut bytes = [0; 32];
            bytes[24..].copy_from_slice(&ordinal.to_be_bytes());
            (StableDocumentKey::from_bytes(bytes).unwrap(), 7)
        }))
        .unwrap(),
    );
    let mut mask = SegmentLiveDocuments::none_live(documents);
    let initial = mask.resident_bytes();
    mask.set_live(SegmentDocumentId(65535)).unwrap();
    assert_eq!(mask.resident_bytes() - initial, 512);
    assert!(mask.is_live(SegmentDocumentId(65535)));
    assert!(!mask.is_live(SegmentDocumentId(0)));
}
