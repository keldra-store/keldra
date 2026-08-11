#[test]
fn query_tokenization_bounds_cursor_fanout_before_building_cursor_state() {
    let text = (0..=MAX_QUERY_TERM_CURSORS)
        .map(|index| format!("term{index}"))
        .collect::<Vec<_>>()
        .join(" ");
    assert_eq!(
        query_terms(&text, false).unwrap_err(),
        IndexError::ResourceLimit {
            needed: (MAX_QUERY_TERM_CURSORS + 1) * crate::MAX_INDEX_DECODED_BLOCK_BYTES,
            limit: MAX_QUERY_TERM_CURSORS * crate::MAX_INDEX_DECODED_BLOCK_BYTES,
        }
    );
}

#[test]
fn repeated_non_phrase_terms_share_one_cursor_but_phrase_state_is_bounded() {
    let repeated = std::iter::repeat_n("same", MAX_QUERY_TERM_CURSORS + 1)
        .collect::<Vec<_>>()
        .join(" ");
    let (terms, unique) = query_terms(&repeated, false).unwrap();
    assert_eq!(terms, ["same"]);
    assert_eq!(unique, ["same"]);
    assert!(matches!(
        query_terms(&repeated, true),
        Err(IndexError::ResourceLimit { .. })
    ));
}
