use super::*;

#[test]
fn authz_consistency_parses_latest_without_zookie() {
    assert_eq!(
        AuthzConsistency::from_request("", "").unwrap(),
        AuthzConsistency::Latest
    );
    assert_eq!(
        AuthzConsistency::from_request("latest", "").unwrap(),
        AuthzConsistency::Latest
    );
}

#[test]
fn authz_consistency_requires_zookie_for_at_least_and_exact() {
    assert_eq!(
        AuthzConsistency::from_request("at_least", "authz:42").unwrap(),
        AuthzConsistency::AtLeast(42)
    );
    assert_eq!(
        AuthzConsistency::from_request("exact", "authz:7").unwrap(),
        AuthzConsistency::Exact(7)
    );
    assert_eq!(
        AuthzConsistency::from_request("exact", "")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        AuthzConsistency::from_request("at_least", "authz:-1")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn authz_watch_cursor_split_round_trips() {
    let cursor = (u128::from(99_u64) << 64) | u128::from(42_u64);
    let (low, high) = split_u128(cursor);
    assert_eq!(join_u128(low, high), cursor);
}

#[test]
fn authz_pages_bind_tokens_to_principal_filter_and_revision() {
    let object_filter = authz_page_filter_hash("read_tuples", &["object_order"]);
    let binding = AuthzPageBinding {
        tenant_id: 7,
        principal_id: "app-a",
        revision: 42,
        filter_hash: &object_filter,
        page_size: 1,
    };
    let token = encode_authz_page_token(&binding, "a", b"test-key").unwrap();
    let token = parse_authz_page_token(&token, &binding, b"test-key")
        .unwrap()
        .unwrap();
    assert_eq!(token.position, "a");

    let other_principal = AuthzPageBinding {
        principal_id: "app-b",
        ..binding
    };
    assert_eq!(
        parse_authz_page_token(
            &encode_authz_page_token(&binding, "a", b"test-key").unwrap(),
            &other_principal,
            b"test-key",
        )
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );

    let subject_filter = authz_page_filter_hash("read_tuples", &["subject_order"]);
    let other_filter = AuthzPageBinding {
        filter_hash: &subject_filter,
        ..binding
    };
    assert_eq!(
        parse_authz_page_token(
            &encode_authz_page_token(&binding, "a", b"test-key").unwrap(),
            &other_filter,
            b"test-key",
        )
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );

    let other_revision = AuthzPageBinding {
        revision: 43,
        ..binding
    };
    assert_eq!(
        parse_authz_page_token(
            &encode_authz_page_token(&binding, "a", b"test-key").unwrap(),
            &other_revision,
            b"test-key",
        )
        .unwrap_err()
        .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn authz_page_size_rejects_unbounded_requests() {
    assert_eq!(normalize_page_size(0).unwrap(), 100);
    assert_eq!(normalize_page_size(1000).unwrap(), 1000);
    assert_eq!(
        normalize_page_size(1001).unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn authz_page_positions_cover_the_full_coremeta_tuple_key_bound() {
    let position = vec![0x5a; MAX_AUTHZ_PAGE_POSITION_BYTES];
    let encoded = encode_authz_page_position(&position).unwrap();
    assert_eq!(
        decode_authz_page_position(Some(&encoded)).unwrap(),
        Some(position)
    );
    assert!(encode_authz_page_position(&vec![0; MAX_AUTHZ_PAGE_POSITION_BYTES + 1]).is_err());
}
