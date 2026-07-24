use crate::anvil_api::PageRequest;
use crate::system_realm::AdminPrincipal;
use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tonic::{Code, Status};

type HmacSha256 = Hmac<Sha256>;

const CURSOR_VERSION: u8 = 1;
const CURSOR_DOMAIN: &[u8] = b"anvil-admin-list-cursor-v1";

#[derive(Debug, Clone, Copy)]
pub(crate) struct AdminCursorBinding<'a> {
    pub scope: &'static str,
    pub filters: &'a [(&'a str, &'a str)],
    pub principal: &'a AdminPrincipal,
    pub limit: usize,
    pub revision: &'a str,
    pub sort: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AdminListCursorToken {
    version: u8,
    scope: String,
    position: String,
    filter_hash: String,
    principal_hash: String,
    limit: u32,
    revision: String,
    sort: String,
    signature: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct AdminListCursorTokenProto {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    scope: String,
    #[prost(string, tag = "3")]
    position: String,
    #[prost(string, tag = "4")]
    filter_hash: String,
    #[prost(string, tag = "5")]
    principal_hash: String,
    #[prost(uint32, tag = "6")]
    limit: u32,
    #[prost(string, tag = "7")]
    revision: String,
    #[prost(string, tag = "8")]
    sort: String,
    #[prost(string, tag = "9")]
    signature: String,
}

pub(crate) fn decode_page_cursor(
    page: Option<&PageRequest>,
    binding: &AdminCursorBinding<'_>,
    signing_key: &[u8],
) -> Result<Option<String>, Status> {
    let Some(cursor) = page
        .map(|page| page.page_token.trim())
        .filter(|cursor| !cursor.is_empty())
    else {
        return Ok(None);
    };

    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| invalid_cursor())?;
    let token = admin_list_cursor_from_proto(
        crate::core_store::decode_deterministic_proto::<AdminListCursorTokenProto>(
            &bytes,
            "admin list cursor",
        )
        .map_err(|_| invalid_cursor())?,
    )?;
    if token.version != CURSOR_VERSION {
        return Err(invalid_cursor());
    }

    let expected_signature = sign_cursor_claims(
        CursorSignatureClaims {
            version: token.version,
            scope: &token.scope,
            position: &token.position,
            filter_hash: &token.filter_hash,
            principal_hash: &token.principal_hash,
            limit: token.limit,
            revision: &token.revision,
            sort: &token.sort,
        },
        signing_key,
    )?;
    if !constant_time_eq::constant_time_eq(
        token.signature.as_bytes(),
        expected_signature.as_bytes(),
    ) {
        return Err(invalid_cursor());
    }

    let expected = expected_claims(binding)?;
    if token.scope != binding.scope
        || token.filter_hash != expected.filter_hash
        || token.principal_hash != expected.principal_hash
        || token.limit != expected.limit
        || token.revision != binding.revision
        || token.sort != binding.sort
    {
        return Err(Status::invalid_argument(
            "Admin list cursor does not match this request",
        ));
    }

    Ok(Some(token.position))
}

pub(crate) fn encode_next_cursor(
    position: &str,
    binding: &AdminCursorBinding<'_>,
    signing_key: &[u8],
) -> Result<String, Status> {
    let expected = expected_claims(binding)?;
    let signature = sign_cursor_claims(
        CursorSignatureClaims {
            version: CURSOR_VERSION,
            scope: binding.scope,
            position,
            filter_hash: &expected.filter_hash,
            principal_hash: &expected.principal_hash,
            limit: expected.limit,
            revision: binding.revision,
            sort: binding.sort,
        },
        signing_key,
    )?;
    let token = AdminListCursorToken {
        version: CURSOR_VERSION,
        scope: binding.scope.to_string(),
        position: position.to_string(),
        filter_hash: expected.filter_hash,
        principal_hash: expected.principal_hash,
        limit: expected.limit,
        revision: binding.revision.to_string(),
        sort: binding.sort.to_string(),
        signature,
    };
    let bytes = crate::core_store::encode_deterministic_proto(&admin_list_cursor_to_proto(&token));
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn admin_list_cursor_to_proto(token: &AdminListCursorToken) -> AdminListCursorTokenProto {
    AdminListCursorTokenProto {
        version: u32::from(token.version),
        scope: token.scope.clone(),
        position: token.position.clone(),
        filter_hash: token.filter_hash.clone(),
        principal_hash: token.principal_hash.clone(),
        limit: token.limit,
        revision: token.revision.clone(),
        sort: token.sort.clone(),
        signature: token.signature.clone(),
    }
}

fn admin_list_cursor_from_proto(
    proto: AdminListCursorTokenProto,
) -> Result<AdminListCursorToken, Status> {
    Ok(AdminListCursorToken {
        version: u8::try_from(proto.version).map_err(|_| invalid_cursor())?,
        scope: proto.scope,
        position: proto.position,
        filter_hash: proto.filter_hash,
        principal_hash: proto.principal_hash,
        limit: proto.limit,
        revision: proto.revision,
        sort: proto.sort,
        signature: proto.signature,
    })
}

pub(crate) fn collection_revision<'a>(records: impl IntoIterator<Item = (&'a str, u64)>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"admin-list-collection-revision-v1");
    for (key, generation) in records {
        update_part(&mut hasher, key.as_bytes());
        hasher.update(&generation.to_le_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpectedClaims {
    filter_hash: String,
    principal_hash: String,
    limit: u32,
}

#[derive(Debug, Clone, Copy)]
struct CursorSignatureClaims<'a> {
    version: u8,
    scope: &'a str,
    position: &'a str,
    filter_hash: &'a str,
    principal_hash: &'a str,
    limit: u32,
    revision: &'a str,
    sort: &'a str,
}

fn expected_claims(binding: &AdminCursorBinding<'_>) -> Result<ExpectedClaims, Status> {
    Ok(ExpectedClaims {
        filter_hash: filter_hash(binding.filters),
        principal_hash: principal_hash(binding.principal),
        limit: u32::try_from(binding.limit)
            .map_err(|_| Status::invalid_argument("Admin list limit is invalid"))?,
    })
}

fn filter_hash(filters: &[(&str, &str)]) -> String {
    let mut filters = filters.to_vec();
    filters.sort_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(right.1)));

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"admin-list-filter-v1");
    for (name, value) in filters {
        update_part(&mut hasher, name.as_bytes());
        update_part(&mut hasher, value.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn principal_hash(principal: &AdminPrincipal) -> String {
    let mut methods = principal.authenticated_methods.clone();
    methods.sort();

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"admin-list-principal-v1");
    update_part(&mut hasher, principal.principal_id.as_bytes());
    hasher.update(&principal.tenant_id.to_le_bytes());
    for method in methods {
        update_part(&mut hasher, method.as_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn sign_cursor_claims(
    claims: CursorSignatureClaims<'_>,
    signing_key: &[u8],
) -> Result<String, Status> {
    let mut mac = HmacSha256::new_from_slice(signing_key)
        .map_err(|_| Status::internal("Invalid admin list cursor signing key"))?;
    mac.update(CURSOR_DOMAIN);
    mac.update(&[claims.version]);
    update_mac_part(&mut mac, claims.scope.as_bytes());
    update_mac_part(&mut mac, claims.position.as_bytes());
    update_mac_part(&mut mac, claims.filter_hash.as_bytes());
    update_mac_part(&mut mac, claims.principal_hash.as_bytes());
    mac.update(&claims.limit.to_le_bytes());
    update_mac_part(&mut mac, claims.revision.as_bytes());
    update_mac_part(&mut mac, claims.sort.as_bytes());
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn update_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn update_mac_part(mac: &mut HmacSha256, value: &[u8]) {
    mac.update(&(value.len() as u64).to_le_bytes());
    mac.update(value);
}

fn invalid_cursor() -> Status {
    Status::new(Code::InvalidArgument, "Invalid admin list cursor")
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"admin cursor signing key";

    fn test_principal(id: &str) -> AdminPrincipal {
        AdminPrincipal {
            principal_id: id.to_string(),
            tenant_id: 7,
            authenticated_methods: vec!["bearer".to_string()],
            checked_relation: None,
            checked_object: None,
        }
    }

    #[test]
    fn admin_cursor_round_trips_and_binds_request_context() {
        let principal = test_principal("admin-a");
        let filters = [("tenant_id", "7"), ("bucket_name", "photos")];
        let binding = AdminCursorBinding {
            scope: "admin.list_host_aliases.v1",
            filters: &filters,
            principal: &principal,
            limit: 25,
            revision: "rev-a",
            sort: "link_key.asc",
        };
        let cursor = encode_next_cursor("photos/2026.jpg", &binding, KEY).unwrap();
        let page = PageRequest {
            page_size: 25,
            page_token: cursor,
        };

        assert_eq!(
            decode_page_cursor(Some(&page), &binding, KEY).unwrap(),
            Some("photos/2026.jpg".to_string())
        );

        let other_filters = [("tenant_id", "7"), ("bucket_name", "videos")];
        let other_filter_binding = AdminCursorBinding {
            filters: &other_filters,
            ..binding
        };
        assert_eq!(
            decode_page_cursor(Some(&page), &other_filter_binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let other_principal = test_principal("admin-b");
        let other_principal_binding = AdminCursorBinding {
            principal: &other_principal,
            ..binding
        };
        assert_eq!(
            decode_page_cursor(Some(&page), &other_principal_binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let other_limit_binding = AdminCursorBinding {
            limit: 50,
            ..binding
        };
        assert_eq!(
            decode_page_cursor(Some(&page), &other_limit_binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let other_revision_binding = AdminCursorBinding {
            revision: "rev-b",
            ..binding
        };
        assert_eq!(
            decode_page_cursor(Some(&page), &other_revision_binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );

        let other_sort_binding = AdminCursorBinding {
            sort: "link_key.desc",
            ..binding
        };
        assert_eq!(
            decode_page_cursor(Some(&page), &other_sort_binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn admin_cursor_rejects_tampering_and_empty_cursor_is_absent() {
        let principal = test_principal("admin-a");
        let filters = [("region", "eu-west-1")];
        let binding = AdminCursorBinding {
            scope: "admin.list_host_aliases.v1",
            filters: &filters,
            principal: &principal,
            limit: 100,
            revision: "rev-a",
            sort: "hostname.asc",
        };
        let mut cursor = encode_next_cursor("example.com", &binding, KEY).unwrap();
        cursor.push('a');
        let page = PageRequest {
            page_size: 100,
            page_token: cursor,
        };

        assert_eq!(
            decode_page_cursor(Some(&page), &binding, KEY)
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(decode_page_cursor(None, &binding, KEY).unwrap(), None);
        assert_eq!(
            decode_page_cursor(
                Some(&PageRequest {
                    page_size: 100,
                    page_token: String::new(),
                }),
                &binding,
                KEY,
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn collection_revision_binds_keys_and_generations() {
        let baseline = collection_revision([("a", 1), ("b", 2)]);

        assert_eq!(baseline, collection_revision([("a", 1), ("b", 2)]));
        assert_ne!(baseline, collection_revision([("a", 1), ("b", 3)]));
        assert_ne!(baseline, collection_revision([("b", 2), ("a", 1)]));
    }
}
