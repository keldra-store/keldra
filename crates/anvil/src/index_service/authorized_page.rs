//! Authorization-aware index pagination.
//!
//! Engine positions may identify source objects, so they are private until the
//! corresponding hit has passed Zanzibar. This collector deliberately asks
//! for one candidate at a time. That keeps the implementation simple and
//! ensures a public continuation can only point immediately after a hit the
//! caller was allowed to see.

use std::future::Future;

use anvil_api::v1::{IndexFreshness, IndexQueryHit};
use tonic::Status;

use super::{ExecutedIndexQuery, IndexPageCursor};

pub(crate) async fn collect_authorized_page<Execute, ExecuteFuture, Authorize, AuthorizeFuture>(
    requested_limit: usize,
    initial_resume: Option<IndexPageCursor>,
    required_authorization_revision: Option<u64>,
    mut execute_one: Execute,
    mut authorize: Authorize,
) -> Result<ExecutedIndexQuery, Status>
where
    Execute: FnMut(Option<IndexPageCursor>) -> ExecuteFuture,
    ExecuteFuture: Future<Output = Result<ExecutedIndexQuery, Status>>,
    Authorize: FnMut(Vec<IndexQueryHit>) -> AuthorizeFuture,
    AuthorizeFuture: Future<Output = Result<(Vec<IndexQueryHit>, u64), Status>>,
{
    if requested_limit == 0 {
        return Err(Status::internal(
            "authorization-aware index pagination requires a non-zero limit",
        ));
    }
    if required_authorization_revision == Some(0) {
        return Err(Status::data_loss(
            "index authorization evidence has a zero revision",
        ));
    }
    if let (Some(required), Some(resume)) =
        (required_authorization_revision, initial_resume.as_ref())
    {
        if required != resume.authorization_revision {
            return Err(revision_changed());
        }
    }

    let mut scan_resume = initial_resume;
    let mut stable_revision = required_authorization_revision.or_else(|| {
        scan_resume
            .as_ref()
            .map(|cursor| cursor.authorization_revision)
    });
    let mut stable_freshness: Option<FreshnessIdentity> = None;
    let mut visible = Vec::with_capacity(requested_limit);
    let mut cursor_after_last_visible = None;

    loop {
        let raw = execute_one(scan_resume.clone()).await?;
        validate_single_candidate(&raw, scan_resume.as_ref())?;
        let identity = FreshnessIdentity::from(&raw.freshness);
        if let Some(stable) = stable_freshness {
            if stable != identity {
                return Err(Status::failed_precondition(
                    "index generation changed during pagination",
                ));
            }
        } else {
            stable_freshness = Some(identity);
        }

        let upstream_revision = raw.freshness.authorization_revision;
        let raw_hit_count = raw.hits.len();
        let raw_next = raw.next_position.clone();
        let (authorized, authorization_revision) = authorize(raw.hits).await?;
        if authorization_revision == 0 || authorized.len() > raw_hit_count {
            return Err(Status::data_loss(
                "Zanzibar returned invalid index authorization evidence",
            ));
        }
        if upstream_revision != 0 && upstream_revision != authorization_revision {
            return Err(revision_changed());
        }
        match stable_revision {
            Some(stable) if stable != authorization_revision => {
                return Err(revision_changed());
            }
            None => stable_revision = Some(authorization_revision),
            Some(_) => {}
        }

        let mut freshness = raw.freshness;
        freshness.authorization_revision = authorization_revision;

        if let Some(hit) = authorized.into_iter().next() {
            if visible.len() == requested_limit {
                let next_position = cursor_after_last_visible.ok_or_else(|| {
                    Status::data_loss("an authorized index continuation has no visible predecessor")
                })?;
                return Ok(ExecutedIndexQuery {
                    hits: visible,
                    freshness,
                    next_position: Some(next_position),
                });
            }
            visible.push(hit);
            cursor_after_last_visible = raw_next.clone();
        }

        let Some(last_position) = raw_next else {
            return Ok(ExecutedIndexQuery {
                hits: visible,
                freshness,
                next_position: None,
            });
        };
        if scan_resume
            .as_ref()
            .is_some_and(|resume| resume.last_position == last_position)
        {
            return Err(Status::data_loss(
                "index executor returned a continuation that made no progress",
            ));
        }
        scan_resume = Some(IndexPageCursor {
            generation: identity.generation,
            last_position,
            authorization_revision,
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FreshnessIdentity {
    generation: u64,
    placement_term: u64,
    placement_index: u64,
    index_id: u64,
    definition_version: u64,
}

impl From<&IndexFreshness> for FreshnessIdentity {
    fn from(freshness: &IndexFreshness) -> Self {
        Self {
            generation: freshness.generation,
            placement_term: freshness.placement_term,
            placement_index: freshness.placement_index,
            index_id: freshness.index_id,
            definition_version: freshness.definition_version,
        }
    }
}

fn validate_single_candidate(
    result: &ExecutedIndexQuery,
    resume: Option<&IndexPageCursor>,
) -> Result<(), Status> {
    if result.hits.len() > 1
        || result.next_position.as_ref().is_some_and(Vec::is_empty)
        || (result.next_position.is_some() && result.freshness.generation == 0)
    {
        return Err(Status::data_loss(
            "index executor returned an invalid single-candidate page",
        ));
    }
    if resume.is_some_and(|resume| resume.generation != result.freshness.generation) {
        return Err(Status::failed_precondition(
            "requested index generation is no longer available",
        ));
    }
    Ok(())
}

fn revision_changed() -> Status {
    Status::failed_precondition("authorization revision changed during index pagination")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anvil_api::v1::ObjectAddress;
    use anvil_store::StorageTenantId;

    use super::super::boundary::{IndexPageTokenBinding, IndexPageTokenCodec};
    use super::*;
    use crate::authentication::{Caller, JwtManager};

    const KEY: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

    async fn execute_candidate(
        paths: Arc<Vec<&'static str>>,
        resume: Option<IndexPageCursor>,
    ) -> Result<ExecutedIndexQuery, Status> {
        let index = match resume.as_ref() {
            Some(cursor) => {
                paths
                    .iter()
                    .position(|path| path.as_bytes() == cursor.last_position)
                    .ok_or_else(|| Status::invalid_argument("unknown test continuation"))?
                    + 1
            }
            None => 0,
        };
        let Some(path) = paths.get(index) else {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: freshness(),
                next_position: None,
            });
        };
        Ok(ExecutedIndexQuery {
            hits: vec![hit(path)],
            freshness: freshness(),
            next_position: (index + 1 < paths.len()).then(|| path.as_bytes().to_vec()),
        })
    }

    async fn authorize_paths(
        hits: Vec<IndexQueryHit>,
    ) -> Result<(Vec<IndexQueryHit>, u64), Status> {
        Ok((
            hits.into_iter()
                .filter(|hit| {
                    hit.address
                        .as_ref()
                        .is_some_and(|address| address.path != "docs/hidden")
                })
                .collect(),
            17,
        ))
    }

    fn hit(path: &str) -> IndexQueryHit {
        IndexQueryHit {
            address: Some(ObjectAddress {
                tenant: "tenant".into(),
                bucket: "objects".into(),
                path: path.into(),
            }),
            object_version: 1,
            score: None,
            fields_json: Vec::new(),
        }
    }

    fn freshness() -> IndexFreshness {
        IndexFreshness {
            generation: 31,
            placement_term: 2,
            placement_index: 3,
            index_id: 5,
            definition_version: 7,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn hidden_candidates_never_enter_a_token_and_later_visible_hits_are_pageable() {
        let paths = Arc::new(vec!["docs/a", "docs/hidden", "docs/c", "docs/d"]);
        let first_paths = paths.clone();
        let first = collect_authorized_page(
            1,
            None,
            None,
            move |resume| execute_candidate(first_paths.clone(), resume),
            authorize_paths,
        )
        .await
        .unwrap();

        assert_eq!(first.hits, vec![hit("docs/a")]);
        assert_eq!(first.next_position.as_deref(), Some(b"docs/a".as_slice()));

        let caller = Caller::from_authenticated_application(
            StorageTenantId::parse("tenant").unwrap(),
            "application",
        )
        .unwrap();
        let binding = IndexPageTokenBinding {
            index_id: 5,
            definition_version: 7,
            query_hash: [9; 32],
        };
        let cursor = IndexPageCursor {
            generation: first.freshness.generation,
            last_position: first.next_position.clone().unwrap(),
            authorization_revision: first.freshness.authorization_revision,
        };
        let tokens = JwtManager::new(KEY).unwrap();
        let token = tokens.encode(&caller, binding, &cursor).unwrap();
        let decoded = tokens.decode(&caller, &token, binding).unwrap();
        assert_eq!(decoded.last_position, b"docs/a");
        assert!(
            !decoded
                .last_position
                .windows(b"hidden".len())
                .any(|window| window == b"hidden")
        );

        let second_paths = paths.clone();
        let second = collect_authorized_page(
            1,
            Some(decoded),
            None,
            move |resume| execute_candidate(second_paths.clone(), resume),
            authorize_paths,
        )
        .await
        .unwrap();
        assert_eq!(second.hits, vec![hit("docs/c")]);
    }
}
