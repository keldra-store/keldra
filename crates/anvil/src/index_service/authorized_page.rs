//! Authorization-aware index pagination.
//!
//! Engine positions may identify source objects, so they are private until the
//! corresponding hit has passed Zanzibar. This collector first tries one
//! authorization batch. It returns that batch only when Zanzibar preserved
//! every hit in its original order, so the continuation still follows an
//! authorized hit. Any filtering or reordering falls back to the deliberately
//! simple one-candidate scan.

use std::future::Future;

use anvil_api::v1::{IndexFreshness, IndexQueryHit};
use tonic::Status;

use super::{ExecutedIndexQuery, IndexPageCursor};

pub(crate) async fn collect_authorized_page<Execute, ExecuteFuture, Authorize, AuthorizeFuture>(
    requested_limit: usize,
    initial_resume: Option<IndexPageCursor>,
    required_authorization_revision: Option<u64>,
    mut execute_page: Execute,
    mut authorize: Authorize,
) -> Result<ExecutedIndexQuery, Status>
where
    Execute: FnMut(Option<IndexPageCursor>, usize) -> ExecuteFuture,
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

    let fast = execute_page(initial_resume.clone(), requested_limit).await?;
    validate_candidate_page(&fast, initial_resume.as_ref(), requested_limit)?;
    let fast_upstream_revision = fast.freshness.authorization_revision;
    let (fast_authorized, fast_authorization_revision) = authorize(fast.hits.clone()).await?;
    if fast_authorization_revision == 0 || fast_authorized.len() > fast.hits.len() {
        return Err(Status::data_loss(
            "Zanzibar returned invalid index authorization evidence",
        ));
    }
    if fast_upstream_revision != 0 && fast_upstream_revision != fast_authorization_revision {
        return Err(revision_changed());
    }
    let required_revision = required_authorization_revision.or_else(|| {
        initial_resume
            .as_ref()
            .map(|cursor| cursor.authorization_revision)
    });
    if required_revision.is_some_and(|required| required != fast_authorization_revision) {
        return Err(revision_changed());
    }
    let fast_continuation_is_safe = fast.next_position.is_none() || !fast.hits.is_empty();
    if fast_continuation_is_safe && fast_authorized == fast.hits {
        let mut freshness = fast.freshness;
        freshness.authorization_revision = fast_authorization_revision;
        return Ok(ExecutedIndexQuery {
            hits: fast_authorized,
            freshness,
            next_position: fast.next_position,
        });
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
        let raw = execute_page(scan_resume.clone(), 1).await?;
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

fn validate_candidate_page(
    result: &ExecutedIndexQuery,
    resume: Option<&IndexPageCursor>,
    limit: usize,
) -> Result<(), Status> {
    if result.hits.len() > limit
        || result.next_position.as_ref().is_some_and(Vec::is_empty)
        || (result.next_position.is_some() && result.freshness.generation == 0)
    {
        return Err(Status::data_loss(
            "index executor returned an invalid candidate page",
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anvil_api::v1::ObjectAddress;
    use anvil_store::StorageTenantId;

    use super::super::boundary::{IndexPageTokenBinding, IndexPageTokenCodec};
    use super::*;
    use crate::authentication::{Caller, JwtManager};

    const KEY: &[u8; 32] = b"0123456789abcdef0123456789abcdef";

    async fn execute_page(
        paths: Arc<Vec<&'static str>>,
        resume: Option<IndexPageCursor>,
        limit: usize,
    ) -> Result<ExecutedIndexQuery, Status> {
        let start = match resume.as_ref() {
            Some(cursor) => {
                paths
                    .iter()
                    .position(|path| path.as_bytes() == cursor.last_position)
                    .ok_or_else(|| Status::invalid_argument("unknown test continuation"))?
                    + 1
            }
            None => 0,
        };
        if start >= paths.len() {
            return Ok(ExecutedIndexQuery {
                hits: Vec::new(),
                freshness: freshness(),
                next_position: None,
            });
        }
        let end = start.saturating_add(limit).min(paths.len());
        let hits = paths[start..end]
            .iter()
            .map(|path| hit(path))
            .collect::<Vec<_>>();
        Ok(ExecutedIndexQuery {
            hits,
            freshness: freshness(),
            next_position: (end < paths.len()).then(|| paths[end - 1].as_bytes().to_vec()),
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

    fn all_authorized(
        hits: Vec<IndexQueryHit>,
    ) -> impl std::future::Future<Output = Result<(Vec<IndexQueryHit>, u64), Status>> {
        std::future::ready(Ok((hits, 17)))
    }

    #[tokio::test]
    async fn all_authorized_page_uses_one_execution_and_one_authorization_batch() {
        let paths = Arc::new(vec!["docs/a", "docs/b", "docs/c", "docs/d"]);
        let executions = Arc::new(AtomicUsize::new(0));
        let authorizations = Arc::new(AtomicUsize::new(0));
        let execute_paths = paths.clone();
        let execute_count = executions.clone();
        let authorization_count = authorizations.clone();

        let page = collect_authorized_page(
            100,
            None,
            None,
            move |resume, limit| {
                execute_count.fetch_add(1, Ordering::Relaxed);
                execute_page(execute_paths.clone(), resume, limit)
            },
            move |hits| {
                authorization_count.fetch_add(1, Ordering::Relaxed);
                all_authorized(hits)
            },
        )
        .await
        .unwrap();

        assert_eq!(
            page.hits,
            vec![hit("docs/a"), hit("docs/b"), hit("docs/c"), hit("docs/d")]
        );
        assert_eq!(page.freshness.authorization_revision, 17);
        assert!(page.next_position.is_none());
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert_eq!(authorizations.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn denied_batch_falls_back_without_exposing_denied_position() {
        let paths = Arc::new(vec!["docs/a", "docs/hidden", "docs/c", "docs/d"]);
        let executions = Arc::new(AtomicUsize::new(0));
        let execute_paths = paths.clone();
        let execute_count = executions.clone();
        let page = collect_authorized_page(
            2,
            None,
            None,
            move |resume, limit| {
                execute_count.fetch_add(1, Ordering::Relaxed);
                execute_page(execute_paths.clone(), resume, limit)
            },
            authorize_paths,
        )
        .await
        .unwrap();

        assert_eq!(page.hits, vec![hit("docs/a"), hit("docs/c")]);
        assert_eq!(page.next_position.as_deref(), Some(b"docs/c".as_slice()));
        assert_eq!(executions.load(Ordering::Relaxed), 5);
    }

    #[tokio::test]
    async fn reordered_batch_falls_back_to_original_engine_order() {
        let paths = Arc::new(vec!["docs/a", "docs/b", "docs/c"]);
        let executions = Arc::new(AtomicUsize::new(0));
        let execute_paths = paths.clone();
        let execute_count = executions.clone();
        let page = collect_authorized_page(
            2,
            None,
            None,
            move |resume, limit| {
                execute_count.fetch_add(1, Ordering::Relaxed);
                execute_page(execute_paths.clone(), resume, limit)
            },
            |mut hits| async move {
                hits.reverse();
                Ok((hits, 17))
            },
        )
        .await
        .unwrap();

        assert_eq!(page.hits, vec![hit("docs/a"), hit("docs/b")]);
        assert_eq!(page.next_position.as_deref(), Some(b"docs/b".as_slice()));
        assert_eq!(executions.load(Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn empty_batch_with_continuation_uses_serial_fallback() {
        let executions = Arc::new(AtomicUsize::new(0));
        let execute_count = executions.clone();
        let page = collect_authorized_page(
            2,
            None,
            None,
            move |_resume, limit| {
                let call = execute_count.fetch_add(1, Ordering::Relaxed);
                async move {
                    if call == 0 {
                        assert_eq!(limit, 2);
                        Ok(ExecutedIndexQuery {
                            hits: Vec::new(),
                            freshness: freshness(),
                            next_position: Some(b"private-position".to_vec()),
                        })
                    } else {
                        assert_eq!(limit, 1);
                        Ok(ExecutedIndexQuery {
                            hits: vec![hit("docs/a")],
                            freshness: freshness(),
                            next_position: None,
                        })
                    }
                }
            },
            all_authorized,
        )
        .await
        .unwrap();

        assert_eq!(page.hits, vec![hit("docs/a")]);
        assert!(page.next_position.is_none());
        assert_eq!(executions.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn batch_fast_path_preserves_revision_and_generation_checks() {
        let revision_error = collect_authorized_page(
            2,
            None,
            None,
            |_resume, _limit| async {
                let mut observed = freshness();
                observed.authorization_revision = 19;
                Ok(ExecutedIndexQuery {
                    hits: vec![hit("docs/a")],
                    freshness: observed,
                    next_position: None,
                })
            },
            all_authorized,
        )
        .await
        .unwrap_err();
        assert_eq!(revision_error.code(), tonic::Code::FailedPrecondition);

        let resume = IndexPageCursor {
            generation: 99,
            last_position: b"docs/previous".to_vec(),
            authorization_revision: 17,
        };
        let authorization_calls = Arc::new(AtomicUsize::new(0));
        let counted = authorization_calls.clone();
        let generation_error = collect_authorized_page(
            2,
            Some(resume),
            Some(17),
            |_resume, _limit| async {
                Ok(ExecutedIndexQuery {
                    hits: vec![hit("docs/a")],
                    freshness: freshness(),
                    next_position: None,
                })
            },
            move |hits| {
                counted.fetch_add(1, Ordering::Relaxed);
                all_authorized(hits)
            },
        )
        .await
        .unwrap_err();
        assert_eq!(generation_error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(authorization_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn hidden_candidates_never_enter_a_token_and_later_visible_hits_are_pageable() {
        let paths = Arc::new(vec!["docs/a", "docs/hidden", "docs/c", "docs/d"]);
        let first_paths = paths.clone();
        let first = collect_authorized_page(
            1,
            None,
            None,
            move |resume, limit| execute_page(first_paths.clone(), resume, limit),
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
            move |resume, limit| execute_page(second_paths.clone(), resume, limit),
            authorize_paths,
        )
        .await
        .unwrap();
        assert_eq!(second.hits, vec![hit("docs/c")]);
    }
}
