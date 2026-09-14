use super::{IndexClient, config::Config, data, index_client, progress::Counters, query_page};
use anyhow::{Context, Result, bail, ensure};
use keldra_storage::object_client;
use keldra_storage::v1::object_head::State as ObjectHeadState;
use keldra_storage::v1::{
    HeadObjectRequest, IndexFreshness, IndexSourceFreshness, ObjectAddress, QueryIndexResponse,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tonic::transport::Channel;

// Final-state qualification must use the public API's maximum accepted page.
// Query execution itself is optimized to continue from the pinned page token;
// the harness must not bypass that public pagination contract.
const FINAL_VERIFICATION_PAGE_SIZE: u32 = 1_000;

pub(super) async fn load_authoritative_mutable_state(
    config: &Config,
    channels: &[Channel],
    token: &str,
    expected_mutable_ids: Arc<[bool]>,
) -> Result<Arc<BTreeMap<String, u64>>> {
    let mut authority = BTreeMap::new();
    let mutable_records = usize::try_from(config.mutable_records)
        .context("configured mutable key count does not fit in memory")?;
    let worker_count = config.query_max_in_flight.min(mutable_records);
    let mut workers = JoinSet::new();
    for worker in 0..worker_count {
        let channel = channels[worker % channels.len()].clone();
        let token = token.to_owned();
        let tenant = config.tenant.clone();
        let bucket = config.bucket.clone();
        let mutable_records = config.mutable_records;
        let expected_mutable_ids = expected_mutable_ids.clone();
        workers.spawn(async move {
            let mut objects = object_client(channel, &token)?;
            let mut entries = Vec::new();
            let mut id = worker as u64;
            while id < mutable_records {
                let path = data::mutable_path(id);
                let head = objects
                    .head_object(HeadObjectRequest {
                        address: Some(ObjectAddress {
                            tenant: tenant.clone(),
                            bucket: bucket.clone(),
                            path: path.clone(),
                        }),
                    })
                    .await?
                    .into_inner();
                match head.state.context("mutable head omitted state")? {
                    ObjectHeadState::Present(present) => entries.push((path, present.version)),
                    _ if expected_mutable_ids[id as usize] => {
                        bail!("acknowledged mutable authority path {path} is not present")
                    }
                    _ => {}
                }
                id = id
                    .checked_add(worker_count as u64)
                    .context("mutable authority worker index overflow")?;
            }
            Ok::<_, anyhow::Error>(entries)
        });
    }
    while let Some(completed) = workers.join_next().await {
        for (path, version) in completed.context("mutable authority worker panicked")?? {
            insert_authoritative_entry(&mut authority, path, version)?;
        }
    }
    ensure_authority_covers_expected(&authority, &expected_mutable_ids)?;
    Ok(Arc::new(authority))
}

pub(super) fn ensure_authority_covers_expected(
    authority: &BTreeMap<String, u64>,
    expected_mutable_ids: &[bool],
) -> Result<()> {
    let expected_len = expected_mutable_ids
        .iter()
        .filter(|expected| **expected)
        .count();
    ensure!(
        authority.len() >= expected_len,
        "mutable authority returned {} paths but {expected_len} acknowledged or preseeded paths are required",
        authority.len()
    );
    for (id, expected) in expected_mutable_ids.iter().enumerate() {
        if !expected {
            continue;
        }
        let expected_path = data::mutable_path(id as u64);
        ensure!(
            authority.contains_key(&expected_path),
            "acknowledged or preseeded mutable authority path {expected_path} is not present"
        );
    }
    Ok(())
}

pub(super) fn insert_authoritative_entry(
    authority: &mut BTreeMap<String, u64>,
    path: String,
    version: u64,
) -> Result<()> {
    ensure!(
        authority.insert(path.clone(), version).is_none(),
        "mutable authority returned duplicate path {path}"
    );
    Ok(())
}

pub(super) async fn verify_final_mutable_state(
    config: &Config,
    names: &[String],
    channels: &[Channel],
    token: &str,
    authority: Arc<BTreeMap<String, u64>>,
    counters: Arc<Counters>,
) -> Result<(bool, Option<bool>, BTreeSet<u64>)> {
    let deadline = Instant::now() + config.drain_timeout;
    let mut nodes = BTreeSet::new();
    let mut all_observed_tails_available = true;
    let mut next = names.iter().cloned().enumerate();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < config.query_max_in_flight {
            let Some((position, name)) = next.next() else {
                break;
            };
            let channel = channels[position % channels.len()].clone();
            let token = token.to_owned();
            let bucket = config.bucket.clone();
            let visibility_poll = config.visibility_poll;
            let request_timeout = config.request_timeout;
            let expected_source_count = config.endpoints.len();
            let authority = authority.clone();
            let counters = counters.clone();
            tasks.spawn(async move {
                verify_one_final_definition(
                    channel,
                    token,
                    bucket,
                    name,
                    authority,
                    deadline,
                    visibility_poll,
                    request_timeout,
                    expected_source_count,
                    counters,
                )
                .await
            });
        }
        let Some(completed) = tasks.join_next().await else {
            break;
        };
        let (observed_tails_available, source_nodes) =
            completed.context("final mutable verification task panicked")??;
        all_observed_tails_available &= observed_tails_available;
        nodes.extend(source_nodes);
    }
    Ok((true, all_observed_tails_available.then_some(true), nodes))
}

#[allow(clippy::too_many_arguments)]
async fn verify_one_final_definition(
    channel: Channel,
    token: String,
    bucket: String,
    name: String,
    authority: Arc<BTreeMap<String, u64>>,
    deadline: Instant,
    visibility_poll: Duration,
    request_timeout: Duration,
    expected_source_count: usize,
    counters: Arc<Counters>,
) -> Result<(bool, BTreeSet<u64>)> {
    let mut client = index_client(channel, &token)?;
    let mut last_observation = "no query attempt completed".to_owned();
    let mut last_successful_observation = None::<String>;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(
            !remaining.is_zero(),
            "index {name} did not converge to authoritative mutable state and zero lag; last observation: {last_observation}"
        );
        let response = paginated_class_query(
            &mut client,
            &bucket,
            &name,
            "mutable",
            deadline,
            request_timeout,
            counters.clone(),
        )
        .await;
        match response {
            Ok(response) => {
                let exact = response.hits == *authority;
                last_observation = mutable_query_observation(&response, &authority);
                last_successful_observation = Some(last_observation.clone());
                if let Some(freshness) = response.freshness {
                    let source_ids = freshness
                        .sources
                        .iter()
                        .map(|source| source.node_id)
                        .collect::<BTreeSet<_>>();
                    let healthy = freshness.initial_build_complete
                        && !freshness.rebuilding
                        && freshness.sources.len() == expected_source_count
                        && source_ids.len() == expected_source_count
                        && freshness
                            .sources
                            .iter()
                            .all(|source| source.node_id != 0 && source.source_epoch.len() == 32);
                    let observed_tails_available = freshness
                        .sources
                        .iter()
                        .all(|source| source.observed_tail.is_some());
                    let no_observed_lag = freshness.sources.iter().all(source_has_no_observed_lag);
                    if exact && healthy && no_observed_lag {
                        return Ok((observed_tails_available, source_ids));
                    }
                }
            }
            Err(error) => {
                last_observation = match &last_successful_observation {
                    Some(successful) => format!(
                        "query failed: {error:#}; last successful observation: {successful}"
                    ),
                    None => format!("query failed: {error:#}"),
                };
            }
        }
        tokio::time::sleep(visibility_poll).await;
    }
}

fn mutable_query_observation(
    response: &PaginatedQueryResult,
    authority: &BTreeMap<String, u64>,
) -> String {
    let first_difference = authority
        .iter()
        .find_map(|(path, version)| match response.hits.get(path) {
            None => Some(format!("missing {path}@{version}")),
            Some(actual) if actual != version => {
                Some(format!("version {path}: expected {version}, got {actual}"))
            }
            Some(_) => None,
        })
        .or_else(|| {
            response
                .hits
                .iter()
                .find(|(path, _)| !authority.contains_key(*path))
                .map(|(path, version)| format!("extra {path}@{version}"))
        })
        .unwrap_or_else(|| "none".into());
    let freshness = response.freshness.as_ref().map_or_else(
        || "absent".into(),
        |freshness| {
            format!(
                "commit={}, build_complete={}, rebuilding={}, sources={}, max_lag={}",
                freshness.commit_revision,
                freshness.initial_build_complete,
                freshness.rebuilding,
                freshness.sources.len(),
                freshness
                    .sources
                    .iter()
                    .map(|source| source.lag_hint)
                    .max()
                    .unwrap_or(0)
            )
        },
    );
    format!(
        "hits={}/{}, first_difference={first_difference}, freshness=[{freshness}]",
        response.hits.len(),
        authority.len()
    )
}

#[derive(Default)]
pub(super) struct PaginatedQueryResult {
    pub(super) hits: BTreeMap<String, u64>,
    freshness: Option<IndexFreshness>,
    seen_page_tokens: BTreeSet<Vec<u8>>,
}

impl PaginatedQueryResult {
    pub(super) fn absorb(&mut self, response: QueryIndexResponse) -> Result<Option<Vec<u8>>> {
        let freshness = response
            .freshness
            .context("paginated mutable query omitted freshness")?;
        if let Some(previous) = self.freshness.as_ref() {
            ensure!(
                freshness.commit_revision == previous.commit_revision
                    && freshness.authorization_revision == previous.authorization_revision
                    && freshness.index_id == previous.index_id
                    && freshness.definition_version == previous.definition_version,
                "paginated mutable query changed its pinned revision"
            );
        }
        for hit in response.hits {
            let path = hit
                .address
                .context("mutable query hit omitted address")?
                .path;
            ensure!(
                self.hits.insert(path.clone(), hit.object_version).is_none(),
                "paginated mutable query returned duplicate path {path}"
            );
        }
        self.freshness = Some(freshness);
        if response.next_page_token.is_empty() {
            return Ok(None);
        }
        ensure!(
            self.seen_page_tokens
                .insert(response.next_page_token.clone()),
            "paginated mutable query repeated a continuation token"
        );
        Ok(Some(response.next_page_token))
    }
}

async fn paginated_class_query(
    client: &mut IndexClient,
    bucket: &str,
    index_name: &str,
    class: &str,
    deadline: Instant,
    request_timeout: Duration,
    counters: Arc<Counters>,
) -> Result<PaginatedQueryResult> {
    counters.pagination_attempt_started();
    let traversal_started = Instant::now();
    let value = serde_json::to_vec(class)?;
    let mut page_token = Vec::new();
    let mut result = PaginatedQueryResult::default();
    for page_ordinal in 1usize.. {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            counters.pagination_attempt_failed(traversal_started.elapsed());
            bail!("paginated mutable query exceeded its verification deadline");
        }
        let page_timeout = remaining.min(request_timeout);
        let page_started = Instant::now();
        let response = tokio::time::timeout(
            page_timeout,
            query_page(
                client,
                bucket,
                index_name,
                "probe",
                value.clone(),
                FINAL_VERIFICATION_PAGE_SIZE,
                page_token,
            ),
        )
        .await;
        let response = match response {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                counters.pagination_page_failed(page_started.elapsed());
                counters.pagination_attempt_failed(traversal_started.elapsed());
                return Err(anyhow::anyhow!(
                    "page {page_ordinal} RPC failed after {:?}; completed_pages={}, accumulated_hits={}, traversal_elapsed={:?}: {error:#}",
                    page_started.elapsed(),
                    page_ordinal - 1,
                    result.hits.len(),
                    traversal_started.elapsed(),
                ));
            }
            Err(_) => {
                counters.pagination_page_failed(page_started.elapsed());
                counters.pagination_attempt_failed(traversal_started.elapsed());
                return Err(anyhow::anyhow!(
                    "page {page_ordinal} exceeded its {:?} request timeout; completed_pages={}, accumulated_hits={}, traversal_elapsed={:?}",
                    page_timeout,
                    page_ordinal - 1,
                    result.hits.len(),
                    traversal_started.elapsed(),
                ));
            }
        };
        let next_page_token = match result.absorb(response) {
            Ok(next_page_token) => next_page_token,
            Err(error) => {
                counters.pagination_page_failed(page_started.elapsed());
                counters.pagination_attempt_failed(traversal_started.elapsed());
                return Err(error.context(format!(
                    "page {page_ordinal} response validation failed after {:?}; completed_pages={}, accumulated_hits={}, traversal_elapsed={:?}",
                    page_started.elapsed(),
                    page_ordinal - 1,
                    result.hits.len(),
                    traversal_started.elapsed(),
                )));
            }
        };
        let Some(next_page_token) = next_page_token else {
            counters.pagination_page_completed(
                page_ordinal,
                result.hits.len(),
                page_started.elapsed(),
            );
            counters.pagination_attempt_completed(traversal_started.elapsed());
            return Ok(result);
        };
        counters.pagination_page_completed(page_ordinal, result.hits.len(), page_started.elapsed());
        page_token = next_page_token;
    }
    unreachable!("unbounded page ordinal iterator ended")
}

pub(super) fn source_has_no_observed_lag(source: &IndexSourceFreshness) -> bool {
    source.lag_hint == 0
        && source
            .observed_tail
            .is_none_or(|tail| tail.checked_add(1) == Some(source.indexed_next_offset))
}
