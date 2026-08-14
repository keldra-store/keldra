//! Isolated public definition-update and definition-delete qualification.

use anvil_storage::RawAdministrationClient;
use anvil_storage::v1::{
    CreateIndexRequest, DeleteIndexRequest, GetIndexRequest, IndexDefinition, UpdateIndexRequest,
};
use tokio::time::{Instant, sleep};
use tonic::Code;

use super::{
    CONTENT_TYPE, EngineCase, IndexClient, POLL_INTERVAL, TestResult, WAIT_LIMIT, create_bucket,
    invalid, retryable_transport,
};

const INITIAL_PREFIX: &str = "docs/";
const UPDATED_PREFIX: &str = "definition-lifecycle/";

pub(super) async fn qualify(
    administrators: &mut [RawAdministrationClient],
    clients: &mut [IndexClient],
    cases: &[EngineCase],
) -> TestResult<()> {
    if administrators.len() != clients.len() || clients.is_empty() {
        return Err(invalid(
            "definition lifecycle qualification requires matching public clients",
        ));
    }

    let endpoint_count = clients.len();
    for (position, case) in cases.iter().enumerate() {
        let bucket = lifecycle_bucket(case);
        create_bucket(&mut administrators[position % endpoint_count], &bucket).await?;

        let created = clients[position % endpoint_count]
            .create_index(CreateIndexRequest {
                bucket: bucket.clone(),
                name: case.name.into(),
                path_prefix: INITIAL_PREFIX.into(),
                content_type: CONTENT_TYPE.into(),
                specification: Some(case.specification.clone()),
                command_id: format!("qualification-lifecycle-create-{}", case.name),
            })
            .await?
            .into_inner();
        require_created(case, &bucket, &created)?;

        let updated = clients[(position + 1) % endpoint_count]
            .update_index(UpdateIndexRequest {
                bucket: bucket.clone(),
                name: case.name.into(),
                expected_version: created.version,
                path_prefix: UPDATED_PREFIX.into(),
                content_type: CONTENT_TYPE.into(),
                specification: Some(case.specification.clone()),
                command_id: format!("qualification-lifecycle-update-{}", case.name),
            })
            .await?
            .into_inner();
        require_updated(case, &bucket, &created, &updated)?;

        for client in clients.iter_mut() {
            wait_for_definition(client, case, &bucket, &updated).await?;
        }

        let deleted = clients[(position + 2) % endpoint_count]
            .delete_index(DeleteIndexRequest {
                bucket: bucket.clone(),
                name: case.name.into(),
                expected_version: updated.version,
                command_id: format!("qualification-lifecycle-delete-{}", case.name),
            })
            .await?
            .into_inner();
        if !deleted.deleted {
            return Err(invalid(format!(
                "{} lifecycle definition returned a non-delete response",
                case.name
            )));
        }

        for client in clients.iter_mut() {
            wait_for_absence(client, case, &bucket).await?;
        }
    }

    Ok(())
}

fn lifecycle_bucket(case: &EngineCase) -> String {
    format!("{}-definition-lifecycle", case.bucket)
}

fn require_created(
    case: &EngineCase,
    bucket: &str,
    definition: &IndexDefinition,
) -> TestResult<()> {
    if definition.index_id == 0
        || definition.version == 0
        || definition.bucket != bucket
        || definition.name != case.name
        || definition.path_prefix != INITIAL_PREFIX
        || definition.content_type != CONTENT_TYPE
        || definition.specification.as_ref() != Some(&case.specification)
    {
        return Err(invalid(format!(
            "{} lifecycle create returned an inconsistent definition",
            case.name
        )));
    }
    Ok(())
}

fn require_updated(
    case: &EngineCase,
    bucket: &str,
    created: &IndexDefinition,
    updated: &IndexDefinition,
) -> TestResult<()> {
    if updated.index_id != created.index_id
        || updated.version <= created.version
        || updated.bucket != bucket
        || updated.name != case.name
        || updated.path_prefix != UPDATED_PREFIX
        || updated.content_type != CONTENT_TYPE
        || updated.kind != created.kind
        || updated.specification.as_ref() != Some(&case.specification)
    {
        return Err(invalid(format!(
            "{} lifecycle update changed immutable identity or omitted the requested definition",
            case.name
        )));
    }
    Ok(())
}

async fn wait_for_definition(
    client: &mut IndexClient,
    case: &EngineCase,
    bucket: &str,
    expected: &IndexDefinition,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match client
            .get_index(GetIndexRequest {
                bucket: bucket.into(),
                name: case.name.into(),
            })
            .await
        {
            Ok(response) if response.get_ref() == expected => return Ok(()),
            Ok(_) if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            Ok(_) => {
                return Err(invalid(format!(
                    "{} lifecycle update was not visible before the deadline",
                    case.name
                )));
            }
            Err(status)
                if (status.code() == Code::NotFound || retryable_transport(&status))
                    && Instant::now() < deadline =>
            {
                sleep(POLL_INTERVAL).await;
            }
            Err(status) => {
                return Err(invalid(format!(
                    "{} lifecycle update lookup failed: {status}",
                    case.name
                )));
            }
        }
    }
}

async fn wait_for_absence(
    client: &mut IndexClient,
    case: &EngineCase,
    bucket: &str,
) -> TestResult<()> {
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        match client
            .get_index(GetIndexRequest {
                bucket: bucket.into(),
                name: case.name.into(),
            })
            .await
        {
            Err(status) if status.code() == Code::NotFound => return Ok(()),
            Ok(_) if Instant::now() < deadline => sleep(POLL_INTERVAL).await,
            Err(status) if retryable_transport(&status) && Instant::now() < deadline => {
                sleep(POLL_INTERVAL).await;
            }
            Ok(_) => {
                return Err(invalid(format!(
                    "{} lifecycle definition remained visible after delete",
                    case.name
                )));
            }
            Err(status) => {
                return Err(invalid(format!(
                    "{} lifecycle delete lookup failed: {status}",
                    case.name
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{super::engine_cases, lifecycle_bucket};

    #[test]
    fn lifecycle_buckets_are_isolated_from_retained_index_cases() {
        let cases = engine_cases();
        let buckets = cases.iter().map(lifecycle_bucket).collect::<BTreeSet<_>>();
        assert_eq!(buckets.len(), cases.len());
        for case in &cases {
            assert_ne!(lifecycle_bucket(case), case.bucket);
        }
    }
}
