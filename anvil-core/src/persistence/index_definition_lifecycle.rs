use super::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, LazyLock, Mutex as StdMutex, Weak},
    time::Duration as StdDuration,
};
use tokio::sync::Mutex;

const INDEX_DEFINITION_MUTATION_ATTEMPTS: usize = 64;

type IndexDefinitionScope = (i64, i64);

static INDEX_DEFINITION_WRITE_LOCKS: LazyLock<
    StdMutex<BTreeMap<IndexDefinitionScope, Weak<Mutex<()>>>>,
> = LazyLock::new(|| StdMutex::new(BTreeMap::new()));

#[derive(Debug, Clone)]
pub enum IndexDefinitionMutation {
    Create {
        name: String,
        kind: String,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: String,
        build_policy: JsonValue,
    },
    Update {
        name: String,
        expected_kind: String,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: String,
        build_policy: JsonValue,
    },
    Disable {
        name: String,
    },
    Drop {
        name: String,
    },
}

#[derive(Debug)]
pub enum IndexDefinitionMutationOutcome {
    Published {
        index: IndexDefinition,
        event: IndexDefinitionEvent,
    },
    NotFound,
    AlreadyExists,
    KindChanged,
}

enum PreparedIndexDefinitionMutation {
    Publish {
        index: IndexDefinition,
        event_type: &'static str,
    },
    NotFound,
    AlreadyExists,
    KindChanged,
}

impl Persistence {
    pub async fn apply_index_definition_mutation(
        &self,
        bucket: &Bucket,
        mutation: &IndexDefinitionMutation,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<IndexDefinitionMutationOutcome> {
        let write_lock = index_definition_write_lock(bucket.tenant_id, bucket.id)?;
        let _write_guard = write_lock.lock().await;
        let attempts = if transaction_id.is_some() {
            1
        } else {
            INDEX_DEFINITION_MUTATION_ATTEMPTS
        };

        for attempt in 0..attempts {
            let prepared = prepare_index_definition_mutation(
                &self.storage,
                bucket.tenant_id,
                bucket.id,
                mutation,
            )
            .await?;
            let (index, event_type) = match prepared {
                PreparedIndexDefinitionMutation::Publish { index, event_type } => {
                    (index, event_type)
                }
                PreparedIndexDefinitionMutation::NotFound => {
                    return Ok(IndexDefinitionMutationOutcome::NotFound);
                }
                PreparedIndexDefinitionMutation::AlreadyExists => {
                    return Ok(IndexDefinitionMutationOutcome::AlreadyExists);
                }
                PreparedIndexDefinitionMutation::KindChanged => {
                    return Ok(IndexDefinitionMutationOutcome::KindChanged);
                }
            };

            match self
                .create_index_definition_event_with_transaction(
                    bucket.tenant_id,
                    bucket.id,
                    &bucket.name,
                    &index,
                    event_type,
                    transaction_id,
                    transaction_principal,
                )
                .await
            {
                Ok(event) => {
                    return Ok(IndexDefinitionMutationOutcome::Published { index, event });
                }
                Err(error)
                    if transaction_id.is_none()
                        && attempt + 1 < attempts
                        && is_retryable_index_definition_conflict(&error) =>
                {
                    tokio::task::yield_now().await;
                    tokio::time::sleep(StdDuration::from_millis(
                        1 + u64::try_from(attempt % 8).unwrap_or_default(),
                    ))
                    .await;
                }
                Err(error) => return Err(error),
            }
        }

        Err(anyhow!(
            "index definition mutation exhausted its retry budget"
        ))
    }
}

async fn prepare_index_definition_mutation(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    mutation: &IndexDefinitionMutation,
) -> Result<PreparedIndexDefinitionMutation> {
    match mutation {
        IndexDefinitionMutation::Create {
            name,
            kind,
            selector,
            extractor,
            authorization_mode,
            build_policy,
        } => {
            if index_journal::read_current_index_definition(storage, tenant_id, bucket_id, name)
                .await?
                .is_some()
            {
                return Ok(PreparedIndexDefinitionMutation::AlreadyExists);
            }
            let now = Utc::now();
            Ok(PreparedIndexDefinitionMutation::Publish {
                index: IndexDefinition {
                    id: index_journal::next_index_definition_id(storage, tenant_id, bucket_id)
                        .await?,
                    tenant_id,
                    bucket_id,
                    name: name.clone(),
                    kind: kind.clone(),
                    selector: selector.clone(),
                    extractor: extractor.clone(),
                    authorization_mode: authorization_mode.clone(),
                    build_policy: build_policy.clone(),
                    enabled: true,
                    version: 1,
                    created_at: now,
                    updated_at: now,
                },
                event_type: "create",
            })
        }
        IndexDefinitionMutation::Update {
            name,
            expected_kind,
            selector,
            extractor,
            authorization_mode,
            build_policy,
        } => {
            let Some(mut index) =
                index_journal::read_current_index_definition(storage, tenant_id, bucket_id, name)
                    .await?
            else {
                return Ok(PreparedIndexDefinitionMutation::NotFound);
            };
            if index.kind != *expected_kind {
                return Ok(PreparedIndexDefinitionMutation::KindChanged);
            }
            index.selector = selector.clone();
            index.extractor = extractor.clone();
            index.authorization_mode = authorization_mode.clone();
            index.build_policy = build_policy.clone();
            index.version = index
                .version
                .checked_add(1)
                .ok_or_else(|| anyhow!("index definition version overflow"))?;
            index.updated_at = Utc::now();
            Ok(PreparedIndexDefinitionMutation::Publish {
                index,
                event_type: "update",
            })
        }
        IndexDefinitionMutation::Disable { name } => {
            let Some(mut index) =
                index_journal::read_current_index_definition(storage, tenant_id, bucket_id, name)
                    .await?
            else {
                return Ok(PreparedIndexDefinitionMutation::NotFound);
            };
            index.enabled = false;
            index.version = index
                .version
                .checked_add(1)
                .ok_or_else(|| anyhow!("index definition version overflow"))?;
            index.updated_at = Utc::now();
            Ok(PreparedIndexDefinitionMutation::Publish {
                index,
                event_type: "disable",
            })
        }
        IndexDefinitionMutation::Drop { name } => {
            let Some(index) =
                index_journal::read_current_index_definition(storage, tenant_id, bucket_id, name)
                    .await?
            else {
                return Ok(PreparedIndexDefinitionMutation::NotFound);
            };
            Ok(PreparedIndexDefinitionMutation::Publish {
                index,
                event_type: "drop",
            })
        }
    }
}

fn index_definition_write_lock(tenant_id: i64, bucket_id: i64) -> Result<Arc<Mutex<()>>> {
    let scope = (tenant_id, bucket_id);
    let mut locks = INDEX_DEFINITION_WRITE_LOCKS
        .lock()
        .map_err(|_| anyhow!("index definition write lock registry is poisoned"))?;
    if let Some(lock) = locks.get(&scope).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    locks.insert(scope, Arc::downgrade(&lock));
    Ok(lock)
}

fn is_retryable_index_definition_conflict(error: &anyhow::Error) -> bool {
    if crate::core_store::is_retryable_mutation_conflict(error) {
        return true;
    }
    error.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("index definition event cursor")
            || message.contains("index definition projection cursor")
            || message.contains("differs from durable stream head")
            || message.contains("index definition projection changed")
    })
}
