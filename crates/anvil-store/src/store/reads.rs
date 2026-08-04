use super::*;

impl Store {
    pub fn head(&self, key: &ObjectKey) -> Result<Option<Head>, MutationError> {
        self.head_by_storage_key(&self.head_storage_key(key)?)
    }

    pub(crate) fn head_by_storage_key(
        &self,
        encoded_key: &[u8],
    ) -> Result<Option<Head>, MutationError> {
        self.read_json(CF_HEADS, encoded_key)
    }

    /// Lists current live paths directly from the prefix-sortable head keys.
    /// No listing projection or side index is maintained: the iterator seeks
    /// to `[format][tenant ID][bucket ID][literal prefix]` and stops as soon as
    /// that byte prefix no longer matches.
    pub fn list_objects(
        &self,
        tenant: &str,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ListObjectsPage, MutationError> {
        let limit = limit.min(MAX_LIST_OBJECTS);
        if limit == 0 {
            return Ok(ListObjectsPage {
                paths: Vec::new(),
                has_more: false,
            });
        }

        let identity = self.resolve_bucket_identity(tenant, bucket)?;
        self.list_local_owned_objects(
            identity.tenant_id.0,
            identity.bucket_id.0,
            prefix,
            start_after,
            limit,
            |_, _, _| true,
        )
    }

    /// Lists the current live heads held by this node for one stable bucket.
    ///
    /// The caller supplies the fenced placement decision for each exact path.
    /// Keeping placement outside the storage kernel lets the same prefix scan
    /// serve both a one-node store and one source of a cluster-wide merge.
    pub fn list_local_owned_objects(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        mut is_local_rank_zero: impl FnMut(u64, u64, &str) -> bool,
    ) -> Result<ListObjectsPage, MutationError> {
        self.list_local_owned_objects_with_scope(
            tenant_id,
            bucket_id,
            prefix,
            start_after,
            limit,
            ReservedListScope::Public,
            &mut is_local_rank_zero,
        )
    }

    /// Narrow internal scan for immutable index definitions only.
    ///
    /// This deliberately cannot enumerate any other reserved namespace.
    pub fn list_local_owned_index_definitions(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        mut is_local_rank_zero: impl FnMut(u64, u64, &str) -> bool,
    ) -> Result<ListObjectsPage, MutationError> {
        if !valid_index_definition_prefix(prefix)
            || start_after.is_some_and(|path| !is_index_definition_path(path))
        {
            return Err(MutationError::InvalidObjectMutation(
                "index-definition listing is restricted to its exact reserved namespace".into(),
            ));
        }
        self.list_local_owned_objects_with_scope(
            tenant_id,
            bucket_id,
            prefix,
            start_after,
            limit,
            ReservedListScope::IndexDefinitions,
            &mut is_local_rank_zero,
        )
    }

    /// Narrow internal scan for published PersonalDB group manifests only.
    /// Hidden preparation objects and every other reserved namespace remain
    /// impossible to enumerate through this entry point.
    pub fn list_local_owned_personaldb_manifests(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        mut is_local_rank_zero: impl FnMut(u64, u64, &str) -> bool,
    ) -> Result<ListObjectsPage, MutationError> {
        if prefix != PERSONALDB_MANIFEST_PREFIX
            || start_after.is_some_and(|path| !is_personaldb_manifest_path(path))
        {
            return Err(MutationError::InvalidObjectMutation(
                "PersonalDB listing is restricted to published group manifests".into(),
            ));
        }
        self.list_local_owned_objects_with_scope(
            tenant_id,
            bucket_id,
            prefix,
            start_after,
            limit,
            ReservedListScope::PersonalDbManifests,
            &mut is_local_rank_zero,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn list_local_owned_objects_with_scope(
        &self,
        tenant_id: u64,
        bucket_id: u64,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
        reserved_scope: ReservedListScope,
        is_local_rank_zero: &mut impl FnMut(u64, u64, &str) -> bool,
    ) -> Result<ListObjectsPage, MutationError> {
        let limit = limit.min(MAX_LIST_OBJECTS);
        if limit == 0 {
            return Ok(ListObjectsPage {
                paths: Vec::new(),
                has_more: false,
            });
        }

        let identity = BucketIdentity {
            tenant_id: TenantId(tenant_id),
            bucket_id: BucketId(bucket_id),
        };
        let bucket_prefix = identity.encode();
        let mut range_prefix = Vec::with_capacity(bucket_prefix.len() + prefix.len());
        range_prefix.extend_from_slice(&bucket_prefix);
        range_prefix.extend_from_slice(prefix.as_bytes());
        let mut seek = range_prefix.clone();
        if let Some(cursor) = start_after
            && cursor.as_bytes() > prefix.as_bytes()
        {
            seek.truncate(bucket_prefix.len());
            seek.extend_from_slice(cursor.as_bytes());
        }
        let snapshot = self.db.snapshot();
        let mut paths = Vec::with_capacity(limit.saturating_add(1));
        for entry in snapshot.iterator_cf(
            self.cf(CF_HEADS)?,
            IteratorMode::From(&seek, Direction::Forward),
        ) {
            let (stored_key, encoded_head) = entry.map_err(storage_error)?;
            if !stored_key.starts_with(&range_prefix) {
                break;
            }
            let path = identity
                .decode_head_path(&stored_key)
                .map_err(storage_error)?;
            let head = serde_json::from_slice::<Head>(&encoded_head).map_err(storage_error)?;
            if head.deleted
                || (contains_reserved_anvil_segment(path) && !reserved_scope.allows(path))
                || start_after.is_some_and(|cursor| path <= cursor)
                || !is_local_rank_zero(tenant_id, bucket_id, path)
            {
                continue;
            }
            paths.push(path.to_owned());
            if paths.len() > limit {
                break;
            }
        }

        let has_more = paths.len() > limit;
        paths.truncate(limit);
        Ok(ListObjectsPage { paths, has_more })
    }

    /// Returns the last durable offset in this store's local invalidation
    /// journal. Zero means that no ordinary or atomic head change has been
    /// appended.
    pub async fn get(&self, key: &ObjectKey) -> Result<Option<Object>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        let selected = {
            let _commit_guard = self.commit_lock.lock().await;
            let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
                return Ok(None);
            };
            if head.deleted {
                return Ok(None);
            }
            let version = self
                .version_metadata_by_identity(identity, key, head.version)?
                .ok_or_else(|| {
                    MutationError::Storage("head references a missing version descriptor".into())
                })?;
            validate_selected_head(&head, &version)?;
            version
        };
        self.materialize_selected_object(key, selected)
            .await
            .map(Some)
    }

    pub async fn get_version(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Object>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let selected = {
            let _commit_guard = self.commit_lock.lock().await;
            let selected = self.version_metadata_by_identity(identity, key, version_id)?;
            if let Some(version) = &selected {
                validate_selected_version_id(version_id, version)?;
            }
            selected
        };
        match selected {
            Some(version) => self
                .materialize_selected_object(key, version)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    async fn materialize_selected_object(
        &self,
        key: &ObjectKey,
        version: Version,
    ) -> Result<Object, MutationError> {
        let bytes = match (&version.blob, version.deleted) {
            (Some(blob), false) => self.read_blob_bytes(blob).await?,
            (None, true) => Vec::new(),
            _ => {
                return Err(MutationError::Storage(
                    "version has an invalid payload shape".into(),
                ));
            }
        };
        Ok(Object {
            key: key.clone(),
            version,
            bytes,
        })
    }

    pub fn version_metadata(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        self.version_metadata_by_identity(identity, key, version_id)
    }

    pub(crate) fn version_metadata_by_identity(
        &self,
        identity: BucketIdentity,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<Version>, MutationError> {
        self.read_json(CF_VERSIONS, &version_key(identity, key, version_id))
    }

    /// Returns the current descriptor without loading its payload.
    ///
    /// The head and descriptor are selected under the commit fence so an
    /// unversioned replacement cannot retire the descriptor between the two
    /// reads. This is the cheap metadata path used by `HeadObject`.
    pub async fn current_version_metadata(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        let _commit_guard = self.commit_lock.lock().await;
        let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
            return Ok(None);
        };
        let version = self
            .version_metadata_by_identity(identity, key, head.version)?
            .ok_or_else(|| {
                MutationError::Storage("head references a missing version descriptor".into())
            })?;
        validate_selected_head(&head, &version)?;
        Ok(Some(version))
    }

    pub async fn open_object(
        &self,
        key: &ObjectKey,
        requested_version: Option<VersionId>,
    ) -> Result<Option<OpenedObject>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if requested_version.is_some()
            && self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled
        {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let version = {
            let _commit_guard = self.commit_lock.lock().await;
            let (version_id, selected_head) = match requested_version {
                Some(version) => (version, None),
                None => match self.head_by_storage_key(&identity.head_key(key.path()))? {
                    Some(head) => (head.version, Some(head)),
                    None => return Ok(None),
                },
            };
            let Some(version) = self.version_metadata_by_identity(identity, key, version_id)?
            else {
                return if selected_head.is_some() {
                    Err(MutationError::Storage(
                        "head references a missing version descriptor".into(),
                    ))
                } else {
                    Ok(None)
                };
            };
            match &selected_head {
                Some(head) => validate_selected_head(head, &version)?,
                None => validate_selected_version_id(version_id, &version)?,
            }
            version
        };
        let reader = match version_blob_reference(&version)? {
            Some(reference) => Some(self.open_blob(&reference).await?),
            None => None,
        };
        Ok(Some(OpenedObject { version, reader }))
    }

    /// Lists retained descriptors for one exact path in ascending version
    /// order. `after` is exclusive and the store always applies its own cap.
    pub fn list_object_versions(
        &self,
        key: &ObjectKey,
        after: Option<VersionId>,
        limit: usize,
    ) -> Result<Vec<Version>, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let limit = limit.min(MAX_LIST_OBJECT_VERSIONS);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = version_prefix(identity, key);
        let start = after.map_or_else(
            || prefix.clone(),
            |version| version_key(identity, key, version),
        );
        let mut versions = Vec::with_capacity(limit);
        for entry in self.db.iterator_cf(
            self.cf(CF_VERSIONS)?,
            IteratorMode::From(&start, Direction::Forward),
        ) {
            let (stored_key, encoded) = entry.map_err(storage_error)?;
            if !stored_key.starts_with(&prefix) || stored_key.len() != prefix.len() + 8 {
                break;
            }
            let stored_id = VersionId(u64::from_be_bytes(
                stored_key[prefix.len()..]
                    .try_into()
                    .expect("retained version key length was checked"),
            ));
            if after.is_some_and(|after| stored_id <= after) {
                continue;
            }
            let version = serde_json::from_slice::<Version>(&encoded).map_err(storage_error)?;
            if version.id != stored_id {
                return Err(MutationError::Storage(
                    "retained version key and descriptor disagree".into(),
                ));
            }
            version_blob_reference(&version)?;
            versions.push(version);
            if versions.len() == limit {
                break;
            }
        }
        Ok(versions)
    }

    pub async fn delete_retained_version(
        &self,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<DeleteRetainedVersionOutcome, MutationError> {
        let identity = self.resolve_bucket_identity(key.tenant(), key.bucket())?;
        if self.bucket_versioning_by_key(&identity.encode())? != ObjectVersioning::Enabled {
            return Err(MutationError::ObjectVersioningNotEnabled);
        }
        let _policy_guard = self.policy_gate.read().await;
        let _path_guard = self.ordinary_locks.acquire(&[object_path(key)]).await;
        let _commit_guard = self.commit_lock.lock().await;
        let policy = self
            .bucket_policy_by_key(&identity.encode())?
            .unwrap_or_default();
        if policy.is_program_only(key.path()) && !is_program_definition_path(key.path()) {
            return Err(MutationError::ProgramConcurrencyViolation);
        }
        if policy.is_immutable(key.path()) || is_program_definition_path(key.path()) {
            return Err(MutationError::Immutable);
        }
        let Some(head) = self.head_by_storage_key(&identity.head_key(key.path()))? else {
            return Ok(DeleteRetainedVersionOutcome::NotFound);
        };
        let Some(target) = self.version_metadata_by_identity(identity, key, version_id)? else {
            if head.version == version_id {
                return Err(MutationError::Storage(
                    "head references a missing retained version".into(),
                ));
            }
            return Ok(DeleteRetainedVersionOutcome::NotFound);
        };
        if target.id != version_id || target.deleted != (target.blob.is_none()) {
            return Err(MutationError::Storage(
                "retained version descriptor is malformed".into(),
            ));
        }

        let now = now_unix_millis()?;
        let mut batch = WriteBatch::default();
        let mut pending_references = PendingBlobReferences::new();
        let mut reference_deltas = Vec::new();
        if let Some(reference) = version_blob_reference(&target)? {
            let (reference_key, state) =
                self.prepare_blob_reference_retirement(&reference, &pending_references, now)?;
            self.stage_blob_reference_update(
                &mut batch,
                &mut pending_references,
                reference_key,
                state,
            )?;
            reference_deltas.push(ReferenceDelta {
                blob: reference,
                change: -1,
            });
        }
        batch.delete_cf(
            self.cf(CF_VERSIONS)?,
            version_key(identity, key, version_id),
        );

        let (outcome, resulting_head_version, head_change) = if head.version != version_id {
            (DeleteRetainedVersionOutcome::DeletedNonCurrent, None, None)
        } else {
            if target.deleted {
                return Err(MutationError::CurrentTombstoneCannotBeDeleted);
            }
            let tombstone_id = self.clock.next().map_err(storage_error)?;
            let tombstone = Version {
                id: tombstone_id,
                blob: None,
                content_type: None,
                deleted: true,
                committed_at_unix_millis: now,
            };
            batch.put_cf(
                self.cf(CF_VERSIONS)?,
                version_key(identity, key, tombstone_id),
                serde_json::to_vec(&tombstone).map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_HEADS)?,
                identity.head_key(key.path()),
                serde_json::to_vec(&Head {
                    version: tombstone_id,
                    deleted: true,
                    mutation_stamp: None,
                })
                .map_err(storage_error)?,
            );
            batch.put_cf(
                self.cf(CF_METADATA)?,
                VERSION_HIGH_WATERMARK_KEY,
                serde_json::to_vec(&tombstone_id).map_err(storage_error)?,
            );
            (
                DeleteRetainedVersionOutcome::ReplacedCurrentWithTombstone {
                    version: tombstone_id,
                },
                Some(tombstone_id),
                Some(PendingLocalChange::ObjectHead {
                    identity,
                    exact_path: key.path().to_owned(),
                    path_version: tombstone_id,
                    deleted: true,
                    reference_deltas: Vec::new(),
                    // The retained-version event carries the live-head
                    // decrement.  Mark the companion tombstone event as an
                    // explicit no-op so accounting consumers do not mistake
                    // it for an old journal entry that lacks transition
                    // evidence and unnecessarily rebuild their baseline.
                    accounting_transition: Some(AccountingHeadTransition::new(None, None)),
                }),
            )
        };
        let mut changes = vec![PendingLocalChange::RetainedVersionDeleted {
            identity,
            exact_path: key.path().to_owned(),
            deleted_version: version_id,
            resulting_head_version,
            reference_deltas,
            accounting_transition: Some(if resulting_head_version.is_some() {
                AccountingHeadTransition::new(target.blob.as_ref().map(|blob| blob.length), None)
            } else {
                AccountingHeadTransition::new(None, None)
            }),
        }];
        if let Some(head_change) = head_change {
            changes.push(head_change);
        }
        self.stage_local_changes(&mut batch, &changes)?;
        let mut options = WriteOptions::default();
        options.set_sync(self.sync_writes);
        self.db.write_opt(batch, &options).map_err(storage_error)?;
        self.notify_local_invalidations();
        Ok(outcome)
    }

    /// Resolves every requested head and immutable version descriptor from one
    /// local RocksDB snapshot without reading referenced blob payloads.
    pub async fn select_batch_get(
        &self,
        requests: &[(ObjectKey, Option<VersionId>)],
    ) -> BatchGetSelection {
        let commit_guard = self.commit_lock.lock().await;
        let entries = {
            let snapshot = self.db.snapshot();
            let mut identity_cache =
                BTreeMap::<(String, String), Result<BucketIdentity, MutationError>>::new();
            let mut entries = Vec::with_capacity(requests.len());
            for (key, requested_version) in requests {
                let cache_key = (key.tenant().to_owned(), key.bucket().to_owned());
                let identity = identity_cache
                    .entry(cache_key)
                    .or_insert_with(|| self.resolve_bucket_identity(key.tenant(), key.bucket()))
                    .clone();
                let selected = identity.and_then(|identity| {
                    let selected_head = match requested_version {
                        Some(_) => {
                            if self.bucket_versioning_by_key(&identity.encode())?
                                != ObjectVersioning::Enabled
                            {
                                return Err(MutationError::ObjectVersioningNotEnabled);
                            }
                            None
                        }
                        None => snapshot
                            .get_cf(self.cf(CF_HEADS)?, identity.head_key(key.path()))
                            .map_err(storage_error)?
                            .map(|bytes| {
                                serde_json::from_slice::<Head>(&bytes).map_err(storage_error)
                            })
                            .transpose()?,
                    };
                    let version_id = requested_version
                        .as_ref()
                        .copied()
                        .or_else(|| selected_head.as_ref().map(|head| head.version));
                    let Some(version_id) = version_id else {
                        return Ok(None);
                    };
                    let selected = snapshot
                        .get_cf(
                            self.cf(CF_VERSIONS)?,
                            version_key(identity, key, version_id),
                        )
                        .map_err(storage_error)?
                        .map(|bytes| {
                            serde_json::from_slice::<Version>(&bytes).map_err(storage_error)
                        })
                        .transpose()?;
                    let Some(version) = selected else {
                        return if selected_head.is_some() {
                            Err(MutationError::Storage(
                                "head references a missing version descriptor".into(),
                            ))
                        } else {
                            Ok(None)
                        };
                    };
                    match &selected_head {
                        Some(head) => validate_selected_head(head, &version)?,
                        None => validate_selected_version_id(version_id, &version)?,
                    }
                    Ok(Some(version))
                });
                entries.push((key.clone(), selected));
            }
            entries
        };
        drop(commit_guard);
        BatchGetSelection { entries }
    }

    /// Reads payloads for descriptors previously selected by
    /// [`Store::select_batch_get`]. Immutable descriptors are materialised
    /// after the short commit fence has already been released.
    pub async fn read_batch_get_selection(
        &self,
        selection: BatchGetSelection,
    ) -> Vec<Result<Option<Object>, MutationError>> {
        let BatchGetSelection { entries } = selection;
        let mut outcomes = Vec::with_capacity(entries.len());
        for (key, version) in entries {
            let outcome = match version {
                Ok(Some(version)) => match (&version.blob, version.deleted) {
                    (Some(blob), false) => self
                        .read_blob_bytes(blob)
                        .await
                        .map(|bytes| {
                            Some(Object {
                                key,
                                version,
                                bytes,
                            })
                        })
                        .map_err(storage_error),
                    (None, true) => Ok(Some(Object {
                        key,
                        version,
                        bytes: Vec::new(),
                    })),
                    _ => Err(MutationError::Storage(
                        "version has an invalid payload shape".into(),
                    )),
                },
                Ok(None) => Ok(None),
                Err(error) => Err(error),
            };
            outcomes.push(outcome);
        }
        outcomes
    }

    /// Resolves one snapshot and materialises its selected payloads.
    pub async fn batch_get(
        &self,
        requests: &[(ObjectKey, Option<VersionId>)],
    ) -> Vec<Result<Option<Object>, MutationError>> {
        let selection = self.select_batch_get(requests).await;
        self.read_batch_get_selection(selection).await
    }
}

const INDEX_DEFINITION_PREFIX: &str = "_anvil/indexes/definitions/";
const PERSONALDB_MANIFEST_PREFIX: &str = "_anvil/personaldb/v1/";

#[derive(Clone, Copy)]
enum ReservedListScope {
    Public,
    IndexDefinitions,
    PersonalDbManifests,
}

impl ReservedListScope {
    fn allows(self, path: &str) -> bool {
        match self {
            Self::Public => false,
            Self::IndexDefinitions => is_index_definition_path(path),
            Self::PersonalDbManifests => is_personaldb_manifest_path(path),
        }
    }
}

fn valid_index_definition_prefix(prefix: &str) -> bool {
    prefix
        .strip_prefix(INDEX_DEFINITION_PREFIX)
        .is_some_and(|suffix| !suffix.contains('/') && !suffix.contains('\0'))
}

fn is_index_definition_path(path: &str) -> bool {
    path.strip_prefix(INDEX_DEFINITION_PREFIX)
        .is_some_and(|name| {
            !name.is_empty()
                && name != "."
                && name != ".."
                && !name.contains('/')
                && !name.contains('\0')
                && !name.chars().any(char::is_control)
        })
}

fn is_personaldb_manifest_path(path: &str) -> bool {
    let Some(remainder) = path
        .strip_prefix(PERSONALDB_MANIFEST_PREFIX)
        .and_then(|path| path.strip_suffix("/manifest.json"))
    else {
        return false;
    };
    let Some((database, group)) = remainder.split_once('/') else {
        return false;
    };
    !database.is_empty()
        && !group.is_empty()
        && !group.contains('/')
        && database.len() <= 256
        && group.len() <= 256
        && database.len() % 2 == 0
        && group.len() % 2 == 0
        && database.bytes().all(|byte| byte.is_ascii_hexdigit())
        && group.bytes().all(|byte| byte.is_ascii_hexdigit())
}
