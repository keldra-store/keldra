use super::*;

const OBJECT_CURRENT_PAGE_FAMILY: &[u8] = b"object-page-current";
const OBJECT_KEY_CATALOG_FAMILY: &[u8] = b"object-page-key";
const OBJECT_VERSION_PAGE_FAMILY: &[u8] = b"object-page-version";
const OBJECT_VERSION_CATALOG_FAMILY: &[u8] = b"object-page-version-catalog";
const OBJECT_VERSION_ID_FAMILY: &[u8] = b"object-version-id";
const OBJECT_KEY_END: u64 = 0;
const OBJECT_KEY_AFTER: u64 = 1;
const OBJECT_KEY_BYTE_OFFSET: u64 = 2;
const MAX_OBJECT_METADATA_PAGE_ROWS: usize = CORE_META_MAX_SCAN_PAGE_ROWS - 1;
const MAX_OBJECT_METADATA_PUBLIC_PAGE_ROWS: usize = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectMetadataProjection {
    Current,
    CurrentCatalog,
    Versions,
    VersionCatalog,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectMetadataPageCursor {
    projection: ObjectMetadataProjection,
    source_root_key_hash: String,
    source_generation: u64,
    scan_prefix: Vec<u8>,
    after_tuple_key: Vec<u8>,
}

impl ObjectMetadataPageCursor {
    pub(crate) fn after_current_prefix(
        &self,
        bucket: &Bucket,
        object_prefix: &str,
    ) -> Result<Self> {
        if !matches!(
            self.projection,
            ObjectMetadataProjection::Current | ObjectMetadataProjection::CurrentCatalog
        ) || self.source_root_key_hash
            != object_metadata_root_key_hash(bucket.tenant_id, bucket.id)
        {
            bail!("ObjectMetadataPageCursorSourceMismatch");
        }
        let after_tuple_key =
            object_ordered_prefix_after(projection_family(self.projection), bucket, object_prefix);
        if !after_tuple_key.starts_with(&self.scan_prefix) {
            bail!("ObjectMetadataPageCursorSourceMismatch");
        }
        if after_tuple_key < self.after_tuple_key {
            bail!("object metadata page cursor cannot move backwards");
        }
        Ok(Self {
            projection: self.projection,
            source_root_key_hash: self.source_root_key_hash.clone(),
            source_generation: self.source_generation,
            scan_prefix: self.scan_prefix.clone(),
            after_tuple_key,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CurrentObjectMetadataPage {
    pub(crate) objects: Vec<Object>,
    pub(crate) next_cursor: Option<ObjectMetadataPageCursor>,
    pub(crate) source_generation: u64,
    pub(crate) candidates_visited: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectVersionsMetadataPage {
    pub(crate) versions: Vec<ObjectVersion>,
    pub(crate) next_cursor: Option<ObjectMetadataPageCursor>,
    pub(crate) source_generation: u64,
    pub(crate) candidates_visited: usize,
}

impl CoreStore {
    pub async fn read_current_object_metadata(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Result<Option<Object>> {
        self.read_current_object_metadata_with_generation(bucket, object_key, None)
            .await
    }

    pub async fn read_current_object_metadata_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        root_generation: u64,
    ) -> Result<Option<Object>> {
        self.read_current_object_metadata_with_generation(bucket, object_key, Some(root_generation))
            .await
    }

    async fn read_current_object_metadata_with_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        root_generation: Option<u64>,
    ) -> Result<Option<Object>> {
        let decoded = match root_generation {
            Some(root_generation) => self.read_current_object_metadata_row_at_generation(
                bucket,
                object_key,
                root_generation,
            )?,
            None => {
                let Some(bytes) = self.read_coremeta_row(
                    CF_OBJECT_HEADS,
                    TABLE_OBJECT_HEAD_ROW,
                    &object_current_key(bucket, object_key),
                )?
                else {
                    return Ok(None);
                };
                Some(decode_object_metadata_row_with_common(&bytes)?)
            }
        };
        let Some(decoded) = decoded else {
            return Ok(None);
        };
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.key != object_key {
            bail!("CoreStore object metadata current row key mismatch");
        }
        if decoded.object.deleted_at.is_some() {
            return Ok(None);
        }
        Ok(Some(decoded.object))
    }

    pub async fn read_object_version_metadata(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        self.read_object_version_metadata_with_generation(bucket, object_key, version_id, None)
            .await
    }

    pub async fn read_object_version_metadata_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
        root_generation: u64,
    ) -> Result<Option<Object>> {
        self.read_object_version_metadata_with_generation(
            bucket,
            object_key,
            version_id,
            Some(root_generation),
        )
        .await
    }

    async fn read_object_version_metadata_with_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
        root_generation: Option<u64>,
    ) -> Result<Option<Object>> {
        let decoded = match root_generation {
            Some(root_generation) => self.read_object_version_metadata_row_at_generation(
                bucket,
                object_key,
                version_id,
                root_generation,
            )?,
            None => {
                let Some(bytes) = self.read_coremeta_row(
                    CF_OBJECT_VERSIONS,
                    TABLE_OBJECT_VERSION_META_ROW,
                    &object_version_key(bucket, object_key, version_id),
                )?
                else {
                    return Ok(None);
                };
                Some(decode_object_metadata_row_with_common(&bytes)?)
            }
        };
        let Some(decoded) = decoded else {
            return Ok(None);
        };
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.key != object_key || decoded.object.version_id != version_id {
            bail!("CoreStore object metadata version row key mismatch");
        }
        if decoded.object.deleted_at.is_some()
            && (root_generation.is_some() || !decoded.delete_marker)
        {
            return Ok(None);
        }
        Ok(Some(decoded.object))
    }

    pub async fn read_object_version_metadata_by_id(
        &self,
        bucket: &Bucket,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bytes) = self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_id_key(bucket, version_id),
        )?
        else {
            return Ok(None);
        };
        let decoded = decode_object_metadata_row_with_common(&bytes)?;
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.version_id != version_id {
            bail!("CoreStore object metadata version-id row key mismatch");
        }
        if decoded.object.deleted_at.is_some() && !decoded.delete_marker {
            return Ok(None);
        }
        Ok(Some(decoded.object))
    }

    pub(crate) async fn list_current_object_metadata_page(
        &self,
        bucket: &Bucket,
        prefix: &str,
        start_after: &str,
        root_generation: Option<u64>,
        cursor: Option<&ObjectMetadataPageCursor>,
        limit: usize,
    ) -> Result<CurrentObjectMetadataPage> {
        validate_object_metadata_page_limit(limit)?;
        let _source_guard = if root_generation.is_none() {
            Some(
                self.acquire_named_lock(
                    "object-metadata-bucket",
                    &object_metadata_bucket_lock_id(bucket),
                )
                .await?,
            )
        } else {
            None
        };
        let observed_generation = self.object_metadata_source_generation(bucket).await?;
        let source_generation = root_generation.unwrap_or(observed_generation);
        let historical = root_generation.is_some();
        let projection = if historical {
            ObjectMetadataProjection::CurrentCatalog
        } else {
            ObjectMetadataProjection::Current
        };
        let family = projection_family(projection);
        let scan_prefix = object_ordered_prefix(family, bucket, prefix);
        let initial_position = match cursor {
            Some(cursor) => {
                validate_page_cursor(cursor, projection, bucket, source_generation, &scan_prefix)?;
                Some(cursor.after_tuple_key.clone())
            }
            None => current_start_position(family, bucket, prefix, start_after)?,
        };
        if root_generation.is_none() && observed_generation != source_generation {
            bail!("ObjectMetadataSourceChanged");
        }

        let mut rows = self.scan_coremeta_prefix_page(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            &scan_prefix,
            initial_position.as_deref(),
            limit.saturating_add(1),
        )?;
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let candidates_visited = rows.len();
        let last_tuple_key = rows
            .last()
            .map(|row| core_meta_record_tuple_key(&row.key).map(ToOwned::to_owned))
            .transpose()?;
        let mut objects = Vec::with_capacity(rows.len());
        for row in rows {
            let catalog_object = decode_object_metadata_row(&row.payload)?;
            validate_object_scope(bucket, &catalog_object)?;
            validate_current_page_row_key(family, bucket, &catalog_object, &row.key)?;
            let object = if historical {
                self.read_current_object_metadata_row_at_generation(
                    bucket,
                    &catalog_object.key,
                    source_generation,
                )?
                .and_then(|decoded| {
                    decoded
                        .object
                        .deleted_at
                        .is_none()
                        .then_some(decoded.object)
                })
            } else {
                Some(catalog_object)
            };
            if let Some(object) = object {
                objects.push(object);
            }
        }
        if root_generation.is_none()
            && self.object_metadata_source_generation(bucket).await? != source_generation
        {
            bail!("ObjectMetadataSourceChanged");
        }
        let next_cursor = if has_more {
            Some(ObjectMetadataPageCursor {
                projection,
                source_root_key_hash: object_metadata_root_key_hash(bucket.tenant_id, bucket.id),
                source_generation,
                scan_prefix: scan_prefix.clone(),
                after_tuple_key: last_tuple_key
                    .ok_or_else(|| anyhow!("object metadata current page cursor is missing"))?,
            })
        } else {
            None
        };
        Ok(CurrentObjectMetadataPage {
            objects,
            next_cursor,
            source_generation,
            candidates_visited,
        })
    }

    pub(crate) async fn list_object_versions_metadata_page(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        root_generation: Option<u64>,
        cursor: Option<&ObjectMetadataPageCursor>,
        limit: usize,
    ) -> Result<ObjectVersionsMetadataPage> {
        validate_object_metadata_page_limit(limit)?;
        let _source_guard = if root_generation.is_none() {
            Some(
                self.acquire_named_lock(
                    "object-metadata-bucket",
                    &object_metadata_bucket_lock_id(bucket),
                )
                .await?,
            )
        } else {
            None
        };
        let observed_generation = self.object_metadata_source_generation(bucket).await?;
        let source_generation = root_generation.unwrap_or(observed_generation);
        let historical = root_generation.is_some();
        let projection = if historical {
            ObjectMetadataProjection::VersionCatalog
        } else {
            ObjectMetadataProjection::Versions
        };
        let family = projection_family(projection);
        let scan_prefix = object_ordered_prefix(family, bucket, prefix);
        let initial_position = match cursor {
            Some(cursor) => {
                validate_page_cursor(cursor, projection, bucket, source_generation, &scan_prefix)?;
                Some(cursor.after_tuple_key.clone())
            }
            None => self.version_start_position(
                family,
                bucket,
                prefix,
                key_marker,
                version_id_marker,
                root_generation,
            )?,
        };

        let mut rows = self.scan_coremeta_prefix_page(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &scan_prefix,
            initial_position.as_deref(),
            limit.saturating_add(1),
        )?;
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let candidates_visited = rows.len();
        let last_tuple_key = rows
            .last()
            .map(|row| core_meta_record_tuple_key(&row.key).map(ToOwned::to_owned))
            .transpose()?;
        let mut versions = Vec::with_capacity(rows.len());
        let mut latest_version_ids = BTreeMap::<String, Option<uuid::Uuid>>::new();
        for row in rows {
            let catalog = decode_object_metadata_row_with_common(&row.payload)?;
            validate_object_scope(bucket, &catalog.object)?;
            validate_version_page_row_key(family, bucket, &catalog, &row.key)?;
            let decoded = if historical {
                self.read_object_version_metadata_row_at_generation(
                    bucket,
                    &catalog.object.key,
                    catalog.object.version_id,
                    source_generation,
                )?
            } else {
                Some(catalog)
            };
            let Some(decoded) = decoded else {
                continue;
            };
            if decoded.object.deleted_at.is_some() && !decoded.delete_marker {
                continue;
            }
            if !latest_version_ids.contains_key(&decoded.object.key) {
                let latest_version_id = match root_generation {
                    Some(root_generation) => self
                        .read_current_object_metadata_row_at_generation(
                            bucket,
                            &decoded.object.key,
                            root_generation,
                        )?
                        .map(|current| current.object.version_id),
                    None => self.latest_ordered_object_version_id(bucket, &decoded.object.key)?,
                };
                latest_version_ids.insert(decoded.object.key.clone(), latest_version_id);
            }
            versions.push(ObjectVersion {
                is_latest: latest_version_ids
                    .get(&decoded.object.key)
                    .copied()
                    .flatten()
                    .is_some_and(|version_id| version_id == decoded.object.version_id),
                is_delete_marker: decoded.delete_marker,
                object: decoded.object,
            });
        }
        if root_generation.is_none()
            && self.object_metadata_source_generation(bucket).await? != source_generation
        {
            bail!("ObjectMetadataSourceChanged");
        }
        let next_cursor = if has_more {
            Some(ObjectMetadataPageCursor {
                projection,
                source_root_key_hash: object_metadata_root_key_hash(bucket.tenant_id, bucket.id),
                source_generation,
                scan_prefix: scan_prefix.clone(),
                after_tuple_key: last_tuple_key
                    .ok_or_else(|| anyhow!("object metadata version page cursor is missing"))?,
            })
        } else {
            None
        };
        Ok(ObjectVersionsMetadataPage {
            versions,
            next_cursor,
            source_generation,
            candidates_visited,
        })
    }

    pub async fn list_current_object_metadata(&self, bucket: &Bucket) -> Result<Vec<Object>> {
        let mut objects = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .list_current_object_metadata_page(
                    bucket,
                    "",
                    "",
                    None,
                    cursor.as_ref(),
                    MAX_OBJECT_METADATA_PAGE_ROWS,
                )
                .await?;
            objects.extend(page.objects);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(objects)
    }

    pub async fn list_current_object_metadata_at_generation(
        &self,
        bucket: &Bucket,
        root_generation: u64,
    ) -> Result<Vec<Object>> {
        let mut objects = Vec::new();
        let mut cursor = None;
        loop {
            let page = self
                .list_current_object_metadata_page(
                    bucket,
                    "",
                    "",
                    Some(root_generation),
                    cursor.as_ref(),
                    MAX_OBJECT_METADATA_PAGE_ROWS,
                )
                .await?;
            objects.extend(page.objects);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(objects)
    }

    pub async fn list_object_versions_metadata(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage> {
        self.list_object_versions_metadata_with_generation(
            bucket,
            prefix,
            key_marker,
            version_id_marker,
            limit,
            None,
        )
        .await
    }

    pub async fn list_object_versions_metadata_at_generation(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
        root_generation: u64,
    ) -> Result<ObjectVersionsPage> {
        self.list_object_versions_metadata_with_generation(
            bucket,
            prefix,
            key_marker,
            version_id_marker,
            limit,
            Some(root_generation),
        )
        .await
    }

    async fn list_object_versions_metadata_with_generation(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
        root_generation: Option<u64>,
    ) -> Result<ObjectVersionsPage> {
        let limit = usize::try_from(limit.max(1))?.min(MAX_OBJECT_METADATA_PUBLIC_PAGE_ROWS);
        let target = limit.saturating_add(1);
        let mut versions = Vec::with_capacity(target);
        let mut cursor = None;
        let mut candidates_visited = 0_usize;
        loop {
            let remaining = MAX_OBJECT_METADATA_PAGE_ROWS.saturating_sub(candidates_visited);
            if remaining == 0 || versions.len() >= target {
                break;
            }
            let page = self
                .list_object_versions_metadata_page(
                    bucket,
                    prefix,
                    key_marker,
                    version_id_marker,
                    root_generation,
                    cursor.as_ref(),
                    remaining.min(target.saturating_sub(versions.len()).max(1)),
                )
                .await?;
            candidates_visited = candidates_visited.saturating_add(page.candidates_visited);
            versions.extend(page.versions);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        if cursor.is_some() && versions.len() < target {
            bail!("ObjectListingCandidateBudgetExceeded");
        }
        let is_truncated = versions.len() > limit || cursor.is_some();
        if versions.len() > limit {
            versions.truncate(limit);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            versions
                .last()
                .map(|version| {
                    (
                        Some(version.object.key.clone()),
                        Some(version.object.version_id),
                    )
                })
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        Ok(ObjectVersionsPage {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    pub(super) fn latest_object_version_for_key_after_delete(
        &self,
        bucket: &Bucket,
        object_key: &str,
        deleted_version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let prefix = object_ordered_full_key_prefix(OBJECT_VERSION_PAGE_FAMILY, bucket, object_key);
        for row in self.scan_coremeta_prefix_page(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &prefix,
            None,
            2,
        )? {
            let decoded = decode_object_metadata_row_with_common(&row.payload)?;
            validate_object_scope(bucket, &decoded.object)?;
            validate_version_page_row_key(OBJECT_VERSION_PAGE_FAMILY, bucket, &decoded, &row.key)?;
            if decoded.object.version_id != deleted_version_id {
                return Ok(Some(decoded.object));
            }
        }
        Ok(None)
    }

    fn read_current_object_metadata_row_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        root_generation: u64,
    ) -> Result<Option<DecodedObjectMetadataRow>> {
        let start = object_current_history_object_prefix(bucket, object_key);
        let end = object_current_history_range_end(bucket, object_key, root_generation);
        let Some(row) = self
            .scan_coremeta_object_range_reverse_inclusive(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                &start,
                &end,
                1,
            )?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let decoded = decode_object_metadata_row_with_common(&row.payload)?;
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.key != object_key || decoded.root_generation > root_generation {
            bail!("CoreStore current-object history row key mismatch");
        }
        Ok(Some(decoded))
    }

    fn read_object_version_metadata_row_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
        root_generation: u64,
    ) -> Result<Option<DecodedObjectMetadataRow>> {
        let start = object_version_history_object_prefix(bucket, object_key, version_id);
        let end = object_version_history_key(bucket, object_key, version_id, root_generation);
        let Some(row) = self
            .scan_coremeta_object_range_reverse_inclusive(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &start,
                &end,
                1,
            )?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let decoded = decode_object_metadata_row_with_common(&row.payload)?;
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.key != object_key
            || decoded.object.version_id != version_id
            || decoded.root_generation > root_generation
        {
            bail!("CoreStore object-version history row key mismatch");
        }
        Ok(Some(decoded))
    }

    async fn object_metadata_source_generation(&self, bucket: &Bucket) -> Result<u64> {
        self.current_object_metadata_root_generation(bucket).await
    }

    fn version_start_position(
        &self,
        family: &[u8],
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        root_generation: Option<u64>,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(version_id_marker) = version_id_marker {
            let marker = match root_generation {
                Some(root_generation) => self.read_object_version_metadata_row_at_generation(
                    bucket,
                    key_marker,
                    version_id_marker,
                    root_generation,
                )?,
                None => self
                    .read_coremeta_row(
                        CF_OBJECT_VERSIONS,
                        TABLE_OBJECT_VERSION_META_ROW,
                        &object_version_key(bucket, key_marker, version_id_marker),
                    )?
                    .map(|bytes| decode_object_metadata_row_with_common(&bytes))
                    .transpose()?,
            }
            .filter(|marker| marker.object.deleted_at.is_none() || marker.delete_marker);
            let Some(marker) = marker else {
                return Ok(Some(object_ordered_prefix_after(family, bucket, prefix)));
            };
            if !key_marker.starts_with(prefix) {
                return if key_marker < prefix {
                    Ok(None)
                } else {
                    Ok(Some(object_ordered_prefix_after(family, bucket, prefix)))
                };
            }
            return Ok(Some(object_version_page_key(
                family,
                bucket,
                &marker.object,
                marker.root_generation,
            )));
        }
        if key_marker.is_empty() || key_marker < prefix {
            return Ok(None);
        }
        if key_marker.starts_with(prefix) {
            return Ok(Some(object_ordered_after_key(family, bucket, key_marker)));
        }
        Ok(Some(object_ordered_prefix_after(family, bucket, prefix)))
    }

    fn latest_ordered_object_version_id(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Result<Option<uuid::Uuid>> {
        let prefix = object_ordered_full_key_prefix(OBJECT_VERSION_PAGE_FAMILY, bucket, object_key);
        let Some(row) = self
            .scan_coremeta_prefix_page(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &prefix,
                None,
                1,
            )?
            .into_iter()
            .next()
        else {
            return Ok(None);
        };
        let decoded = decode_object_metadata_row_with_common(&row.payload)?;
        validate_object_scope(bucket, &decoded.object)?;
        validate_version_page_row_key(OBJECT_VERSION_PAGE_FAMILY, bucket, &decoded, &row.key)?;
        Ok(Some(decoded.object.version_id))
    }

    fn scan_coremeta_object_range_reverse_inclusive(
        &self,
        cf: &'static str,
        table_id: u16,
        start_tuple_key: &[u8],
        end_tuple_key: &[u8],
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        if !(1..=CORE_META_MAX_SCAN_PAGE_ROWS).contains(&limit) {
            bail!(
                "CoreMeta visible reverse scan limit must be between 1 and {CORE_META_MAX_SCAN_PAGE_ROWS}"
            );
        }

        // The raw reverse iterator supplies ordering only. Every candidate is
        // re-read through the publication-aware point API before it can escape.
        let page = self.meta.scan_range_reverse_inclusive(
            cf,
            table_id,
            start_tuple_key,
            end_tuple_key,
            CORE_META_MAX_SCAN_PAGE_ROWS,
        )?;
        let physical_count = page.len();
        let mut visible = Vec::with_capacity(limit);
        for record in page {
            let tuple_key = core_meta_record_tuple_key(&record.key)?;
            if let Some(payload) = self.read_coremeta_row(cf, table_id, tuple_key)? {
                visible.push(CoreMetaRecord {
                    key: record.key,
                    payload,
                });
                if visible.len() == limit {
                    return Ok(visible);
                }
            }
        }
        if physical_count == CORE_META_MAX_SCAN_PAGE_ROWS {
            bail!("CoreMeta visible reverse range exhausted its bounded physical candidate budget");
        }
        Ok(visible)
    }
}

fn validate_object_metadata_page_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_OBJECT_METADATA_PAGE_ROWS).contains(&limit) {
        bail!("object metadata page size must be between 1 and {MAX_OBJECT_METADATA_PAGE_ROWS}");
    }
    Ok(())
}

fn validate_page_cursor(
    cursor: &ObjectMetadataPageCursor,
    projection: ObjectMetadataProjection,
    bucket: &Bucket,
    source_generation: u64,
    scan_prefix: &[u8],
) -> Result<()> {
    if cursor.projection != projection
        || cursor.source_root_key_hash != object_metadata_root_key_hash(bucket.tenant_id, bucket.id)
        || cursor.source_generation != source_generation
        || cursor.scan_prefix.as_slice() != scan_prefix
        || !cursor.after_tuple_key.starts_with(scan_prefix)
    {
        bail!("ObjectMetadataPageCursorSourceMismatch");
    }
    Ok(())
}

fn projection_family(projection: ObjectMetadataProjection) -> &'static [u8] {
    match projection {
        ObjectMetadataProjection::Current => OBJECT_CURRENT_PAGE_FAMILY,
        ObjectMetadataProjection::CurrentCatalog => OBJECT_KEY_CATALOG_FAMILY,
        ObjectMetadataProjection::Versions => OBJECT_VERSION_PAGE_FAMILY,
        ObjectMetadataProjection::VersionCatalog => OBJECT_VERSION_CATALOG_FAMILY,
    }
}

fn current_start_position(
    family: &[u8],
    bucket: &Bucket,
    prefix: &str,
    start_after: &str,
) -> Result<Option<Vec<u8>>> {
    if start_after.is_empty() || start_after < prefix {
        return Ok(None);
    }
    if start_after.starts_with(prefix) {
        return Ok(Some(object_current_page_key(family, bucket, start_after)));
    }
    Ok(Some(object_ordered_prefix_after(family, bucket, prefix)))
}

fn validate_current_page_row_key(
    family: &[u8],
    bucket: &Bucket,
    object: &Object,
    core_meta_key: &[u8],
) -> Result<()> {
    let tuple_key = core_meta_record_tuple_key(core_meta_key)?;
    if tuple_key != object_current_page_key(family, bucket, &object.key) {
        bail!("CoreStore current-object page row key mismatch");
    }
    Ok(())
}

fn validate_version_page_row_key(
    family: &[u8],
    bucket: &Bucket,
    decoded: &DecodedObjectMetadataRow,
    core_meta_key: &[u8],
) -> Result<()> {
    let tuple_key = core_meta_record_tuple_key(core_meta_key)?;
    let expected =
        object_version_page_key(family, bucket, &decoded.object, decoded.root_generation);
    if tuple_key != expected {
        bail!(
            "CoreStore object-version page row key mismatch: key={} version={} generation={} actual={} expected={}",
            decoded.object.key,
            decoded.object.version_id,
            decoded.root_generation,
            hex::encode(tuple_key),
            hex::encode(expected),
        );
    }
    Ok(())
}

pub(super) fn object_current_key(bucket: &Bucket, object_key: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
    ])
}

pub(super) fn object_version_key(
    bucket: &Bucket,
    object_key: &str,
    version_id: uuid::Uuid,
) -> Vec<u8> {
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
    ])
}

pub(super) fn object_version_id_key(bucket: &Bucket, version_id: uuid::Uuid) -> Vec<u8> {
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        OBJECT_VERSION_ID_FAMILY,
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        version_id.as_bytes(),
    ])
}

fn object_current_history_object_prefix(bucket: &Bucket, object_key: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-history-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
    ])
}

pub(super) fn object_current_history_key(
    bucket: &Bucket,
    object_key: &str,
    root_generation: u64,
    version_id: uuid::Uuid,
) -> Vec<u8> {
    let root_generation = root_generation.to_be_bytes();
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-history-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        &root_generation,
        version_id.as_bytes(),
    ])
}

fn object_current_history_range_end(
    bucket: &Bucket,
    object_key: &str,
    root_generation: u64,
) -> Vec<u8> {
    let root_generation = root_generation.to_be_bytes();
    meta_tuple_key(&[
        b"object-history-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        &root_generation,
        &[u8::MAX],
    ])
}

fn object_version_history_object_prefix(
    bucket: &Bucket,
    object_key: &str,
    version_id: uuid::Uuid,
) -> Vec<u8> {
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-history-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
    ])
}

pub(super) fn object_version_history_key(
    bucket: &Bucket,
    object_key: &str,
    version_id: uuid::Uuid,
    root_generation: u64,
) -> Vec<u8> {
    let version_id = version_id.to_string();
    let root_generation = root_generation.to_be_bytes();
    meta_tuple_key(&[
        b"object-history-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
        &root_generation,
    ])
}

pub(super) fn object_id_counter_key(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-id-counter",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

pub(super) fn object_current_page_key_for_object(bucket: &Bucket, object: &Object) -> Vec<u8> {
    object_current_page_key(OBJECT_CURRENT_PAGE_FAMILY, bucket, &object.key)
}

pub(super) fn object_key_catalog_key(bucket: &Bucket, object: &Object) -> Vec<u8> {
    object_current_page_key(OBJECT_KEY_CATALOG_FAMILY, bucket, &object.key)
}

pub(super) fn object_version_page_key_for_object(
    bucket: &Bucket,
    object: &Object,
    root_generation: u64,
) -> Vec<u8> {
    object_version_page_key(OBJECT_VERSION_PAGE_FAMILY, bucket, object, root_generation)
}

pub(super) fn object_version_catalog_key(
    bucket: &Bucket,
    object: &Object,
    root_generation: u64,
) -> Vec<u8> {
    object_version_page_key(
        OBJECT_VERSION_CATALOG_FAMILY,
        bucket,
        object,
        root_generation,
    )
}

fn object_current_page_key(family: &[u8], bucket: &Bucket, object_key: &str) -> Vec<u8> {
    let mut key = object_ordered_prefix(family, bucket, object_key);
    append_u64_tuple_part(&mut key, OBJECT_KEY_END);
    key
}

fn object_version_page_key(
    family: &[u8],
    bucket: &Bucket,
    object: &Object,
    root_generation: u64,
) -> Vec<u8> {
    let mut key = object_ordered_full_key_prefix(family, bucket, &object.key);
    append_u64_tuple_part(&mut key, descending_i64(object.created_at.timestamp()));
    append_u64_tuple_part(
        &mut key,
        u64::MAX - u64::from(object.created_at.timestamp_subsec_nanos()),
    );
    append_u64_tuple_part(&mut key, u64::MAX - root_generation);
    let mut descending_version_id = *object.version_id.as_bytes();
    for byte in &mut descending_version_id {
        *byte = !*byte;
    }
    append_raw_tuple_part(&mut key, &descending_version_id);
    key
}

fn object_ordered_full_key_prefix(family: &[u8], bucket: &Bucket, object_key: &str) -> Vec<u8> {
    let mut key = object_ordered_prefix(family, bucket, object_key);
    append_u64_tuple_part(&mut key, OBJECT_KEY_END);
    key
}

pub(super) fn object_version_page_prefix(bucket: &Bucket, object_key: &str) -> Vec<u8> {
    object_ordered_full_key_prefix(OBJECT_VERSION_PAGE_FAMILY, bucket, object_key)
}

fn object_ordered_after_key(family: &[u8], bucket: &Bucket, object_key: &str) -> Vec<u8> {
    let mut key = object_ordered_prefix(family, bucket, object_key);
    append_u64_tuple_part(&mut key, OBJECT_KEY_AFTER);
    key
}

fn object_ordered_prefix_after(family: &[u8], bucket: &Bucket, prefix: &str) -> Vec<u8> {
    let mut key = object_ordered_prefix(family, bucket, prefix);
    append_u64_tuple_part(&mut key, u64::MAX);
    key
}

fn object_ordered_prefix(family: &[u8], bucket: &Bucket, object_prefix: &str) -> Vec<u8> {
    let mut key = meta_tuple_key(&[
        family,
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ]);
    for byte in object_prefix.as_bytes() {
        append_u64_tuple_part(&mut key, u64::from(*byte) + OBJECT_KEY_BYTE_OFFSET);
    }
    key
}

fn append_u64_tuple_part(key: &mut Vec<u8>, value: u64) {
    key.extend_from_slice(
        &core_meta_tuple_key(&[CoreMetaTuplePart::U64(value)])
            .expect("object metadata ordered u64 key part must be valid"),
    );
}

fn append_raw_tuple_part(key: &mut Vec<u8>, value: &[u8]) {
    key.extend_from_slice(
        &core_meta_tuple_key(&[CoreMetaTuplePart::Raw(value)])
            .expect("object metadata ordered raw key part must be valid"),
    );
}

fn descending_i64(value: i64) -> u64 {
    !((value as u64) ^ (1_u64 << 63))
}
