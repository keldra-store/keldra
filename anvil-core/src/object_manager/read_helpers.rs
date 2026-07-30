use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObjectReadAuthority {
    Caller,
    CommittedInternalTask,
}

impl ObjectManager {
    pub(in crate::object_manager) async fn get_tenant_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        if let Some(locator) = self
            .persistence
            .get_mesh_bucket_locator(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            && locator.status != crate::mesh_directory::BucketLocatorStatus::Deleted
            && locator.home_region.as_str() != self.region.as_str()
        {
            return Err(self.remote_bucket_status(locator.home_region.as_str()));
        }
        let bucket = bucket_journal::read_current_bucket_mvcc(
            self.installed_mvcc()?,
            tenant_id,
            bucket_name,
        )
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))?;
        if bucket.region != self.region {
            return Err(self.remote_bucket_status(&bucket.region));
        }
        Ok(bucket)
    }
}

pub(super) fn object_data_read_status(error: anyhow::Error) -> Status {
    crate::services::core_store_status::availability_status(&error)
        .unwrap_or_else(|| Status::internal(format!("Object data unavailable: {error}")))
}

pub(super) fn object_list_result_limit(limit: i32, maximum: usize) -> usize {
    usize::try_from(normalized_list_limit(limit))
        .unwrap_or(maximum)
        .clamp(1, maximum)
}

pub(super) fn object_listing_candidate_budget(result_limit: usize) -> usize {
    result_limit
        .saturating_mul(OBJECT_LIST_CANDIDATE_MULTIPLIER)
        .saturating_add(1)
        .clamp(result_limit, MAX_OBJECT_LIST_CANDIDATES)
}

pub(super) fn object_listing_distinct_entry_count(
    objects: &[Object],
    prefix: &str,
    delimiter: &str,
    stop_at: usize,
) -> usize {
    let mut entries = BTreeSet::new();
    for object in objects {
        let entry = object_listing_common_prefix(&object.key, prefix, delimiter)
            .unwrap_or_else(|| object.key.clone());
        entries.insert(entry);
        if entries.len() >= stop_at {
            break;
        }
    }
    entries.len()
}

pub(super) fn object_listing_common_prefix(
    key: &str,
    prefix: &str,
    delimiter: &str,
) -> Option<String> {
    if delimiter.is_empty() {
        return None;
    }
    let suffix = key.strip_prefix(prefix)?;
    let position = suffix.find(delimiter)?;
    Some(format!(
        "{}{}",
        prefix,
        &suffix[..position + delimiter.len()]
    ))
}

pub(super) fn object_metadata_page_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains("ObjectMetadataSourceChanged")
        || message.contains("ObjectMetadataPageCursorSourceMismatch")
    {
        Status::failed_precondition(message)
    } else {
        Status::internal(message)
    }
}
