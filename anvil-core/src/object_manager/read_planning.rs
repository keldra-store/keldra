use super::*;

#[derive(Debug, Clone)]
pub(super) struct ObjectListingPlanDoc {
    pub(super) doc_id: CoreDocId,
    pub(super) object: Object,
    pub(super) version_is_latest: bool,
    pub(super) is_delete_marker: bool,
    pub(super) authz_key: ObjectAuthzKey,
    pub(super) order_tuple: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectListingPlanOutput {
    pub(super) docs: Vec<ObjectListingPlanDoc>,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectListingCandidateReader {
    pub(super) scope: CandidateSetScope,
    pub(super) partition_id: u64,
    pub(super) docs: Vec<ObjectListingPlanDoc>,
}

impl ObjectListingCandidateReader {
    pub(super) fn new(
        scope: CandidateSetScope,
        partition_id: u64,
        docs: Vec<ObjectListingPlanDoc>,
    ) -> Self {
        Self {
            scope,
            partition_id,
            docs,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ObjectListingAuthzCandidateReader {
    pub(super) storage: crate::storage::Storage,
    pub(super) tenant_id: i64,
    pub(super) claims: auth::Claims,
    pub(super) bucket: Bucket,
    pub(super) docs: Vec<ObjectListingPlanDoc>,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectListingAuthzSubject {
    pub(super) subject_kind: String,
    pub(super) subject_id: String,
    pub(super) caveat_hash: String,
}

#[derive(Debug, Clone)]
pub(super) struct ObjectListingAuthzAllowance {
    pub(super) bucket_wide: bool,
    pub(super) object_ids: BTreeSet<String>,
}

impl ObjectListingAuthzCandidateReader {
    pub(super) fn new(
        storage: crate::storage::Storage,
        tenant_id: i64,
        claims: auth::Claims,
        bucket: Bucket,
        docs: Vec<ObjectListingPlanDoc>,
    ) -> Self {
        Self {
            storage,
            tenant_id,
            claims,
            bucket,
            docs,
        }
    }

    async fn allowance(
        &self,
        request: &AuthzCandidateRequest,
    ) -> AnyhowResult<ObjectListingAuthzAllowance> {
        let subject = object_listing_authz_subject(&request.subject);
        let mut object_ids = BTreeSet::new();
        let system_revision = i64::try_from(request.system_revision)
            .map_err(|_| anyhow!("Invalid system authz revision"))?;
        let bucket_wide = access_control::system_realm_relationship_allows(
            &self.storage,
            &self.claims,
            crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
            &access_control::bucket_object_id(&self.bucket),
            "get_object",
            Some(system_revision),
        )
        .await?;
        if bucket_wide {
            return Ok(ObjectListingAuthzAllowance {
                bucket_wide,
                object_ids,
            });
        }

        let Some(segment) = crate::authz_segment::read_required_authz_tuple_segment_at_revision(
            &self.storage,
            self.tenant_id,
            request.system_revision,
        )
        .await?
        else {
            if request.system_revision > 0 {
                bail!("AuthzCandidateSetStale");
            }
            return Ok(ObjectListingAuthzAllowance {
                bucket_wide,
                object_ids,
            });
        };
        if segment.header.generation != request.system_revision {
            bail!("AuthzCandidateSetStale");
        }

        let bucket_namespace =
            access_control::system_realm_namespace(crate::system_realm::SYSTEM_BUCKET_NAMESPACE);
        let object_namespace =
            access_control::system_realm_namespace(crate::system_realm::SYSTEM_OBJECT_NAMESPACE);
        let bucket_object_id = access_control::bucket_object_id(&self.bucket);

        for row in &segment.list_objects {
            if row.revision > request.system_revision
                || !object_listing_authz_row_subject_matches(row, &subject)
            {
                continue;
            }
            if row.namespace == bucket_namespace
                && row.relation == "get_object"
                && row.object_id == bucket_object_id
            {
                return Ok(ObjectListingAuthzAllowance {
                    bucket_wide: true,
                    object_ids,
                });
            } else if row.namespace == object_namespace
                && row.relation == request.relation
                && row.object_id.starts_with(&format!("{}/", self.bucket.id))
            {
                object_ids.insert(row.object_id.clone());
            }
        }

        Ok(ObjectListingAuthzAllowance {
            bucket_wide,
            object_ids,
        })
    }
}

impl AuthzCandidateReader for ObjectListingAuthzCandidateReader {
    async fn candidate_set(&self, request: AuthzCandidateRequest) -> AnyhowResult<CandidateSet> {
        let allowance = self.allowance(&request).await?;
        if allowance.bucket_wide {
            return Ok(CandidateSet::all_within_partition(
                request.candidate_scope,
                request.partition_id,
            ));
        }
        let doc_ordinals = self
            .docs
            .iter()
            .filter(|doc| {
                allowance
                    .object_ids
                    .contains(&doc.authz_key.canonical_object_id)
            })
            .map(|doc| doc.doc_id.ordinal());
        Ok(CandidateSet::bitmap_from_ordinals(
            request.candidate_scope,
            request.partition_id,
            doc_ordinals,
        ))
    }

    async fn verify_page(
        &self,
        request: AuthzCandidateRequest,
        object_keys: Vec<ObjectAuthzKey>,
    ) -> AnyhowResult<Vec<AuthzDecision>> {
        let allowance = self.allowance(&request).await?;
        Ok(object_keys
            .into_iter()
            .map(|object_key| AuthzDecision {
                allowed: allowance.bucket_wide
                    || allowance
                        .object_ids
                        .contains(&object_key.canonical_object_id),
                object_key,
                revision: request.system_revision,
            })
            .collect())
    }
}

fn object_listing_authz_row_subject_matches(
    row: &crate::authz_segment::AuthzListObjectsRow,
    subject: &ObjectListingAuthzSubject,
) -> bool {
    row.subject_kind == subject.subject_kind
        && row.subject_id == subject.subject_id
        && row.caveat_hash == subject.caveat_hash
}

fn object_listing_authz_subject(subject: &str) -> ObjectListingAuthzSubject {
    let (subject_kind, rest) = subject
        .split_once(':')
        .map(|(kind, id)| (kind.to_string(), id.to_string()))
        .unwrap_or_else(|| ("user".to_string(), subject.to_string()));
    let (subject_id, caveat_hash) = rest
        .split_once('@')
        .map(|(id, caveat)| (id.to_string(), caveat.to_string()))
        .unwrap_or((rest, String::new()));
    ObjectListingAuthzSubject {
        subject_kind,
        subject_id,
        caveat_hash,
    }
}

impl BoundaryCandidateReader for ObjectListingCandidateReader {
    async fn boundary_candidates(
        &self,
        request: BoundaryCandidateRequest,
    ) -> AnyhowResult<CandidateSet> {
        if self.scope.root_key_hash != request.root_key_hash
            || self.scope.root_generation != request.root_generation
            || self.scope.boundary_schema_generation_hash != request.boundary_schema_generation_hash
        {
            bail!("IndexGenerationMismatch");
        }
        Ok(CandidateSet::all_within_partition(
            self.scope.clone(),
            self.partition_id,
        ))
    }
}

impl IndexCandidateReader for ObjectListingCandidateReader {
    async fn predicate_candidates(
        &self,
        request: IndexCandidateRequest,
    ) -> AnyhowResult<CandidateSet> {
        if self.scope.index_id != request.index_id
            || self.scope.index_generation != request.generation
            || self.scope.predicate_hash != request.predicate_json
            || request
                .order_json
                .as_ref()
                .is_some_and(|order_hash| *order_hash != self.scope.order_hash)
        {
            bail!("IndexGenerationMismatch");
        }
        Ok(CandidateSet {
            scope: self.scope.clone(),
            kind: CandidateSetKind::OrderedTuples {
                partition_id: self.partition_id,
                tuples: self
                    .docs
                    .iter()
                    .map(|doc| OrderedDocTuple {
                        order_tuple: doc.order_tuple.clone(),
                        doc_id: doc.doc_id,
                    })
                    .collect(),
            },
        })
    }

    async fn range_plan(&self, request: RangePlanRequest) -> AnyhowResult<Vec<ReadRangePlan>> {
        request
            .candidates
            .scope
            .ensure_compatible_with(&self.scope)?;
        let limit = usize::try_from(request.limit)
            .map_err(|_| anyhow!("object listing limit exceeds usize"))?;
        Ok(self
            .docs
            .iter()
            .enumerate()
            .filter(|(_, doc)| request.candidates.contains_doc_id(doc.doc_id))
            .take(limit)
            .map(|(index, doc)| ReadRangePlan {
                manifest_hash: self.scope.index_id.clone(),
                logical_start: index as u64,
                logical_end: index as u64 + 1,
                doc_ids: vec![doc.doc_id],
                authz_keys: vec![doc.authz_key.clone()],
            })
            .collect())
    }
}

pub(super) fn object_listing_docs(
    bucket: &Bucket,
    objects: Vec<Object>,
    family: &str,
) -> Vec<ObjectListingPlanDoc> {
    objects
        .into_iter()
        .map(|object| object_listing_doc(bucket, object, family, false))
        .collect()
}

pub(super) fn object_version_listing_docs(
    bucket: &Bucket,
    versions: Vec<crate::persistence::ObjectVersion>,
    family: &str,
) -> Vec<ObjectListingPlanDoc> {
    versions
        .into_iter()
        .map(|version| {
            let mut doc = object_listing_doc(bucket, version.object, family, version.is_latest);
            doc.is_delete_marker = version.is_delete_marker;
            doc
        })
        .collect()
}

fn object_listing_doc(
    bucket: &Bucket,
    object: Object,
    family: &str,
    version_is_latest: bool,
) -> ObjectListingPlanDoc {
    let namespace =
        access_control::system_realm_namespace(crate::system_realm::SYSTEM_OBJECT_NAMESPACE);
    let object_id = access_control::object_object_id(bucket, &object.key);
    let authz_key = ObjectAuthzKey::realm_object(namespace, object_id);
    let partition_id = object_listing_partition_id(bucket, family);
    let doc_id = authz_key.doc_id(partition_id);
    let order_tuple = vec![
        object.key.as_bytes().to_vec(),
        object.created_at.to_rfc3339().as_bytes().to_vec(),
        object.version_id.as_bytes().to_vec(),
    ];
    let is_delete_marker = object.deleted_at.is_some();
    ObjectListingPlanDoc {
        doc_id,
        object,
        version_is_latest,
        is_delete_marker,
        authz_key,
        order_tuple,
    }
}

pub(super) fn object_listing_root_generation(objects: &[Object]) -> u64 {
    objects
        .iter()
        .map(|object| object.id.max(0) as u64)
        .max()
        .unwrap_or(0)
}

pub(super) fn object_version_listing_root_generation(
    versions: &[crate::persistence::ObjectVersion],
) -> u64 {
    versions
        .iter()
        .map(|version| version.object.id.max(0) as u64)
        .max()
        .unwrap_or(0)
}

pub(super) fn object_listing_partition_id(bucket: &Bucket, family: &str) -> u64 {
    stable_doc_ordinal(&[
        "object-list-partition",
        family,
        &bucket.tenant_id.to_string(),
        &bucket.id.to_string(),
    ])
}

pub(super) fn object_listing_hash(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("blake3:{hex}")
}

pub(super) fn object_listing_objects_from_plan(plan: &ObjectListingPlanOutput) -> Vec<Object> {
    plan.docs.iter().map(|doc| doc.object.clone()).collect()
}

pub(super) fn object_listing_versions_from_plan(
    plan: &ObjectListingPlanOutput,
) -> Vec<crate::persistence::ObjectVersion> {
    plan.docs
        .iter()
        .map(|doc| crate::persistence::ObjectVersion {
            object: doc.object.clone(),
            is_latest: doc.version_is_latest,
            is_delete_marker: doc.is_delete_marker,
        })
        .collect()
}

pub(super) fn shape_object_listing(
    objects: Vec<Object>,
    prefix: &str,
    delimiter: &str,
    limit: i32,
) -> (Vec<Object>, Vec<String>) {
    let limit = normalized_list_limit(limit).max(1) as usize;
    if delimiter.is_empty() {
        return (objects.into_iter().take(limit).collect(), Vec::new());
    }

    enum ListingEntry {
        Object(Object),
        CommonPrefix(String),
    }

    let mut merged = BTreeMap::<String, ListingEntry>::new();
    for object in objects {
        let suffix = &object.key[prefix.len()..];
        if let Some(position) = suffix.find(delimiter) {
            let common_prefix = format!("{}{}", prefix, &suffix[..position + delimiter.len()]);
            merged
                .entry(common_prefix.clone())
                .or_insert(ListingEntry::CommonPrefix(common_prefix));
        } else {
            merged.insert(object.key.clone(), ListingEntry::Object(object));
        }
        if merged.len() >= limit {
            break;
        }
    }

    let mut listed = Vec::new();
    let mut common_prefixes = Vec::new();
    for (_, entry) in merged.into_iter().take(limit) {
        match entry {
            ListingEntry::Object(object) => listed.push(object),
            ListingEntry::CommonPrefix(prefix) => common_prefixes.push(prefix),
        }
    }
    (listed, common_prefixes)
}

pub(super) fn shape_object_version_listing(
    mut versions: Vec<crate::persistence::ObjectVersion>,
    limit: i32,
) -> crate::persistence::ObjectVersionsPage {
    let limit = normalized_list_limit(limit).max(1) as usize;
    let is_truncated = versions.len() > limit;
    if is_truncated {
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
    crate::persistence::ObjectVersionsPage {
        versions,
        is_truncated,
        next_key_marker,
        next_version_id_marker,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bucket_wide_listing_does_not_materialize_an_authz_segment() {
        let temp = tempfile::tempdir().unwrap();
        let config = crate::config::Config {
            jwt_secret: "test-secret".into(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            mesh_id: "object-list-authz-test".into(),
            region: "test-region".into(),
            storage_path: temp.path().to_string_lossy().into_owned(),
            bootstrap_system_admin_subject_kind: "app".into(),
            bootstrap_system_admin_subject_id: "system-admin".into(),
            ..crate::config::Config::default()
        };
        let storage = crate::storage::Storage::new_at(temp.path()).await.unwrap();
        let persistence = crate::persistence::Persistence::new(&config).unwrap();
        crate::system_realm::ensure_bootstrapped(
            &config,
            &persistence,
            &storage,
            &config.secret_keyring().unwrap(),
        )
        .await
        .unwrap();
        persistence.create_region("test-region").await.unwrap();
        let tenant = persistence
            .create_tenant("object-list-tenant", "object-list-tenant")
            .await
            .unwrap();
        let bucket = persistence
            .create_bucket(tenant.id, "objects", "test-region")
            .await
            .unwrap();
        let owner = auth::Claims {
            sub: "tenant-owner".into(),
            exp: usize::MAX,
            tenant_id: tenant.id,
            jti: None,
        };
        access_control::grant_storage_tenant_owner(
            &persistence,
            tenant.id,
            &owner.sub,
            "test",
            "grant computed tenant ownership",
        )
        .await
        .unwrap();
        access_control::grant_bucket_defaults(
            &persistence,
            &bucket,
            &owner.sub,
            "test",
            "connect bucket to tenant",
        )
        .await
        .unwrap();
        let revision = crate::authz_journal::latest_authz_revision(
            &storage,
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
        )
        .await
        .unwrap() as u64;

        assert!(
            crate::authz_segment::existing_authz_tuple_segment_ref(
                &storage,
                crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
                revision,
            )
            .await
            .unwrap()
            .is_none()
        );

        let reader = ObjectListingAuthzCandidateReader::new(
            storage.clone(),
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
            owner.clone(),
            bucket.clone(),
            Vec::new(),
        );
        let scope = CandidateSetScope {
            root_key_hash: String::new(),
            root_generation: 0,
            index_id: String::new(),
            index_generation: 0,
            authz_realm_id: crate::system_realm::SYSTEM_REALM_ID.to_string(),
            authz_scope_hash: String::new(),
            authz_object_namespace: String::new(),
            authz_relation: "get".into(),
            authz_principal_hash: String::new(),
            authz_revision: revision,
            boundary_schema_generation_hash: String::new(),
            predicate_hash: String::new(),
            order_hash: String::new(),
        };
        let allowance = reader
            .allowance(&AuthzCandidateRequest {
                authz_scope: String::new(),
                candidate_scope: scope,
                partition_id: 0,
                subject: format!("{}:{}", access_control::APP_SUBJECT_KIND, owner.sub),
                relation: "get".into(),
                object_namespace: access_control::system_realm_namespace(
                    crate::system_realm::SYSTEM_OBJECT_NAMESPACE,
                ),
                revision,
                system_revision: revision,
                root_generation: 0,
            })
            .await
            .unwrap();

        assert!(allowance.bucket_wide);
        assert!(allowance.object_ids.is_empty());
        assert!(
            crate::authz_segment::existing_authz_tuple_segment_ref(
                &storage,
                crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
                revision,
            )
            .await
            .unwrap()
            .is_none()
        );
    }
}
