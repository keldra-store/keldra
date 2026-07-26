use super::super::local_batch_ops::OwnedCoreMetaBatchOp;
use super::super::local_stream_control::control_record_proto::encode_boundary_value_row;
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectMetadataProjectionMutation {
    Upsert,
    DeleteVersion,
}

pub(crate) struct ObjectMetadataMutationGuard {
    _guard: super::super::CoreStoreLock,
}

#[derive(Debug, Clone)]
pub(crate) struct ObjectMetadataPreconditionSnapshot {
    pub(crate) object: Option<Object>,
    pub(crate) precondition: CoreMutationPrecondition,
}

impl CoreStore {
    pub(crate) fn object_metadata_precondition_snapshot(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Result<ObjectMetadataPreconditionSnapshot> {
        let tuple_key = object_current_key(bucket, object_key);
        let payload = self.read_coremeta_row(CF_OBJECT_HEADS, TABLE_OBJECT_HEAD_ROW, &tuple_key)?;
        object_metadata_precondition_snapshot_from_payload(bucket, object_key, tuple_key, payload)
    }

    pub(crate) fn object_metadata_precondition_snapshot_from_payload(
        &self,
        bucket: &Bucket,
        object_key: &str,
        payload: Option<Vec<u8>>,
    ) -> Result<ObjectMetadataPreconditionSnapshot> {
        object_metadata_precondition_snapshot_from_payload(
            bucket,
            object_key,
            object_current_key(bucket, object_key),
            payload,
        )
    }

    pub(crate) fn object_metadata_current_tuple_key(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Vec<u8> {
        object_current_key(bucket, object_key)
    }
}

fn object_metadata_precondition_snapshot_from_payload(
    bucket: &Bucket,
    object_key: &str,
    tuple_key: Vec<u8>,
    payload: Option<Vec<u8>>,
) -> Result<ObjectMetadataPreconditionSnapshot> {
    let decoded = payload
        .as_deref()
        .map(decode_object_metadata_row)
        .transpose()?;
    if let Some(object) = decoded.as_ref() {
        validate_object_scope(bucket, object)?;
        if object.key != object_key {
            bail!("CoreStore object metadata current row key mismatch");
        }
    }
    let object = decoded.filter(|object| object.deleted_at.is_none());
    let expected_payload_hash = payload
        .as_ref()
        .map(|payload| core_meta_payload_digest(TABLE_OBJECT_HEAD_ROW, payload));
    Ok(ObjectMetadataPreconditionSnapshot {
        object,
        precondition: CoreMutationPrecondition::CoreMetaRow {
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_OBJECT_HEAD_ROW,
            tuple_key,
            require_absent: expected_payload_hash.is_none(),
            require_present: expected_payload_hash.is_some(),
            expected_payload_hash,
        },
    })
}

impl CoreStore {
    pub(crate) async fn acquire_object_metadata_mutation_lock(
        &self,
        bucket: &Bucket,
    ) -> Result<ObjectMetadataMutationGuard> {
        Ok(ObjectMetadataMutationGuard {
            _guard: self
                .acquire_named_lock(
                    "object-metadata-bucket",
                    &object_metadata_bucket_lock_id(bucket),
                )
                .await?,
        })
    }

    pub(crate) async fn materialize_object_metadata_ancillary_projections(
        &self,
        bucket: &Bucket,
        object: &Object,
        mutation: ObjectMetadataProjectionMutation,
    ) -> Result<()> {
        let transaction_id = object.mutation_id.to_string();
        let projection = [(bucket, object, mutation)];
        self.materialize_object_metadata_ancillary_projection_batch(&transaction_id, &projection)
            .await
    }

    pub(crate) async fn materialize_object_metadata_ancillary_projection_batch(
        &self,
        transaction_id: &str,
        projections: &[(&Bucket, &Object, ObjectMetadataProjectionMutation)],
    ) -> Result<()> {
        let mut prepared = PreparedAncillaryProjectionBatch::default();
        for (bucket, object, mutation) in projections {
            validate_object_scope(bucket, object)?;
            let (payload_ops, payload_publications) = match mutation {
                ObjectMetadataProjectionMutation::Upsert => {
                    self.payload_reference_put_ops_for_object(bucket, object, transaction_id)
                        .await?
                }
                ObjectMetadataProjectionMutation::DeleteVersion => {
                    self.payload_reference_delete_ops_for_object(bucket, object, transaction_id)
                        .await?
                }
            };
            prepared.extend(payload_ops, payload_publications)?;

            if *mutation == ObjectMetadataProjectionMutation::Upsert {
                let (boundary_ops, boundary_publications) = self
                    .prepare_object_boundary_value_projections(bucket, object)
                    .await?;
                prepared.extend(boundary_ops, boundary_publications)?;
            }
        }

        let (owned_ops, publications) = prepared.finish()?;
        if owned_ops.is_empty() {
            return Ok(());
        }
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.commit_coremeta_root_groups(transaction_id, &ops, &publications)
            .await?;
        Ok(())
    }

    pub async fn next_object_metadata_id(&self, bucket: &Bucket) -> Result<i64> {
        let counter_key = object_id_counter_key(bucket);
        let projected_max_id = match self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => 0,
        };
        self.next_object_metadata_id_after_counter(bucket, projected_max_id)
            .await
    }

    pub(crate) async fn next_object_metadata_id_in_mvcc_transaction(
        &self,
        bucket: &Bucket,
        mvcc: &crate::mvcc_bootstrap::MvccSubsystem,
        transaction_id: &str,
        transaction_principal: &str,
    ) -> Result<i64> {
        let counter_key = object_id_counter_key(bucket);
        let logical_key = crate::mvcc_product::coremeta_logical_key(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )?;
        let projected_max_id = match mvcc.read_transaction_value(
            transaction_id,
            transaction_principal,
            &logical_key,
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => 0,
        };
        self.next_object_metadata_id_after_counter(bucket, projected_max_id)
            .await
    }

    async fn next_object_metadata_id_after_counter(
        &self,
        bucket: &Bucket,
        projected_max_id: i64,
    ) -> Result<i64> {
        let metadata_stream_id = format!(
            "object_metadata:tenant:{}:bucket:{}",
            bucket.tenant_id, bucket.id
        );
        let stream_sequence = self.stream_head_sequence(&metadata_stream_id).await?;
        let stream_max_id = i64::try_from(stream_sequence)
            .context("object metadata stream sequence exceeds i64")?;
        projected_max_id
            .max(stream_max_id)
            .checked_add(1)
            .ok_or_else(|| anyhow!("object id overflow"))
    }

    pub(super) async fn current_object_metadata_root_generation(
        &self,
        bucket: &Bucket,
    ) -> Result<u64> {
        let counter_key = object_id_counter_key(bucket);
        let Some(payload) = self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )?
        else {
            return Ok(0);
        };
        let counter = decode_object_metadata_counter_for_bucket(&payload, bucket)?;
        Ok(counter
            .common
            .expect("counter decoder requires CoreMeta common")
            .root_generation)
    }

    async fn prepare_object_boundary_value_projections(
        &self,
        bucket: &Bucket,
        object: &Object,
    ) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        let Some(data_target) = object_data_target_from_shard_map(object.shard_map.as_ref())?
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        let boundary_values = match &data_target {
            ObjectDataTarget::LogicalFile { locator, .. } => {
                let manifest = self.read_logical_file_manifest(locator).await?;
                manifest_boundary_values(&manifest)
            }
            ObjectDataTarget::ObjectRef { object_ref, .. } => {
                self.read_object_manifest(object_ref).await?.boundary_values
            }
            ObjectDataTarget::MvccLocal { .. } | ObjectDataTarget::MvccShards { .. } => Vec::new(),
        };
        if boundary_values.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        let bucket_key = boundary_schema_bucket_key(bucket.tenant_id, &bucket.name);
        let object_ref = data_target.target_string();
        validate_logical_id(&bucket_key, "boundary value bucket")?;
        validate_logical_id(object_ref, "boundary value object ref")?;

        let mut operations = Vec::with_capacity(boundary_values.len());
        for value in &boundary_values {
            validate_logical_id(&value.name, "boundary value dimension")?;
            let tuple_key = ancillary_boundary_value_key(&bucket_key, value, object_ref)?;
            let payload = encode_boundary_value_row(&bucket_key, object_ref, "object", value)?;
            operations.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_BOUNDARY,
                table_id: TABLE_BOUNDARY_VALUE_ROW,
                tuple_key,
                payload,
                common: None,
            });
        }
        let publications = vec![CoreMetaRootPublication::new(
            ancillary_boundary_root_anchor_key(&bucket_key),
            WriterFamily::TypedMetadata,
        )];
        Ok((operations, publications))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AncillaryOperationKey {
    cf: &'static str,
    table_id: u16,
    tuple_key: Vec<u8>,
}

#[derive(Default)]
struct PreparedAncillaryProjectionBatch {
    operations: Vec<OwnedCoreMetaBatchOp>,
    operation_positions: BTreeMap<AncillaryOperationKey, (usize, String)>,
    publications: BTreeMap<String, CoreMetaRootPublication>,
}

impl PreparedAncillaryProjectionBatch {
    fn extend(
        &mut self,
        operations: Vec<OwnedCoreMetaBatchOp>,
        publications: Vec<CoreMetaRootPublication>,
    ) -> Result<()> {
        for publication in publications {
            self.merge_publication(publication)?;
        }
        for operation in operations {
            self.push_operation(operation)?;
        }
        Ok(())
    }

    fn merge_publication(&mut self, mut publication: CoreMetaRootPublication) -> Result<()> {
        publication.transaction_coordinator = false;
        let root_key_hash = publication.root_key_hash();
        let Some(existing) = self.publications.get_mut(&root_key_hash) else {
            publication.writer_families.sort();
            publication.writer_families.dedup();
            self.publications.insert(root_key_hash, publication);
            return Ok(());
        };
        if existing.root_anchor_key != publication.root_anchor_key {
            bail!("ancillary projection publication root hash collision");
        }
        existing.writer_families.extend(publication.writer_families);
        existing.writer_families.sort();
        existing.writer_families.dedup();
        for manifest in publication.logical_manifests {
            if !existing.logical_manifests.contains(&manifest) {
                existing.logical_manifests.push(manifest);
            }
        }
        existing
            .idempotency_key_hashes
            .extend(publication.idempotency_key_hashes);
        existing.idempotency_key_hashes.sort();
        existing.idempotency_key_hashes.dedup();
        Ok(())
    }

    fn push_operation(&mut self, operation: OwnedCoreMetaBatchOp) -> Result<()> {
        let (operation_key, root_key_hash) = ancillary_operation_identity(&operation)?;
        if root_key_hash.is_empty() {
            bail!("ancillary projection operation is not bound to a publication root");
        }
        if let Some((position, existing_root_key_hash)) =
            self.operation_positions.get(&operation_key)
        {
            if existing_root_key_hash != &root_key_hash {
                bail!("ancillary projection physical row spans multiple publication roots");
            }
            // Projections follow committed stream order, so the final effect
            // for a repeated physical row is the transaction-visible result.
            self.operations[*position] = operation;
            return Ok(());
        }
        let position = self.operations.len();
        self.operations.push(operation);
        self.operation_positions
            .insert(operation_key, (position, root_key_hash));
        Ok(())
    }

    fn finish(mut self) -> Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreMetaRootPublication>)> {
        if self.operations.is_empty() {
            if !self.publications.is_empty() {
                bail!("ancillary projection declares publication roots without operations");
            }
            return Ok((Vec::new(), Vec::new()));
        }

        let operation_roots = self
            .operation_positions
            .values()
            .map(|(_, root_key_hash)| root_key_hash.clone())
            .collect::<BTreeSet<_>>();
        let publication_roots = self.publications.keys().cloned().collect::<BTreeSet<_>>();
        if operation_roots != publication_roots {
            bail!("ancillary projection operations and publication roots do not match");
        }

        // CoreMeta rebinds every row to its root's next generation. A single
        // coordinator makes those root publications one atomic commit group.
        let coordinator = self
            .publications
            .values_mut()
            .next()
            .ok_or_else(|| anyhow!("ancillary projection has no coordinator root"))?;
        coordinator
            .writer_families
            .push(WriterFamily::CoreControl.as_str().to_string());
        coordinator.writer_families.sort();
        coordinator.writer_families.dedup();
        coordinator.transaction_coordinator = true;

        Ok((self.operations, self.publications.into_values().collect()))
    }
}

fn ancillary_operation_identity(
    operation: &OwnedCoreMetaBatchOp,
) -> Result<(AncillaryOperationKey, String)> {
    let (cf, table_id, tuple_key, common) = match operation {
        OwnedCoreMetaBatchOp::Put {
            cf,
            table_id,
            tuple_key,
            payload,
            common,
        } => (
            *cf,
            *table_id,
            tuple_key,
            match common {
                Some(common) => common.clone(),
                None => core_meta_row_common_from_payload(payload)?,
            },
        ),
        OwnedCoreMetaBatchOp::Delete {
            cf,
            table_id,
            tuple_key,
            common,
        } => (
            *cf,
            *table_id,
            tuple_key,
            common
                .clone()
                .ok_or_else(|| anyhow!("ancillary projection delete is missing row common"))?,
        ),
    };
    Ok((
        AncillaryOperationKey {
            cf,
            table_id,
            tuple_key: tuple_key.clone(),
        },
        common.root_key_hash,
    ))
}

fn ancillary_boundary_root_anchor_key(bucket: &str) -> String {
    format!("boundary/{bucket}")
}

fn ancillary_boundary_value_key(
    bucket: &str,
    value: &CoreBoundaryValue,
    object_ref: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(bucket),
        CoreMetaTuplePart::Utf8(&value.name),
        CoreMetaTuplePart::Utf8(&value.value),
        CoreMetaTuplePart::Utf8(object_ref),
        CoreMetaTuplePart::Utf8("object"),
    ])
}
