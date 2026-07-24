use super::local::write_file_atomic;
use super::*;
use crate::formats::writer::{
    ByteSource, CoreMetaMutation, DurabilityClass, LogicalFileWrite, WriterBuildOutput,
    WriterFamily, WriterRootPublication,
};
use anyhow::{Context, bail};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;
use tokio::fs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreFormatWriteReceipt {
    pub logical_file_ids: Vec<String>,
    pub manifest_hashes: Vec<String>,
    pub written_object_refs: Vec<CoreObjectRef>,
    pub coremeta_certificate_hashes: Vec<String>,
}

#[derive(Debug)]
struct PreparedFormatCoreMetaCommit {
    transaction_id: String,
    publications: Vec<CoreMetaRootPublication>,
}

impl CoreStore {
    pub async fn write_format_build_output(
        &self,
        output: WriterBuildOutput,
    ) -> Result<CoreFormatWriteReceipt> {
        let prepared_coremeta_commit = self.prepare_format_coremeta_commit(
            &output.core_meta_mutations,
            &output.core_meta_root_publications,
        )?;
        let WriterBuildOutput {
            logical_files,
            core_meta_mutations,
            core_meta_root_publications: _,
        } = output;
        let mut logical_file_ids = Vec::new();
        let mut manifest_hashes = Vec::new();
        let mut written_object_refs = Vec::new();
        for logical_file in logical_files {
            let logical_file_id = logical_file.logical_file_id.clone();
            let writer_family = logical_file.writer_family.as_str().to_string();
            let materialise_started_at = Instant::now();
            crate::perf::record_duration(
                "anvil_materialisation_lag_ms",
                &[
                    ("writer_family", writer_family.as_str()),
                    ("bucket", "local"),
                    ("boundary_schema_generation", "0"),
                ],
                materialise_started_at.elapsed(),
            );
            record_corestore_trace_event("materialiser.plan", "ok");
            let writer_started_at = Instant::now();
            let inline_metadata = logical_file.durability_class == DurabilityClass::InlineMetadata;
            let (object_ref, manifest_hash) = if inline_metadata {
                let ByteSource::InlineBytes(bytes) = logical_file.bytes else {
                    bail!("CoreFormat inline metadata requires an inline byte source");
                };
                let object_ref = self
                    .put_inline_blob(
                        PutBlob {
                            logical_name: logical_file.logical_file_id,
                            bytes,
                            boundary_values: logical_file.boundary_values,
                            region_id: logical_file.region_id,
                            mutation_id: logical_file.mutation_id,
                        },
                        logical_file.writer_family.as_str(),
                        self.storage_class_catalog()
                            .select(None)?
                            .inline_payload_policy
                            .clone(),
                    )
                    .await
                    .with_context(|| format!("write inline CoreFormat file {logical_file_id}"))?;
                let manifest_hash = object_ref.hash.clone();
                (object_ref, manifest_hash)
            } else {
                let written = self
                    .write_format_logical_file(logical_file)
                    .await
                    .with_context(|| format!("write CoreFormat logical file {logical_file_id}"))?;
                let manifest_hash = written.locator.manifest_hash.clone();
                (
                    core_object_ref_from_logical_file_write(&written),
                    manifest_hash,
                )
            };
            crate::perf::record_duration(
                "anvil_writer_build_duration_ms",
                &[
                    ("writer_family", writer_family.as_str()),
                    ("output_kind", "logical_file"),
                    ("status", "ok"),
                ],
                writer_started_at.elapsed(),
            );
            record_corestore_trace_event("writer.build", "ok");
            manifest_hashes.push(manifest_hash);
            logical_file_ids.push(logical_file_id);
            written_object_refs.push(object_ref);
        }

        let coremeta_certificate_hashes = self
            .commit_format_coremeta_mutations(core_meta_mutations, prepared_coremeta_commit)
            .await?;
        Ok(CoreFormatWriteReceipt {
            logical_file_ids,
            manifest_hashes,
            written_object_refs,
            coremeta_certificate_hashes,
        })
    }

    async fn write_format_logical_file(
        &self,
        logical_file: LogicalFileWrite,
    ) -> Result<CoreLogicalFileWrite> {
        match logical_file.bytes.clone() {
            ByteSource::InlineBytes(_) => {
                self.write_logical_file_with_locator(
                    logical_file.into_write_logical_file_request()?,
                )
                .await
            }
            ByteSource::TempFile { path, hash, length } => {
                self.write_logical_file_path_with_locator(
                    logical_file.into_write_logical_file_path_request(
                        path,
                        normalise_sha256_hash(&hash)?,
                        length,
                    ),
                )
                .await
            }
            ByteSource::LandedBytes {
                hash,
                length,
                relative_path,
                ..
            } => {
                let source_path = self
                    .storage()
                    .resolve_relative_storage_path(&relative_path)?;
                self.write_logical_file_path_with_locator(
                    logical_file.into_write_logical_file_path_request(
                        source_path,
                        normalise_sha256_hash(&hash)?,
                        length,
                    ),
                )
                .await
            }
            ByteSource::ExistingCoreObject {
                object_ref,
                byte_start,
                byte_end,
            } => {
                let (path, hash, length) = self
                    .materialise_existing_core_object_range_to_staged_file(
                        object_ref, byte_start, byte_end,
                    )
                    .await?;
                let result = self
                    .write_logical_file_path_with_locator(
                        logical_file.into_write_logical_file_path_request(
                            path.clone(),
                            hash,
                            length,
                        ),
                    )
                    .await;
                let _ = fs::remove_file(&path).await;
                result
            }
        }
    }

    async fn materialise_existing_core_object_range_to_staged_file(
        &self,
        object_ref: CoreObjectRef,
        byte_start: u64,
        byte_end: u64,
    ) -> Result<(std::path::PathBuf, String, u64)> {
        if byte_end < byte_start || byte_end > object_ref.logical_size {
            bail!("CoreFormatWriter existing object byte range is invalid");
        }
        let bytes = self
            .get_blob_range(GetBlobRange {
                object_ref,
                range: CoreByteRange {
                    start: byte_start,
                    end_exclusive: byte_end,
                },
            })
            .await?;
        let hash = format!("sha256:{}", sha256_hex(&bytes));
        let length = bytes.len() as u64;
        let path = self
            .storage()
            .temp_dir_path()
            .join(format!("format-source-{}.tmp", uuid::Uuid::new_v4()));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let started_at = Instant::now();
        write_file_atomic(&path, &bytes).await?;
        crate::perf::record_io_duration(
            "core_store",
            "format_writer_stage_existing_core_object_range",
            &path,
            length,
            started_at.elapsed(),
        );
        Ok((path, hash, length))
    }

    async fn commit_format_coremeta_mutations(
        &self,
        mutations: Vec<CoreMetaMutation>,
        prepared: Option<PreparedFormatCoreMetaCommit>,
    ) -> Result<Vec<String>> {
        let Some(prepared) = prepared else {
            if !mutations.is_empty() {
                bail!("CoreFormatWriter lost its prepared CoreMeta commit plan");
            }
            return Ok(Vec::new());
        };
        let ops = mutations
            .iter()
            .map(CoreMetaMutation::as_batch_op)
            .collect::<Vec<_>>();
        let outcomes = self
            .commit_coremeta_root_groups(&prepared.transaction_id, &ops, &prepared.publications)
            .await?;
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.certificate_hash)
            .collect())
    }

    fn prepare_format_coremeta_commit(
        &self,
        mutations: &[CoreMetaMutation],
        declarations: &[WriterRootPublication],
    ) -> Result<Option<PreparedFormatCoreMetaCommit>> {
        let prepared = build_format_coremeta_commit_plan(mutations, declarations)?;
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        let ops = mutations
            .iter()
            .map(CoreMetaMutation::as_batch_op)
            .collect::<Vec<_>>();
        let encoded_rows = self.encode_coremeta_batch_ops(&ops)?;
        validate_encoded_format_rows(mutations, &encoded_rows)?;
        Ok(Some(prepared))
    }
}

fn build_format_coremeta_commit_plan(
    mutations: &[CoreMetaMutation],
    declarations: &[WriterRootPublication],
) -> Result<Option<PreparedFormatCoreMetaCommit>> {
    let Some(first) = mutations.first() else {
        if !declarations.is_empty() {
            bail!("CoreFormatWriter output declares roots without CoreMeta mutations");
        }
        return Ok(None);
    };

    let mut declarations_by_root = BTreeMap::new();
    let mut roots_by_hash = BTreeMap::new();
    let mut coordinator_count = 0_usize;
    for declaration in declarations {
        let root_anchor_key = declaration.root_anchor_key().to_string();
        let root_key_hash = core_meta_root_key_hash(&root_anchor_key);
        if let Some(existing) = roots_by_hash.insert(root_key_hash.clone(), root_anchor_key.clone())
            && existing != root_anchor_key
        {
            bail!("CoreFormatWriter canonical root anchor keys collide at {root_key_hash}");
        }
        if declarations_by_root
            .insert(root_anchor_key.clone(), declaration)
            .is_some()
        {
            bail!("CoreFormatWriter output declares root {root_anchor_key} more than once");
        }
        coordinator_count += usize::from(declaration.is_transaction_coordinator());
    }

    let transaction_id = first.transaction_id.clone();
    let mut root_generations = BTreeMap::new();
    let mut used_roots = BTreeSet::new();
    for mutation in mutations {
        mutation.validate()?;
        if mutation.transaction_id != transaction_id {
            bail!("CoreFormatWriter output spans multiple transaction ids");
        }
        let Some(root_anchor_key) = mutation.scope().root_anchor_key() else {
            continue;
        };
        let post_root_generation = mutation
            .scope()
            .post_root_generation()
            .expect("rooted mutation scopes always have a generation");
        if !declarations_by_root.contains_key(root_anchor_key) {
            bail!(
                "CoreFormatWriter mutation root {root_anchor_key} has no canonical publication declaration"
            );
        }
        if let Some(existing) = root_generations.insert(root_anchor_key, post_root_generation)
            && existing != post_root_generation
        {
            bail!(
                "CoreFormatWriter root {root_anchor_key} spans multiple generations in one build output"
            );
        }
        used_roots.insert(root_anchor_key);
    }

    if used_roots.len() != declarations_by_root.len() {
        let unused = declarations_by_root
            .keys()
            .find(|root| !used_roots.contains(root.as_str()))
            .expect("different root counts imply an unused declaration");
        bail!("CoreFormatWriter output declares unused root {unused}");
    }
    if used_roots.is_empty() {
        if coordinator_count != 0 {
            bail!("CoreFormatWriter local-only output must not declare a coordinator root");
        }
    } else if coordinator_count != 1 {
        bail!("CoreFormatWriter rooted output must declare exactly one coordinator root");
    }

    let mut publications = Vec::with_capacity(declarations_by_root.len());
    for declaration in declarations_by_root.into_values() {
        if declaration.is_transaction_coordinator()
            && !declaration
                .writer_families()
                .contains(&WriterFamily::CoreControl)
        {
            bail!("CoreFormatWriter coordinator root must include the core_control writer family");
        }
        let writer_families = declaration
            .writer_families()
            .iter()
            .map(|family| family.as_str().to_string())
            .collect();
        let mut publication = CoreMetaRootPublication::with_writer_families(
            declaration.root_anchor_key(),
            writer_families,
        );
        if declaration.is_transaction_coordinator() {
            publication = publication.coordinator();
        }
        publications.push(publication);
    }

    Ok(Some(PreparedFormatCoreMetaCommit {
        transaction_id,
        publications,
    }))
}

fn validate_encoded_format_rows(
    mutations: &[CoreMetaMutation],
    rows: &[CoreMetaEncodedOwnedRow],
) -> Result<()> {
    if mutations.len() != rows.len() {
        bail!("CoreFormatWriter CoreMeta row encoding changed the mutation count");
    }
    for (mutation, row) in mutations.iter().zip(rows) {
        if row.visibility_state != CoreMetaVisibilityState::Committed {
            bail!("CoreFormatWriter may only publish committed CoreMeta rows");
        }
        match mutation.scope().root_anchor_key() {
            Some(root_anchor_key) => {
                if row.root_key_hash != core_meta_root_key_hash(root_anchor_key)
                    || Some(row.root_generation) != mutation.scope().post_root_generation()
                {
                    bail!(
                        "CoreFormatWriter encoded row does not match its canonical root declaration"
                    );
                }
            }
            None => {
                if !row.root_key_hash.is_empty() || row.root_generation != 0 {
                    bail!("CoreFormatWriter local mutation encoded into an undeclared rooted row");
                }
            }
        }
    }
    Ok(())
}

fn normalise_sha256_hash(hash: &str) -> Result<String> {
    if hash.starts_with("sha256:") {
        Ok(hash.to_string())
    } else {
        Ok(format!("sha256:{hash}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::writer::CoreMetaMutationScope;

    fn rooted_common(
        root_anchor_key: &str,
        generation: u64,
        transaction_id: &str,
    ) -> CoreMetaRowCommonProto {
        CoreMetaRowCommonProto {
            realm_id: "test".to_string(),
            root_key_hash: core_meta_root_key_hash(root_anchor_key),
            root_generation: generation,
            transaction_id: transaction_id.to_string(),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: 1,
            payload_schema_version: 1,
        }
    }

    fn rooted_mutation(
        root_anchor_key: &str,
        generation: u64,
        transaction_id: &str,
        tuple_key: &[u8],
    ) -> CoreMetaMutation {
        CoreMetaMutation::put(
            CF_INDEX_ROWS,
            TABLE_INDEX_ROW,
            tuple_key.to_vec(),
            b"payload".to_vec(),
            Some(rooted_common(root_anchor_key, generation, transaction_id)),
            CoreMetaMutationScope::rooted(root_anchor_key, generation).expect("rooted scope"),
            transaction_id.to_string(),
        )
        .expect("rooted mutation")
    }

    fn coordinator(root_anchor_key: &str) -> WriterRootPublication {
        WriterRootPublication::new(
            root_anchor_key,
            vec![WriterFamily::CoreControl, WriterFamily::TypedMetadata],
        )
        .expect("root publication")
        .coordinator()
    }

    #[test]
    fn format_plan_preserves_canonical_publication_descriptor() {
        let root_anchor_key = "bucket/acme/index/orders";
        let mutations = vec![rooted_mutation(
            root_anchor_key,
            4,
            "format-write-1",
            b"orders/current",
        )];
        let declarations = vec![coordinator(root_anchor_key)];

        let plan = build_format_coremeta_commit_plan(&mutations, &declarations)
            .expect("valid plan")
            .expect("non-empty plan");
        assert_eq!(plan.transaction_id, "format-write-1");
        assert_eq!(plan.publications.len(), 1);
        assert_eq!(plan.publications[0].root_anchor_key, root_anchor_key);
        assert_eq!(
            plan.publications[0].writer_families,
            vec!["core_control".to_string(), "typed_index".to_string()]
        );
        assert!(plan.publications[0].transaction_coordinator);
    }

    #[test]
    fn format_plan_requires_exactly_one_coordinator() {
        let root_anchor_key = "bucket/acme/index/orders";
        let mutations = vec![rooted_mutation(
            root_anchor_key,
            1,
            "format-write-2",
            b"orders/current",
        )];
        let no_coordinator = vec![
            WriterRootPublication::new(root_anchor_key, vec![WriterFamily::TypedMetadata])
                .expect("root publication"),
        ];
        assert!(build_format_coremeta_commit_plan(&mutations, &no_coordinator).is_err());

        let second_root = "bucket/acme/index/customers";
        let mutations = vec![
            rooted_mutation(root_anchor_key, 1, "format-write-2", b"orders/current"),
            rooted_mutation(second_root, 1, "format-write-2", b"customers/current"),
        ];
        let two_coordinators = vec![coordinator(root_anchor_key), coordinator(second_root)];
        assert!(build_format_coremeta_commit_plan(&mutations, &two_coordinators).is_err());
    }

    #[test]
    fn format_plan_rejects_multiple_generations_for_one_root() {
        let root_anchor_key = "bucket/acme/index/orders";
        let mutations = vec![
            rooted_mutation(root_anchor_key, 4, "format-write-3", b"orders/first"),
            rooted_mutation(root_anchor_key, 5, "format-write-3", b"orders/second"),
        ];
        assert!(
            build_format_coremeta_commit_plan(&mutations, &[coordinator(root_anchor_key)]).is_err()
        );
    }
}
