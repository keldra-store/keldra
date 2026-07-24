use super::super::*;
use prost::Message;
use std::io::Write;

const QUARANTINE_INTENT_SCHEMA: &str = "anvil.root_register.quarantine_intent.v2";
const MAX_QUARANTINE_INTENTS: usize = 8_192;
const MAX_ACTIVE_REGISTER_GENERATIONS: usize = 8_192;
const MAX_REGISTER_CHILD_DIRECTORIES: usize = 8_192;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RootRegisterQuarantineIntent {
    pub(super) root_key_hash: String,
    pub(super) synthetic_generation: u64,
    pub(super) activation_topology_hash: String,
    pub(super) activation_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SyntheticRootRegisterGeneration {
    pub(super) root_key_hash: String,
    pub(super) generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct RootRegisterQuarantineIntentProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    root_key_hash: String,
    #[prost(uint64, tag = "3")]
    synthetic_generation: u64,
    #[prost(string, tag = "4")]
    activation_topology_hash: String,
    #[prost(uint64, tag = "5")]
    activation_generation: u64,
}

pub(super) async fn synthetic_root_register_inventory(
    store: &CoreStore,
) -> Result<Vec<SyntheticRootRegisterGeneration>> {
    let blocks = store.storage.core_store_blocks_path();
    tokio::task::spawn_blocking(move || synthetic_root_register_inventory_at_path(&blocks))
        .await
        .map_err(|error| anyhow!("root-register inventory task failed: {error}"))?
}

pub(super) async fn quarantine_synthetic_root_register(
    store: &CoreStore,
    intent: RootRegisterQuarantineIntent,
) -> Result<()> {
    validate_intent(&intent)?;
    let blocks = store.storage.core_store_blocks_path();
    tokio::task::spawn_blocking(move || persist_and_apply_intent(&blocks, &intent))
        .await
        .map_err(|error| anyhow!("root-register quarantine task failed: {error}"))?
}
pub(super) async fn resume_root_register_quarantine_intents(
    store: &CoreStore,
    activation: Option<(&str, u64)>,
) -> Result<()> {
    let blocks = store.storage.core_store_blocks_path();
    let activation = activation.map(|(hash, generation)| (hash.to_string(), generation));
    tokio::task::spawn_blocking(move || {
        let intent_root = blocks.join("register-quarantine-intents");
        let mut paths = Vec::new();
        collect_intent_paths(&intent_root, &mut paths)?;
        if paths.len() > MAX_QUARANTINE_INTENTS {
            bail!("root-register quarantine intent count exceeds its safety bound");
        }
        paths.sort();
        for path in paths {
            let intent = decode_intent(&std::fs::read(&path)?)?;
            let Some((activation_hash, activation_generation)) = activation.as_ref() else {
                bail!("root-register quarantine intent exists without canonical activation");
            };
            if intent.activation_topology_hash != *activation_hash
                || intent.activation_generation != *activation_generation
            {
                bail!("root-register quarantine intent does not match canonical activation");
            }
            apply_persisted_intent(&blocks, &path, &intent)?;
        }
        Ok(())
    })
    .await
    .map_err(|error| anyhow!("root-register quarantine recovery task failed: {error}"))?
}

fn persist_and_apply_intent(
    blocks: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<()> {
    let path = quarantine_intent_path(blocks, intent)?;
    let bytes = encode_intent(intent);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    if path.exists() {
        if std::fs::read(&path)? != bytes {
            bail!("root-register quarantine intent changed across retry");
        }
    } else {
        let temporary = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4().simple()));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &path)?;
        sync_parent(&path)?;
    }
    apply_persisted_intent(blocks, &path, intent)
}

fn apply_persisted_intent(
    blocks: &std::path::Path,
    intent_path: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<()> {
    validate_intent(intent)?;
    let source = active_generation_path(blocks, intent)?;
    let target = quarantine_generation_path(blocks, intent)?;
    match (source.exists(), target.exists()) {
        (true, false) => {
            let inspection = inspect_generation_directory(&source, intent)?;
            if !inspection.is_synthetic() {
                bail!("root-register quarantine source is not a synthetic cohort");
            }
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&source, &target)?;
            sync_parent(&source)?;
            sync_parent(&target)?;
        }
        (false, true) => {
            let inspection = inspect_generation_directory(&target, intent)?;
            if !inspection.is_synthetic() {
                bail!("root-register quarantine target is not a synthetic cohort");
            }
        }
        (true, true) => bail!("root-register quarantine source and target both exist"),
        (false, false) => bail!("root-register quarantine source and target are both missing"),
    }
    std::fs::remove_file(intent_path)?;
    sync_parent(intent_path)?;
    Ok(())
}

fn inspect_generation_directory(
    path: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<super::super::local_root_register::RootRegisterGenerationInspection> {
    super::super::local_root_register::inspect_root_register_generation(
        path.to_path_buf(),
        &intent.root_key_hash,
        intent.synthetic_generation,
    )?
    .ok_or_else(|| anyhow!("root-register quarantine source has no shards"))
}

fn active_generation_path(
    blocks: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<PathBuf> {
    register_generation_path(
        blocks.join("register"),
        &intent.root_key_hash,
        intent.synthetic_generation,
    )
}

fn quarantine_generation_path(
    blocks: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<PathBuf> {
    register_generation_path(
        blocks.join("register-quarantine"),
        &intent.root_key_hash,
        intent.synthetic_generation,
    )
}

fn register_generation_path(
    root: PathBuf,
    root_key_hash: &str,
    generation: u64,
) -> Result<PathBuf> {
    let hash_hex = root_key_hash
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("root-register quarantine hash has no sha256 prefix"))?;
    let prefix = hash_hex.get(..2).unwrap_or("00");
    let partition =
        super::super::local_root_register::root_register_partition_id_for_hash(root_key_hash)?;
    Ok(root
        .join(format!("{partition:020}"))
        .join(prefix)
        .join(hash_hex)
        .join(format!("generation-{generation:020}")))
}

fn quarantine_intent_path(
    blocks: &std::path::Path,
    intent: &RootRegisterQuarantineIntent,
) -> Result<PathBuf> {
    let hash_hex = intent
        .root_key_hash
        .strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("root-register quarantine hash has no sha256 prefix"))?;
    Ok(blocks
        .join("register-quarantine-intents")
        .join(hash_hex)
        .join(format!(
            "generation-{:020}.anqi",
            intent.synthetic_generation
        )))
}

fn collect_intent_paths(root: &std::path::Path, output: &mut Vec<PathBuf>) -> Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            collect_intent_paths(&path, output)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("anqi") {
            output.push(path);
        }
    }
    Ok(())
}

fn synthetic_root_register_inventory_at_path(
    blocks: &std::path::Path,
) -> Result<Vec<SyntheticRootRegisterGeneration>> {
    let register_root = blocks.join("register");
    let mut scopes = Vec::new();
    let mut visited_generations = 0usize;
    for partition_path in sorted_directory_paths(&register_root)? {
        let partition_name = strict_file_name(&partition_path, "root-register partition")?;
        if partition_name.len() != 20 || !partition_name.bytes().all(|byte| byte.is_ascii_digit()) {
            bail!("root-register partition directory is not canonical");
        }
        let partition = partition_name
            .parse::<u64>()
            .context("parse root-register partition directory")?;
        for prefix_path in sorted_directory_paths(&partition_path)? {
            let prefix = strict_file_name(&prefix_path, "root-register hash prefix")?;
            if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                bail!("root-register hash prefix directory is not canonical");
            }
            for hash_path in sorted_directory_paths(&prefix_path)? {
                let hash_hex = strict_file_name(&hash_path, "root-register root hash")?;
                if hash_hex.len() != 64
                    || !hash_hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    || &hash_hex[..2] != prefix
                {
                    bail!("root-register root hash directory is not canonical");
                }
                let root_key_hash = format!("sha256:{hash_hex}");
                validate_hash(&root_key_hash, "root-register inventory root key hash")?;
                if root_register_partition_id_for_hash(&root_key_hash)? != partition {
                    bail!("root-register root hash is stored in the wrong partition");
                }
                for generation_path in sorted_directory_paths(&hash_path)? {
                    visited_generations = visited_generations.saturating_add(1);
                    if visited_generations > MAX_ACTIVE_REGISTER_GENERATIONS {
                        bail!("active root-register inventory exceeds its safety bound");
                    }
                    let generation_name =
                        strict_file_name(&generation_path, "root-register generation")?;
                    let generation_digits =
                        generation_name.strip_prefix("generation-").ok_or_else(|| {
                            anyhow!("root-register generation directory is not canonical")
                        })?;
                    if generation_digits.len() != 20
                        || !generation_digits.bytes().all(|byte| byte.is_ascii_digit())
                    {
                        bail!("root-register generation directory is not canonical");
                    }
                    let generation = generation_digits
                        .parse::<u64>()
                        .context("parse root-register generation directory")?;
                    if generation == 0 {
                        bail!("root-register inventory contains generation zero");
                    }
                    let inspection = inspect_root_register_generation(
                        generation_path,
                        &root_key_hash,
                        generation,
                    )?;
                    if inspection.is_some_and(|inspection| inspection.is_synthetic()) {
                        scopes.push(SyntheticRootRegisterGeneration {
                            root_key_hash: root_key_hash.clone(),
                            generation,
                        });
                    }
                }
            }
        }
    }
    scopes.sort();
    Ok(scopes)
}

fn sorted_directory_paths(root: &std::path::Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            bail!("root-register inventory contains a non-directory entry");
        }
        if paths.len() >= MAX_REGISTER_CHILD_DIRECTORIES {
            bail!("root-register directory fanout exceeds its safety bound");
        }
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

fn strict_file_name<'a>(path: &'a std::path::Path, context: &str) -> Result<&'a str> {
    path.file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("{context} directory name is not valid UTF-8"))
}

fn encode_intent(intent: &RootRegisterQuarantineIntent) -> Vec<u8> {
    encode_deterministic_proto(&RootRegisterQuarantineIntentProto {
        schema: QUARANTINE_INTENT_SCHEMA.to_string(),
        root_key_hash: intent.root_key_hash.clone(),
        synthetic_generation: intent.synthetic_generation,
        activation_topology_hash: intent.activation_topology_hash.clone(),
        activation_generation: intent.activation_generation,
    })
}

fn decode_intent(bytes: &[u8]) -> Result<RootRegisterQuarantineIntent> {
    let proto = decode_deterministic_proto::<RootRegisterQuarantineIntentProto>(
        bytes,
        "root-register quarantine intent",
    )?;
    if proto.schema != QUARANTINE_INTENT_SCHEMA {
        bail!("root-register quarantine intent schema mismatch");
    }
    let intent = RootRegisterQuarantineIntent {
        root_key_hash: proto.root_key_hash,
        synthetic_generation: proto.synthetic_generation,
        activation_topology_hash: proto.activation_topology_hash,
        activation_generation: proto.activation_generation,
    };
    validate_intent(&intent)?;
    Ok(intent)
}

fn validate_intent(intent: &RootRegisterQuarantineIntent) -> Result<()> {
    validate_hash(
        &intent.root_key_hash,
        "root-register quarantine root key hash",
    )?;
    validate_hash(
        &intent.activation_topology_hash,
        "root-register quarantine activation topology hash",
    )?;
    if intent.synthetic_generation == 0 || intent.activation_generation == 0 {
        bail!("root-register quarantine generations must be nonzero");
    }
    Ok(())
}

fn sync_parent(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent()
        && parent.exists()
    {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_with_cohort(
        intent: &RootRegisterQuarantineIntent,
        cohort: Vec<String>,
    ) -> RootRegisterShard {
        let anchor = b"synthetic-anchor".to_vec();
        RootRegisterShard {
            root_partition_id:
                super::super::super::local_root_register::root_register_partition_id_for_hash(
                    &intent.root_key_hash,
                )
                .unwrap(),
            root_key_hash: intent.root_key_hash.clone(),
            root_generation: intent.synthetic_generation,
            shard_index: 0,
            register_cohort_hash: root_register_cohort_hash(
                &intent.root_key_hash,
                intent.synthetic_generation,
                &cohort,
            ),
            register_cohort_nodes: cohort,
            placement_epoch: 1,
            created_at_unix_nanos: 1,
            root_anchor_hash: format!("sha256:{}", sha256_hex(&anchor)),
            root_anchor_record: anchor,
        }
    }

    fn synthetic_shard(intent: &RootRegisterQuarantineIntent) -> RootRegisterShard {
        shard_with_cohort(
            intent,
            vec![
                "local-control-node-1".to_string(),
                "local-control-node-2".to_string(),
                "local-control-node-3".to_string(),
            ],
        )
    }

    fn write_shard(
        blocks: &std::path::Path,
        intent: &RootRegisterQuarantineIntent,
        shard: &RootRegisterShard,
    ) {
        let generation_path = active_generation_path(blocks, intent).unwrap();
        std::fs::create_dir_all(&generation_path).unwrap();
        std::fs::write(
            generation_path.join(format!("shard-{}.anr", shard.shard_index)),
            super::super::super::local_root_register::encode_root_register_shard(shard).unwrap(),
        )
        .unwrap();
    }

    fn sample_intent() -> RootRegisterQuarantineIntent {
        RootRegisterQuarantineIntent {
            root_key_hash: format!("sha256:{}", sha256_hex(b"root")),
            synthetic_generation: 4,
            activation_topology_hash: format!("sha256:{}", sha256_hex(b"topology")),
            activation_generation: 1,
        }
    }

    #[test]
    fn quarantine_intent_resumes_after_the_atomic_rename() {
        let temporary = tempfile::tempdir().unwrap();
        let blocks = temporary.path();
        let intent = sample_intent();
        let source = active_generation_path(blocks, &intent).unwrap();
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(
            source.join("shard-0.anr"),
            super::super::super::local_root_register::encode_root_register_shard(&synthetic_shard(
                &intent,
            ))
            .unwrap(),
        )
        .unwrap();
        let intent_path = quarantine_intent_path(blocks, &intent).unwrap();
        std::fs::create_dir_all(intent_path.parent().unwrap()).unwrap();
        std::fs::write(&intent_path, encode_intent(&intent)).unwrap();
        let target = quarantine_generation_path(blocks, &intent).unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::rename(&source, &target).unwrap();

        apply_persisted_intent(blocks, &intent_path, &intent).unwrap();
        assert!(!source.exists());
        assert!(target.exists());
        assert!(!intent_path.exists());
    }

    #[test]
    fn quarantine_rejects_zero_activation_generation() {
        let mut intent = sample_intent();
        intent.activation_generation = 0;
        assert!(validate_intent(&intent).is_err());
    }

    #[test]
    fn inventory_discovers_orphaned_synthetic_generation_and_preserves_real_cohort() {
        let temporary = tempfile::tempdir().unwrap();
        let blocks = temporary.path();
        let synthetic = sample_intent();
        write_shard(blocks, &synthetic, &synthetic_shard(&synthetic));

        let mut real = sample_intent();
        real.root_key_hash = format!("sha256:{}", sha256_hex(b"real-root"));
        real.synthetic_generation = 7;
        let real_shard = shard_with_cohort(
            &real,
            vec!["node-a".into(), "node-b".into(), "node-c".into()],
        );
        write_shard(blocks, &real, &real_shard);

        assert_eq!(
            synthetic_root_register_inventory_at_path(blocks).unwrap(),
            vec![SyntheticRootRegisterGeneration {
                root_key_hash: synthetic.root_key_hash,
                generation: synthetic.synthetic_generation,
            }]
        );
    }
}
