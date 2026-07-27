use crate::{
    config::Config, mvcc_bootstrap::MvccSubsystem, persistence::Persistence,
    personaldb_signing::PersonalDbProtocolKeyring,
};
use anyhow::{Context, Result, bail};
use base64::{Engine, engine::general_purpose::STANDARD};
use personaldb_protocol::{
    Ed25519ProtocolSigner, Ed25519PublicKey, KeyGeneration, KeyTrustPolicy, ProtocolSigner,
    PublicKeyTrustRecord, PublicKeyTrustStore, SignaturePurpose,
};
use std::sync::Arc;

const GROUP_CONTROL_PKCS8_B64: &str =
    "MC4CAQAwBQYDK2VwBCIEIBERERERERERERERERERERERERERERERERERERERERER";
const GROUP_CONTROL_PUBLIC_B64U: &str = "0EqyMnQrtKs6E2i9RhXk5tAiSrcaAWuvhSCjMsl3hzc";
const SNAPSHOT_PKCS8_B64: &str = "MC4CAQAwBQYDK2VwBCIEICIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIi";
const SNAPSHOT_PUBLIC_B64U: &str = "oJql9HpnWYAv-VX43C0qFKXJnSO-l_hkEn_5ODRVpPA";
const WITNESS_PKCS8_B64: &str = "MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz";
const WITNESS_PUBLIC_B64U: &str = "F8t5-ytBIPKx7GXkGY1uCLKOgT_rAeSkAIObheGAgM4";

/// Builds the production persistence stack with a real, single-node MVCC
/// subsystem for unit tests.
///
/// `Persistence::new` intentionally remains a synchronous construction step
/// because production installs the cluster-scoped MVCC runtime during
/// `AppState` startup. Unit tests that exercise persistence directly must
/// mirror that second step instead of relying on a legacy non-MVCC write path.
pub(crate) async fn persistence_with_mvcc(config: &Config) -> Result<Persistence> {
    let persistence = Persistence::new(config)?;
    let mvcc_config = single_node_mvcc_test_config(config, persistence.owner_node_id());
    let core_store = persistence
        .core_store()
        .await
        .context("open test persistence CoreStore")?;
    let mvcc = Arc::new(
        MvccSubsystem::bootstrap(&mvcc_config, core_store.core_meta_database())
            .await
            .context("bootstrap test persistence MVCC subsystem")?,
    );
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        while !mvcc.consensus.is_leader() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .context("test persistence MVCC node did not become ready")?;
    persistence
        .install_mvcc(mvcc)
        .context("install test persistence MVCC subsystem")?;
    Ok(persistence)
}

/// Builds the direct-persistence MVCC fixture and installs one writable mesh
/// placement matching the node configuration.
///
/// Keep this opt-in: lifecycle tests use [`persistence_with_mvcc`] to exercise
/// topology creation and fail-closed admission from an intentionally empty
/// projection.
pub(crate) async fn persistence_with_active_topology(config: &Config) -> Result<Persistence> {
    let persistence = persistence_with_mvcc(config).await?;
    install_active_test_topology(&persistence, config).await?;
    Ok(persistence)
}

pub(crate) async fn install_active_test_topology(
    persistence: &Persistence,
    config: &Config,
) -> Result<()> {
    use crate::mesh_lifecycle::{
        CreateRegionDescriptor, LifecycleState, NodeCapability, RegisterCellDescriptor,
        RegisterNodeDescriptor,
    };

    let mesh_id = nonempty_test_value(&config.mesh_id, "default");
    let region_id = nonempty_test_value(&config.region, "default");
    let cell_id = nonempty_test_value(&config.cell_id, "default");
    let node_id = persistence.owner_node_id().to_string();
    let failure_domain = nonempty_test_value(&config.mvcc_failure_domain, "test-local");
    let public_api_addr = nonempty_test_value(&config.public_api_addr, &node_id);
    let virtual_host_suffix = format!("{region_id}.test.invalid");
    let region_input = CreateRegionDescriptor {
        mesh_id: mesh_id.clone(),
        region: region_id.clone(),
        public_base_url: format!("https://{virtual_host_suffix}"),
        virtual_host_suffix,
        placement_weight: 100,
        default_cell: Some(cell_id.clone()),
    };
    let cell_input = RegisterCellDescriptor {
        mesh_id: mesh_id.clone(),
        region: region_id.clone(),
        cell_id: cell_id.clone(),
        placement_weight: 100,
        failure_domain,
    };
    let core_store = persistence
        .core_store()
        .await
        .context("open CoreStore for active test topology")?;
    let receipt_signing_public_key = core_store.local_receipt_signing_public_key();
    let node_input = RegisterNodeDescriptor {
        mesh_id,
        node_id: node_id.clone(),
        region: region_id.clone(),
        cell_id: cell_id.clone(),
        receipt_signing_public_key: receipt_signing_public_key.clone(),
        public_api_addr,
        capabilities: vec![
            NodeCapability::Object,
            NodeCapability::Index,
            NodeCapability::PersonalDb,
            NodeCapability::Metadata,
            NodeCapability::Gateway,
            NodeCapability::Admin,
        ],
        capacity_json: "{}".to_string(),
    };

    let mut region = match persistence
        .list_region_descriptors()
        .await?
        .into_iter()
        .find(|descriptor| descriptor.region == region_id)
    {
        Some(descriptor) => {
            if descriptor.mesh_id != region_input.mesh_id
                || descriptor.public_base_url != region_input.public_base_url
                || descriptor.virtual_host_suffix != region_input.virtual_host_suffix
                || descriptor.placement_weight != region_input.placement_weight
                || descriptor.default_cell != region_input.default_cell
            {
                bail!("existing active-test region descriptor does not match the fixture");
            }
            descriptor
        }
        None => persistence
            .create_region_descriptor(region_input)
            .await
            .context("create active-test region")?,
    };
    require_joining_or_active("region", &region.region, region.state)?;

    let mut cell = match persistence
        .list_cell_descriptors(Some(&region_id))
        .await?
        .into_iter()
        .find(|descriptor| descriptor.cell_id == cell_id)
    {
        Some(descriptor) => {
            if descriptor.mesh_id != cell_input.mesh_id
                || descriptor.region != cell_input.region
                || descriptor.placement_weight != cell_input.placement_weight
                || descriptor.failure_domain != cell_input.failure_domain
            {
                bail!("existing active-test cell descriptor does not match the fixture");
            }
            descriptor
        }
        None => persistence
            .register_cell_descriptor(cell_input)
            .await
            .context("create active-test cell")?,
    };
    require_joining_or_active("cell", &cell.cell_id, cell.state)?;
    if cell.state == LifecycleState::Joining {
        cell = persistence
            .transition_cell_descriptor(
                &region_id,
                &cell_id,
                cell.generation,
                LifecycleState::Active,
            )
            .await
            .context("activate active-test cell")?;
    }

    let expected_capacity_hash = crate::mesh_lifecycle::capacity_json_hash("{}")?;
    let mut node = match persistence
        .list_node_descriptors(Some(&region_id), Some(&cell_id))
        .await?
        .into_iter()
        .find(|descriptor| descriptor.node_id == node_id)
    {
        Some(descriptor) => {
            if descriptor.mesh_id != node_input.mesh_id
                || descriptor.region != node_input.region
                || descriptor.cell_id != node_input.cell_id
                || descriptor.receipt_signing_public_key != node_input.receipt_signing_public_key
                || descriptor.public_api_addr != node_input.public_api_addr
                || descriptor.capabilities != node_input.capabilities
                || descriptor.capacity_json_hash != expected_capacity_hash
            {
                bail!("existing active-test node descriptor does not match the fixture");
            }
            descriptor
        }
        None => persistence
            .register_node_descriptor(node_input)
            .await
            .context("create active-test node")?,
    };
    core_store
        .register_node_receipt_signing_public_key(&node_id, &receipt_signing_public_key)
        .context("bind active-test node to the local CoreStore receipt key")?;
    require_joining_or_active("node", &node.node_id, node.state)?;
    if node.state == LifecycleState::Joining {
        node = persistence
            .transition_node_descriptor(&node_id, node.generation, LifecycleState::Active, None)
            .await
            .context("activate active-test node")?;
    }

    if region.state == LifecycleState::Joining {
        region = persistence
            .transition_region_descriptor(&region_id, region.generation, LifecycleState::Active)
            .await
            .context("activate active-test region")?;
    }
    if region.state != LifecycleState::Active
        || cell.state != LifecycleState::Active
        || node.state != LifecycleState::Active
    {
        bail!("active test topology did not converge to writable state");
    }
    Ok(())
}

fn require_joining_or_active(
    resource_kind: &str,
    resource_id: &str,
    state: crate::mesh_lifecycle::LifecycleState,
) -> Result<()> {
    if matches!(
        state,
        crate::mesh_lifecycle::LifecycleState::Joining
            | crate::mesh_lifecycle::LifecycleState::Active
    ) {
        return Ok(());
    }
    bail!("{resource_kind} {resource_id} is {state:?}, not Joining or Active")
}

fn nonempty_test_value(value: &str, fallback: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

/// Normalizes legacy persistence fixtures into an isolated one-node MVCC
/// topology without changing the configuration used to construct Persistence.
///
/// Many persistence tests predate MVCC and intentionally specify only storage
/// and encryption settings. Their empty node identity and endpoint are not a
/// valid production topology, while any stale distributed settings would make
/// this helper wait for peers that the fixture never starts.
fn single_node_mvcc_test_config(config: &Config, persistence_node_id: &str) -> Config {
    let mut mvcc = config.clone();
    let fingerprint = blake3::hash(
        format!(
            "{}\0{}\0{}",
            config.storage_path, config.mvcc_cluster_id, persistence_node_id
        )
        .as_bytes(),
    )
    .to_hex()
    .to_string();
    let short_fingerprint = &fingerprint[..16];

    mvcc.node_id = if persistence_node_id.trim().is_empty() {
        format!("test-mvcc-node-{short_fingerprint}")
    } else {
        persistence_node_id.to_string()
    };
    if mvcc.mvcc_raft_node_id == 0 {
        mvcc.mvcc_raft_node_id = 1;
    }
    if mvcc.mvcc_node_incarnation == 0 {
        mvcc.mvcc_node_incarnation = 1;
    }
    if mvcc.mvcc_failure_domain.trim().is_empty() {
        mvcc.mvcc_failure_domain = "test-local".to_string();
    }
    if !valid_mvcc_cluster_id(&mvcc.mvcc_cluster_id) {
        mvcc.mvcc_cluster_id = format!("test-{short_fingerprint}");
    }

    // A stable, non-routable endpoint keeps tonic's URI validation honest
    // without relying on a listener that these direct-persistence tests do not
    // start. The storage-derived suffix also keeps concurrent fixtures
    // distinct.
    mvcc.public_api_addr = format!("http://mvcc-{short_fingerprint}.invalid");
    mvcc.mvcc_peers_json = "[]".to_string();
    mvcc.mvcc_bootstrap_membership = true;
    mvcc.mvcc_bundle_quorum_holders = 1;
    mvcc.mvcc_tolerated_failure_domains = 0;
    if mvcc.mvcc_rpc_timeout_ms == 0 {
        mvcc.mvcc_rpc_timeout_ms = 10_000;
    }
    mvcc.mvcc_node_connection_token = format!("test-mvcc-{fingerprint}");
    mvcc.allow_test_only_insecure_mvcc_transport = true;
    mvcc
}

fn valid_mvcc_cluster_id(cluster_id: &str) -> bool {
    !cluster_id.is_empty()
        && cluster_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn personaldb_protocol_keyring() -> PersonalDbProtocolKeyring {
    let signers = [
        signer(
            GROUP_CONTROL_PKCS8_B64,
            GROUP_CONTROL_PUBLIC_B64U,
            SignaturePurpose::GroupControl,
        ),
        signer(
            SNAPSHOT_PKCS8_B64,
            SNAPSHOT_PUBLIC_B64U,
            SignaturePurpose::Snapshot,
        ),
        signer(
            WITNESS_PKCS8_B64,
            WITNESS_PUBLIC_B64U,
            SignaturePurpose::Witness,
        ),
    ];
    let trust_store = PublicKeyTrustStore::from_records(
        signers.iter().map(|signer| signer.trust_record().clone()),
    )
    .unwrap();
    PersonalDbProtocolKeyring::new_test_only(trust_store, signers).unwrap()
}

fn signer(
    private_key_b64: &str,
    public_key_b64u: &str,
    purpose: SignaturePurpose,
) -> Arc<dyn ProtocolSigner> {
    let private_key = STANDARD.decode(private_key_b64).unwrap();
    let public_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(public_key_b64u)
        .unwrap();
    let public_key = Ed25519PublicKey::try_from(public_key.as_slice()).unwrap();
    let policy = KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0);
    let record = PublicKeyTrustRecord::new(public_key, policy);
    Arc::new(Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(&private_key, record).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_mvcc_fixture_normalizes_legacy_topology_stably() {
        let source = Config {
            storage_path: "/tmp/anvil-mvcc-fixture-a".to_string(),
            node_id: String::new(),
            public_api_addr: String::new(),
            mvcc_raft_node_id: 0,
            mvcc_node_incarnation: 0,
            mvcc_failure_domain: String::new(),
            mvcc_cluster_id: "invalid/cluster".to_string(),
            mvcc_peers_json: "not-json".to_string(),
            mvcc_bootstrap_membership: false,
            mvcc_bundle_quorum_holders: 3,
            mvcc_tolerated_failure_domains: 2,
            mvcc_rpc_timeout_ms: 0,
            mvcc_node_connection_token: String::new(),
            allow_test_only_insecure_mvcc_transport: false,
            ..Config::default()
        };

        let normalized = single_node_mvcc_test_config(&source, "persistence-node");
        let restarted = single_node_mvcc_test_config(&source, "persistence-node");

        assert_eq!(normalized, restarted);
        assert_eq!(normalized.node_id, "persistence-node");
        assert_eq!(normalized.mvcc_raft_node_id, 1);
        assert_eq!(normalized.mvcc_node_incarnation, 1);
        assert_eq!(normalized.mvcc_failure_domain, "test-local");
        assert!(normalized.mvcc_cluster_id.starts_with("test-"));
        assert!(normalized.public_api_addr.starts_with("http://mvcc-"));
        assert!(normalized.public_api_addr.ends_with(".invalid"));
        assert_eq!(normalized.mvcc_peers_json, "[]");
        assert!(normalized.mvcc_bootstrap_membership);
        assert_eq!(normalized.mvcc_bundle_quorum_holders, 1);
        assert_eq!(normalized.mvcc_tolerated_failure_domains, 0);
        assert_eq!(normalized.mvcc_rpc_timeout_ms, 10_000);
        assert!(
            normalized
                .mvcc_node_connection_token
                .starts_with("test-mvcc-")
        );
        assert!(normalized.allow_test_only_insecure_mvcc_transport);

        assert!(source.node_id.is_empty());
        assert_eq!(source.mvcc_peers_json, "not-json");
        assert!(!source.mvcc_bootstrap_membership);
    }
}
