use std::collections::{BTreeMap, BTreeSet};

use anvil::error_codes::AnvilErrorCode;
use anvil_test_utils::isolated_docker_test_cluster;
use prost::Message;
use sha2::{Digest, Sha256};

use super::common::*;

const BLOCK_SHARD_MAGIC: &[u8; 8] = b"ANBLK\n\0\0";
const BLOCK_SHARD_VERSION: u16 = 1;
const BLOCK_SHARD_SCHEMA: &str = "anvil.core.block_shard.v1";

#[tokio::test]
async fn docker_equal_peers_replicate_and_converge() {
    let cluster = isolated_docker_test_cluster("equal-peer-convergence", "test-region-1").await;
    let fixture = create_fixture(&cluster, "equal-peer-convergence").await;
    let peer_one = cluster.equal_peer(1);
    let shard_counts_before = shard_counts(&cluster).await;

    let inline_key = "inline-convergence";
    let inline_content = deterministic_bytes(4 * 1024, 11);
    let inline_observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        inline_key,
        &inline_content,
        &MutationIdentity::unique("inline-convergence"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            inline_key,
            &inline_observation,
            DISTRIBUTED_WAIT,
        )
        .await;

    let large_key = "large-convergence";
    let large_content = deterministic_bytes(512 * 1024, 29);
    let large_observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        large_key,
        &large_content,
        &MutationIdentity::unique("large-convergence"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            large_key,
            &large_observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster
        .wait_for_all_peer_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            large_key,
            &large_observation,
            DISTRIBUTED_WAIT,
        )
        .await;

    for peer in cluster.equal_peers() {
        let inline = get_object_at(&peer.grpc_addr, &fixture, inline_key)
            .await
            .unwrap_or_else(|status| panic!("peer {} inline read: {status:?}", peer.ordinal));
        assert_eq!(inline, inline_content, "peer {} inline bytes", peer.ordinal);
        let large = get_object_at(&peer.grpc_addr, &fixture, large_key)
            .await
            .unwrap_or_else(|status| panic!("peer {} large read: {status:?}", peer.ordinal));
        assert_eq!(large, large_content, "peer {} large bytes", peer.ordinal);
    }

    let shard_counts_after = shard_counts(&cluster).await;
    for (index, (before, after)) in shard_counts_before
        .iter()
        .zip(&shard_counts_after)
        .enumerate()
    {
        assert!(
            after > before,
            "equal peer {} received no EC4+2 shard: before={before}, after={after}",
            index + 1
        );
    }
    let cells = cluster
        .equal_peers()
        .into_iter()
        .map(|peer| peer.cell_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(cells.len(), 6, "EC4+2 placements span six cells");
}

#[tokio::test]
async fn docker_ec42_degraded_read_write_threshold_and_repair() {
    let cluster = isolated_docker_test_cluster("ec42-repair", "test-region-1").await;
    let fixture = create_fixture(&cluster, "ec42-repair").await;
    let peer_one = cluster.equal_peer(1);
    let object_key = "degraded-object";
    let content = deterministic_bytes(768 * 1024, 47);
    let node_six_shards_before = cluster.node_block_shard_paths(6).await;
    let response = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        object_key,
        &content,
        &MutationIdentity::unique("degraded-object"),
    )
    .await
    .unwrap_or_else(|status| panic!("put {object_key}: {status:?}"));
    let observation =
        anvil_test_utils::DockerObjectObservation::from_put_response(&response, content.len());
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    let original_repair_targets =
        wait_for_original_repair_targets(&cluster, &node_six_shards_before, DISTRIBUTED_WAIT).await;
    let original_repair_paths = original_repair_targets
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();

    cluster.stop_node(4).await;
    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("EC4+2 read with one missing shard"),
        content
    );

    let failed_key = "write-without-six-acks";
    let failed_write = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        failed_key,
        &deterministic_bytes(256 * 1024, 53),
        &MutationIdentity::unique(failed_key),
    )
    .await
    .expect_err("EC4+2 publication must fail without all six shard receipts");
    assert_object_shard_quorum_unavailable(&failed_write, "EC4+2 degraded write");
    let failed_read = get_object_at(&peer_one.grpc_addr, &fixture, failed_key)
        .await
        .expect_err("failed EC4+2 publication must remain invisible");
    assert_eq!(failed_read.code(), tonic::Code::NotFound);

    cluster.stop_node(5).await;
    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("EC4+2 read with two missing shards"),
        content
    );

    cluster.stop_node(6).await;
    let below_quorum = get_object_at(&peer_one.grpc_addr, &fixture, object_key)
        .await
        .expect_err("EC4+2 read must fail below four shards");
    assert_object_shard_quorum_unavailable(&below_quorum, "EC4+2 below-quorum read");

    cluster.start_node(4).await;
    cluster.start_node(5).await;
    cluster
        .erase_stopped_node_block_shards(6, &original_repair_paths)
        .await;
    cluster.start_node(6).await;

    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("degraded read remains available while repair runs"),
        content
    );
    cluster
        .wait_for_node_block_shard_paths(6, &original_repair_paths, DISTRIBUTED_WAIT)
        .await;
    let repaired_targets = observe_exact_shards(&cluster, 6, &original_repair_paths).await;
    assert_repaired_shard_identities(&original_repair_targets, &repaired_targets);

    cluster.stop_node(4).await;
    cluster.stop_node(5).await;
    wait_for_metadata_replica_repaired_reads(
        &cluster,
        &fixture,
        object_key,
        &content,
        DISTRIBUTED_WAIT,
    )
    .await;
    cluster.start_node(4).await;
    cluster.start_node(5).await;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockShardObservation {
    path: String,
    node_id: String,
    block_id: String,
    erasure_set_id: String,
    shard_index: u16,
    erasure_profile_id: String,
    logical_file_id: String,
    logical_offset: u64,
    logical_length: u64,
    payload_plain_hash: String,
    payload_stored_hash: String,
    compression: String,
    encryption: String,
    placement_epoch: u64,
    boundary_summary_hash: String,
    boundary_values_b64: String,
    writer_family: String,
    created_by_mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct BlockShardHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    block_id: String,
    #[prost(string, tag = "3")]
    erasure_set_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    erasure_profile_id: String,
    #[prost(string, tag = "6")]
    logical_file_id: String,
    #[prost(uint64, tag = "7")]
    logical_offset: u64,
    #[prost(uint64, tag = "8")]
    logical_length: u64,
    #[prost(string, tag = "9")]
    payload_plain_hash: String,
    #[prost(string, tag = "10")]
    payload_stored_hash: String,
    #[prost(string, tag = "11")]
    compression: String,
    #[prost(string, tag = "12")]
    encryption: String,
    #[prost(uint64, tag = "13")]
    placement_epoch: u64,
    #[prost(string, tag = "14")]
    boundary_summary_hash: String,
    #[prost(string, tag = "15")]
    boundary_values_b64: String,
    #[prost(string, tag = "16")]
    writer_family: String,
    #[prost(string, tag = "17")]
    created_by_mutation_id: String,
}

async fn wait_for_original_repair_targets(
    cluster: &anvil_test_utils::DockerTestCluster,
    paths_before: &BTreeSet<String>,
    timeout: std::time::Duration,
) -> BTreeMap<String, BlockShardObservation> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let paths_after = cluster.node_block_shard_paths(6).await;
        let new_paths = paths_after
            .difference(paths_before)
            .cloned()
            .collect::<BTreeSet<_>>();
        let last_observations = observe_exact_shards(cluster, 6, &new_paths).await;
        let application_targets = last_observations
            .values()
            .filter(|shard| shard.writer_family == "object_blob")
            .collect::<Vec<_>>();
        let application_mutations = application_targets
            .iter()
            .map(|shard| logical_file_mutation_id(&shard.created_by_mutation_id))
            .collect::<BTreeSet<_>>();
        let expected_manifest_mutations = application_mutations
            .iter()
            .map(|mutation_id| manifest_mutation_id(mutation_id))
            .collect::<BTreeSet<_>>();
        let core_control_targets = last_observations
            .values()
            .filter(|shard| {
                shard.writer_family == "core_control"
                    && expected_manifest_mutations.contains(&shard.created_by_mutation_id)
            })
            .collect::<Vec<_>>();
        if !application_targets.is_empty() && !core_control_targets.is_empty() {
            assert_eq!(
                application_mutations.len(),
                1,
                "one logical-file publication must use one application mutation identity: {application_targets:#?}"
            );
            let targets = last_observations
                .into_iter()
                .filter(|(_, shard)| {
                    shard.writer_family == "object_blob"
                        || (shard.writer_family == "core_control"
                            && expected_manifest_mutations.contains(&shard.created_by_mutation_id))
                })
                .collect::<BTreeMap<_, _>>();
            assert!(
                targets.values().all(|shard| shard.placement_epoch > 0),
                "initial repair targets must have nonzero placement epochs: {targets:#?}"
            );
            return targets;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "peer 6 never exposed the new application block and its correlated CoreControl block after publication; before={paths_before:?}; observed={last_observations:#?}"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

fn manifest_mutation_id(application_mutation_id: &str) -> String {
    format!(
        "manifest_{}",
        hex::encode(Sha256::digest(application_mutation_id.as_bytes()))
    )
}

fn logical_file_mutation_id(block_mutation_id: &str) -> &str {
    let (logical_file_mutation_id, block_ordinal) =
        block_mutation_id.rsplit_once("-block-").unwrap_or_else(|| {
            panic!("logical-file block mutation lacks -block- suffix: {block_mutation_id}")
        });
    assert!(
        block_ordinal.len() == 6 && block_ordinal.bytes().all(|byte| byte.is_ascii_digit()),
        "logical-file block mutation has invalid block ordinal: {block_mutation_id}"
    );
    logical_file_mutation_id
}

async fn observe_exact_shards(
    cluster: &anvil_test_utils::DockerTestCluster,
    ordinal: u8,
    paths: &BTreeSet<String>,
) -> BTreeMap<String, BlockShardObservation> {
    let node_id = cluster.equal_peer(ordinal).node_id;
    let mut observations = BTreeMap::new();
    for path in paths {
        let bytes = cluster.node_block_shard_file(ordinal, path).await;
        let observation = decode_block_shard(path, &node_id, &bytes)
            .unwrap_or_else(|error| panic!("decode Docker block shard {path}: {error}"));
        assert!(
            observations.insert(path.clone(), observation).is_none(),
            "duplicate Docker block shard path {path}"
        );
    }
    observations
}

fn assert_repaired_shard_identities(
    original: &BTreeMap<String, BlockShardObservation>,
    repaired: &BTreeMap<String, BlockShardObservation>,
) {
    assert_eq!(
        repaired.keys().collect::<Vec<_>>(),
        original.keys().collect::<Vec<_>>(),
        "repair must recreate the exact original application and CoreControl shard identities"
    );
    for (path, before) in original {
        let after = &repaired[path];
        assert_eq!(after.path, before.path, "repaired shard path for {path}");
        assert_eq!(after.node_id, before.node_id, "replacement node for {path}");
        assert_eq!(after.block_id, before.block_id, "block identity for {path}");
        assert_eq!(
            after.erasure_set_id, before.erasure_set_id,
            "erasure set for {path}"
        );
        assert_eq!(
            after.shard_index, before.shard_index,
            "shard index for {path}"
        );
        assert_eq!(
            after.erasure_profile_id, before.erasure_profile_id,
            "erasure profile for {path}"
        );
        assert_eq!(
            after.logical_file_id, before.logical_file_id,
            "logical file identity for {path}"
        );
        assert_eq!(
            after.logical_offset, before.logical_offset,
            "logical offset for {path}"
        );
        assert_eq!(
            after.logical_length, before.logical_length,
            "logical length for {path}"
        );
        assert_eq!(
            after.payload_plain_hash, before.payload_plain_hash,
            "plain shard hash for {path}"
        );
        assert_eq!(
            after.payload_stored_hash, before.payload_stored_hash,
            "stored shard hash for {path}"
        );
        assert_eq!(
            after.compression, before.compression,
            "compression for {path}"
        );
        assert_eq!(after.encryption, before.encryption, "encryption for {path}");
        assert_eq!(
            after.boundary_summary_hash, before.boundary_summary_hash,
            "boundary summary for {path}"
        );
        assert_eq!(
            after.boundary_values_b64, before.boundary_values_b64,
            "boundary values for {path}"
        );
        assert_eq!(
            after.writer_family, before.writer_family,
            "writer family for {path}"
        );
        assert_eq!(
            after.created_by_mutation_id, before.created_by_mutation_id,
            "originating mutation for {path}"
        );
        assert!(
            after.placement_epoch > before.placement_epoch,
            "repair must advance the placement epoch for {path}: before={}, after={}",
            before.placement_epoch,
            after.placement_epoch
        );
    }
}

fn decode_block_shard(
    path: &str,
    node_id: &str,
    bytes: &[u8],
) -> Result<BlockShardObservation, String> {
    let mut offset = 0usize;
    let magic = read_bytes(bytes, &mut offset, BLOCK_SHARD_MAGIC.len())?;
    if magic != BLOCK_SHARD_MAGIC {
        return Err("invalid block-shard magic".to_string());
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != BLOCK_SHARD_VERSION {
        return Err(format!("unsupported block-shard version {version}"));
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let header_bytes = read_bytes(bytes, &mut offset, header_len)?;
    let header = BlockShardHeaderProto::decode(header_bytes)
        .map_err(|error| format!("decode deterministic block-shard header: {error}"))?;
    if header.encode_to_vec() != header_bytes {
        return Err("block-shard header is not deterministic protobuf".to_string());
    }
    if header.schema != BLOCK_SHARD_SCHEMA {
        return Err(format!("invalid block-shard schema {}", header.schema));
    }
    let payload_len = usize::try_from(read_u64_le(bytes, &mut offset)?)
        .map_err(|_| "block-shard payload length exceeds usize".to_string())?;
    let payload = read_bytes(bytes, &mut offset, payload_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    let mut crc_input = Vec::with_capacity(header_bytes.len() + payload.len());
    crc_input.extend_from_slice(header_bytes);
    crc_input.extend_from_slice(payload);
    let actual_crc = crc32c(&crc_input);
    if actual_crc != expected_crc {
        return Err(format!(
            "block-shard CRC32C mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
        ));
    }
    let file_hash_start = offset;
    let expected_file_hash = read_bytes(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        return Err("block-shard file has trailing bytes".to_string());
    }
    let actual_file_hash = Sha256::digest(&bytes[..file_hash_start]);
    if expected_file_hash != &actual_file_hash[..] {
        return Err("block-shard SHA-256 mismatch".to_string());
    }
    let payload_hash = format!("sha256:{}", hex::encode(Sha256::digest(payload)));
    if header.payload_plain_hash != payload_hash || header.payload_stored_hash != payload_hash {
        return Err(format!(
            "block-shard payload hash mismatch: payload={payload_hash}, plain={}, stored={}",
            header.payload_plain_hash, header.payload_stored_hash
        ));
    }
    if header.logical_length != payload.len() as u64 {
        return Err(format!(
            "block-shard logical length {} differs from payload length {}",
            header.logical_length,
            payload.len()
        ));
    }
    let shard_index = u16::try_from(header.shard_index)
        .map_err(|_| format!("block-shard index {} exceeds u16", header.shard_index))?;
    let expected_file_name = format!("shard-{shard_index:05}-{}.anb", header.block_id);
    if path.rsplit('/').next() != Some(expected_file_name.as_str()) {
        return Err(format!(
            "block-shard path does not match header identity: path={path}, expected={expected_file_name}"
        ));
    }
    if !path.contains(&format!("/{node_id}/")) {
        return Err(format!(
            "block-shard path does not identify replacement node {node_id}: {path}"
        ));
    }
    if header.placement_epoch == 0 {
        return Err("block-shard placement epoch is zero".to_string());
    }

    Ok(BlockShardObservation {
        path: path.to_string(),
        node_id: node_id.to_string(),
        block_id: header.block_id,
        erasure_set_id: header.erasure_set_id,
        shard_index,
        erasure_profile_id: header.erasure_profile_id,
        logical_file_id: header.logical_file_id,
        logical_offset: header.logical_offset,
        logical_length: header.logical_length,
        payload_plain_hash: header.payload_plain_hash,
        payload_stored_hash: header.payload_stored_hash,
        compression: header.compression,
        encryption: header.encryption,
        placement_epoch: header.placement_epoch,
        boundary_summary_hash: header.boundary_summary_hash,
        boundary_values_b64: header.boundary_values_b64,
        writer_family: header.writer_family,
        created_by_mutation_id: header.created_by_mutation_id,
    })
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "block-shard offset overflow".to_string())?;
    let value = bytes
        .get(*offset..end)
        .ok_or_else(|| "truncated block-shard file".to_string())?;
    *offset = end;
    Ok(value)
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Result<u16, String> {
    let raw = read_bytes(bytes, offset, 2)?;
    Ok(u16::from_le_bytes([raw[0], raw[1]]))
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32, String> {
    let raw = read_bytes(bytes, offset, 4)?;
    Ok(u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let raw = read_bytes(bytes, offset, 8)?;
    Ok(u64::from_le_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0_u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0_u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

// Full generation-level proof needs one bounded, admin-authorized inspection
// API keyed by `(object_hash, manifest_ref, block_id)`. Its response must expose
// the base and effective placements (shard index, node/cell/region, generation,
// placement epoch, shard hash), overlay root generation, replaced node, and
// repair-finding id as observed by each metadata replica. Existing public APIs
// expose completed repair findings but not the effective overlay generation.

async fn shard_counts(cluster: &anvil_test_utils::DockerTestCluster) -> Vec<u64> {
    let mut counts = Vec::new();
    for ordinal in 1..=6 {
        counts.push(cluster.node_block_shard_count(ordinal).await);
    }
    counts
}

fn assert_object_shard_quorum_unavailable(status: &tonic::Status, operation: &str) {
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "{operation} must be retryable: {status:?}"
    );
    let expected_prefix = format!("{}:", AnvilErrorCode::ObjectShardQuorumUnavailable);
    assert!(
        status.message().starts_with(&expected_prefix),
        "{operation} must expose {expected_prefix}: {status:?}"
    );
}

async fn wait_for_metadata_replica_repaired_reads(
    cluster: &anvil_test_utils::DockerTestCluster,
    fixture: &DistributedFixture,
    object_key: &str,
    expected_content: &[u8],
    timeout: std::time::Duration,
) {
    let peers = cluster.selected_metadata_replicas();
    let peer_ordinals = peers.iter().map(|peer| peer.ordinal).collect::<Vec<_>>();
    assert_eq!(
        peer_ordinals,
        vec![1, 2, 3],
        "EC4+2 repair convergence must cover metadata replicas 1-3"
    );
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut converged = true;
        let mut observations = Vec::with_capacity(peers.len());
        for peer in &peers {
            match get_object_at(&peer.grpc_addr, fixture, object_key).await {
                Ok(content) if content.as_slice() == expected_content => {
                    observations.push(format!(
                        "peer {}: repaired {} bytes",
                        peer.ordinal,
                        content.len()
                    ));
                }
                Ok(content) => {
                    converged = false;
                    observations.push(format!(
                        "peer {}: unexpected {} bytes",
                        peer.ordinal,
                        content.len()
                    ));
                }
                Err(status) => {
                    converged = false;
                    observations.push(format!(
                        "peer {}: code={:?} message={}",
                        peer.ordinal,
                        status.code(),
                        status.message()
                    ));
                }
            }
        }
        if converged {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "metadata replicas did not converge on an effective repaired manifest for {object_key}: {}",
                observations.join("; ")
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}
