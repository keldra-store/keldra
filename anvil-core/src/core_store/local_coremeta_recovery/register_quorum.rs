use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) enum RootRegisterQuorumResolution {
    Committed,
    CommittedConflict { anchor_record: Vec<u8> },
    DefinitivelyAbsent,
    Indeterminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::core_store::local) enum RootRegisterGenerationResolution {
    Committed { anchor_record: Vec<u8> },
    DefinitivelyAbsent,
    Indeterminate,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct RegisterCandidateKey {
    pub(super) anchor_record: Vec<u8>,
    pub(super) cohort_hash: String,
    pub(super) cohort_nodes: Vec<String>,
    pub(super) placement_epoch: u64,
}

#[derive(Debug, Default)]
struct RegisterCandidate {
    replicas: BTreeMap<String, u32>,
}

impl RegisterCandidate {
    fn distinct_replica_count(&self) -> usize {
        self.replicas
            .values()
            .copied()
            .collect::<BTreeSet<_>>()
            .len()
            .min(self.replicas.len())
    }
}

impl CoreStore {
    /// Resolves the physical commit decision for one exact root-register
    /// generation. A successful read of "not found" is evidence of absence;
    /// an unreachable peer is not.
    pub(in crate::core_store::local) async fn resolve_root_register_quorum(
        &self,
        peers: &[RecoveryPeer],
        authoritative_cohort: Option<&[String]>,
        root_key_hash: &str,
        generation: u64,
        expected_anchor: &[u8],
    ) -> Result<RootRegisterQuorumResolution> {
        validate_expected_anchor(root_key_hash, generation, expected_anchor)?;
        let resolution = self
            .resolve_root_register_generation(
                peers,
                authoritative_cohort,
                root_key_hash,
                generation,
            )
            .await?;
        Ok(expected_root_register_resolution(
            resolution,
            expected_anchor,
        ))
    }

    /// Resolves whichever physical commit decision, if any, won for one root
    /// generation. This is used when a recovering publisher persisted its
    /// intent before quorum outcomes were recorded and therefore cannot yet
    /// reconstruct the exact expected anchor bytes.
    pub(in crate::core_store::local) async fn resolve_root_register_generation(
        &self,
        peers: &[RecoveryPeer],
        authoritative_cohort: Option<&[String]>,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<RootRegisterGenerationResolution> {
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;

        let mut candidates = BTreeMap::<RegisterCandidateKey, RegisterCandidate>::new();
        let configured_nodes = match authoritative_cohort {
            Some(cohort) => validate_authoritative_cohort(cohort, profile.replica_count)?,
            None => peers
                .iter()
                .map(|peer| peer.node_id.clone())
                .chain(std::iter::once(self.node_identity.node_id.clone()))
                .collect(),
        };
        let mut responding_nodes = BTreeSet::new();
        if configured_nodes.contains(&self.node_identity.node_id) {
            responding_nodes.insert(self.node_identity.node_id.clone());
            if let Some(shard) = self
                .read_exact_root_register_shard(root_key_hash, generation)
                .await?
            {
                let read = RootAnchorRead {
                    root_key_hash: shard.root_key_hash,
                    generation: shard.root_generation,
                    root_anchor_record: shard.root_anchor_record,
                    root_anchor_hash: shard.root_anchor_hash,
                    shard_index: u32::from(shard.shard_index),
                    register_cohort_node_ids: shard.register_cohort_nodes,
                    register_cohort_hash: shard.register_cohort_hash,
                    placement_epoch: shard.placement_epoch,
                };
                validate_recovery_root_anchor_read_for_node(
                    &self.node_identity.node_id,
                    root_key_hash,
                    generation,
                    &read,
                )?;
                validate_authoritative_read(authoritative_cohort, &read)?;
                record_candidate(&mut candidates, &self.node_identity.node_id, read);
            }
        }

        if let Some(resolution) = terminal_root_register_generation_resolution(
            &candidates,
            &configured_nodes,
            &responding_nodes,
            profile.replica_count,
            profile.prepare_quorum,
            root_key_hash,
            generation,
        )? {
            return Ok(resolution);
        }

        let mut pending = FuturesUnordered::new();
        for peer in peers
            .iter()
            .filter(|peer| configured_nodes.contains(&peer.node_id))
            .cloned()
        {
            let root_key_hash = root_key_hash.to_string();
            pending.push(async move {
                let result = self
                    .read_exact_root_replica(&peer, &root_key_hash, generation, false)
                    .await;
                (peer, result)
            });
        }
        while let Some((peer, result)) = pending.next().await {
            match result {
                Ok(read) => {
                    responding_nodes.insert(peer.node_id.clone());
                    if let Some(read) = read {
                        validate_recovery_root_anchor_read(
                            &peer,
                            root_key_hash,
                            generation,
                            &read,
                        )?;
                        validate_authoritative_read(authoritative_cohort, &read)?;
                        record_candidate(&mut candidates, &peer.node_id, read);
                    }
                }
                Err(_) => {}
            }
            if let Some(resolution) = terminal_root_register_generation_resolution(
                &candidates,
                &configured_nodes,
                &responding_nodes,
                profile.replica_count,
                profile.prepare_quorum,
                root_key_hash,
                generation,
            )? {
                return Ok(resolution);
            }
        }

        classify_root_register_generation(
            &candidates,
            &configured_nodes,
            &responding_nodes,
            profile.replica_count,
            profile.prepare_quorum,
            root_key_hash,
            generation,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn terminal_root_register_generation_resolution(
    candidates: &BTreeMap<RegisterCandidateKey, RegisterCandidate>,
    configured_nodes: &BTreeSet<String>,
    responding_nodes: &BTreeSet<String>,
    replica_count: usize,
    prepare_quorum: usize,
    root_key_hash: &str,
    generation: u64,
) -> Result<Option<RootRegisterGenerationResolution>> {
    let resolution = classify_root_register_generation(
        candidates,
        configured_nodes,
        responding_nodes,
        replica_count,
        prepare_quorum,
        root_key_hash,
        generation,
    )?;
    Ok((resolution != RootRegisterGenerationResolution::Indeterminate).then_some(resolution))
}

fn validate_authoritative_cohort(
    cohort: &[String],
    replica_count: usize,
) -> Result<BTreeSet<String>> {
    let nodes = cohort.iter().cloned().collect::<BTreeSet<_>>();
    if nodes.len() != replica_count || cohort.len() != replica_count {
        bail!("root-register authoritative cohort does not match the quorum profile");
    }
    if nodes.iter().any(|node| {
        node.trim().is_empty() || crate::mesh_lifecycle::is_synthetic_control_node_id(node)
    }) {
        bail!("root-register authoritative cohort contains an invalid node");
    }
    Ok(nodes)
}

fn validate_authoritative_read(
    authoritative_cohort: Option<&[String]>,
    read: &RootAnchorRead,
) -> Result<()> {
    let Some(authoritative_cohort) = authoritative_cohort else {
        return Ok(());
    };
    if read.register_cohort_node_ids != authoritative_cohort {
        bail!("root-register shard conflicts with the authoritative physical cohort");
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn classify_root_register_quorum(
    candidates: &BTreeMap<RegisterCandidateKey, RegisterCandidate>,
    configured_nodes: &BTreeSet<String>,
    responding_nodes: &BTreeSet<String>,
    replica_count: usize,
    prepare_quorum: usize,
    expected_anchor: &[u8],
    root_key_hash: &str,
    generation: u64,
) -> Result<RootRegisterQuorumResolution> {
    Ok(expected_root_register_resolution(
        classify_root_register_generation(
            candidates,
            configured_nodes,
            responding_nodes,
            replica_count,
            prepare_quorum,
            root_key_hash,
            generation,
        )?,
        expected_anchor,
    ))
}

fn classify_root_register_generation(
    candidates: &BTreeMap<RegisterCandidateKey, RegisterCandidate>,
    configured_nodes: &BTreeSet<String>,
    responding_nodes: &BTreeSet<String>,
    replica_count: usize,
    prepare_quorum: usize,
    root_key_hash: &str,
    generation: u64,
) -> Result<RootRegisterGenerationResolution> {
    let committed = candidates
        .iter()
        .filter(|(key, candidate)| {
            valid_distributed_cohort(key, replica_count)
                && candidate.distinct_replica_count() >= prepare_quorum
        })
        .collect::<Vec<_>>();
    if committed.len() > 1 {
        bail!(
            "root-register recovery found conflicting physical quorums: root={root_key_hash} generation={generation}"
        );
    }
    if let Some((candidate, _)) = committed.first() {
        return Ok(RootRegisterGenerationResolution::Committed {
            anchor_record: candidate.anchor_record.clone(),
        });
    }

    let any_candidate_can_reach_quorum = candidates.iter().any(|(key, candidate)| {
        valid_distributed_cohort(key, replica_count)
            && candidate.distinct_replica_count().saturating_add(
                key.cohort_nodes
                    .iter()
                    .filter(|node| !responding_nodes.contains(*node))
                    .count(),
            ) >= prepare_quorum
    });
    let expected_candidate_can_appear =
        configured_nodes.difference(responding_nodes).count() >= prepare_quorum;
    if any_candidate_can_reach_quorum || expected_candidate_can_appear {
        Ok(RootRegisterGenerationResolution::Indeterminate)
    } else {
        Ok(RootRegisterGenerationResolution::DefinitivelyAbsent)
    }
}

fn expected_root_register_resolution(
    resolution: RootRegisterGenerationResolution,
    expected_anchor: &[u8],
) -> RootRegisterQuorumResolution {
    match resolution {
        RootRegisterGenerationResolution::Committed { anchor_record }
            if anchor_record == expected_anchor =>
        {
            RootRegisterQuorumResolution::Committed
        }
        RootRegisterGenerationResolution::Committed { anchor_record } => {
            RootRegisterQuorumResolution::CommittedConflict { anchor_record }
        }
        RootRegisterGenerationResolution::DefinitivelyAbsent => {
            RootRegisterQuorumResolution::DefinitivelyAbsent
        }
        RootRegisterGenerationResolution::Indeterminate => {
            RootRegisterQuorumResolution::Indeterminate
        }
    }
}

fn valid_distributed_cohort(candidate: &RegisterCandidateKey, replica_count: usize) -> bool {
    candidate.cohort_nodes.len() == replica_count
        && candidate
            .cohort_nodes
            .iter()
            .all(|node| !crate::mesh_lifecycle::is_synthetic_control_node_id(node))
}

fn validate_expected_anchor(
    root_key_hash: &str,
    generation: u64,
    expected_anchor: &[u8],
) -> Result<()> {
    let anchor = decode_root_anchor_record(expected_anchor)?;
    validate_root_anchor_record(&anchor)?;
    if anchor.root_key_hash != root_key_hash || anchor.root_generation != generation {
        bail!("root-register expected anchor scope is invalid");
    }
    Ok(())
}

fn record_candidate(
    candidates: &mut BTreeMap<RegisterCandidateKey, RegisterCandidate>,
    node_id: &str,
    read: RootAnchorRead,
) {
    let shard_index = read.shard_index;
    candidates
        .entry(register_candidate_key(&read))
        .or_default()
        .replicas
        .insert(node_id.to_string(), shard_index);
}

pub(super) fn register_candidate_key(read: &RootAnchorRead) -> RegisterCandidateKey {
    RegisterCandidateKey {
        anchor_record: read.root_anchor_record.clone(),
        cohort_hash: read.register_cohort_hash.clone(),
        cohort_nodes: read.register_cohort_node_ids.clone(),
        placement_epoch: read.placement_epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn candidate(
        anchor: &[u8],
        replicas: &[(&str, u32)],
    ) -> (RegisterCandidateKey, RegisterCandidate) {
        (
            RegisterCandidateKey {
                anchor_record: anchor.to_vec(),
                cohort_hash: "cohort".to_string(),
                cohort_nodes: vec!["node-a".into(), "node-b".into(), "node-c".into()],
                placement_epoch: 1,
            },
            RegisterCandidate {
                replicas: replicas
                    .iter()
                    .map(|(node, shard)| ((*node).to_string(), *shard))
                    .collect(),
            },
        )
    }

    #[test]
    fn distinct_replica_count_rejects_duplicate_shard_indexes() {
        let candidate = RegisterCandidate {
            replicas: BTreeMap::from([
                ("node-a".to_string(), 0),
                ("node-b".to_string(), 0),
                ("node-c".to_string(), 2),
            ]),
        };
        assert_eq!(candidate.distinct_replica_count(), 2);
    }

    #[test]
    fn matching_q2_is_committed() {
        let candidates = BTreeMap::from([candidate(b"expected", &[("node-a", 0), ("node-b", 1)])]);
        let resolution = classify_root_register_quorum(
            &candidates,
            &nodes(&["node-a", "node-b", "node-c"]),
            &nodes(&["node-a", "node-b"]),
            3,
            2,
            b"expected",
            "sha256:test",
            2,
        )
        .unwrap();
        assert_eq!(resolution, RootRegisterQuorumResolution::Committed);
    }

    #[test]
    fn generation_resolution_returns_committed_anchor_without_an_expected_value() {
        let candidates = BTreeMap::from([candidate(b"winner", &[("node-a", 0), ("node-b", 1)])]);
        let resolution = classify_root_register_generation(
            &candidates,
            &nodes(&["node-a", "node-b", "node-c"]),
            &nodes(&["node-a", "node-b"]),
            3,
            2,
            "sha256:test",
            2,
        )
        .unwrap();
        assert_eq!(
            resolution,
            RootRegisterGenerationResolution::Committed {
                anchor_record: b"winner".to_vec(),
            }
        );
    }

    #[test]
    fn one_shard_and_one_unreachable_cohort_member_is_indeterminate() {
        let candidates = BTreeMap::from([candidate(b"expected", &[("node-a", 0)])]);
        let resolution = classify_root_register_quorum(
            &candidates,
            &nodes(&["node-a", "node-b", "node-c"]),
            &nodes(&["node-a", "node-b"]),
            3,
            2,
            b"expected",
            "sha256:test",
            2,
        )
        .unwrap();
        assert_eq!(resolution, RootRegisterQuorumResolution::Indeterminate);
    }

    #[test]
    fn two_confirmed_absences_make_q2_definitively_absent() {
        let resolution = classify_root_register_quorum(
            &BTreeMap::new(),
            &nodes(&["node-a", "node-b", "node-c"]),
            &nodes(&["node-a", "node-b"]),
            3,
            2,
            b"expected",
            "sha256:test",
            2,
        )
        .unwrap();
        assert_eq!(resolution, RootRegisterQuorumResolution::DefinitivelyAbsent);
    }

    #[test]
    fn committed_conflicting_value_is_reported_to_the_caller() {
        let candidates =
            BTreeMap::from([candidate(b"conflicting", &[("node-a", 0), ("node-b", 1)])]);
        let resolution = classify_root_register_quorum(
            &candidates,
            &nodes(&["node-a", "node-b", "node-c"]),
            &nodes(&["node-a", "node-b"]),
            3,
            2,
            b"expected",
            "sha256:test",
            2,
        )
        .unwrap();
        assert_eq!(
            resolution,
            RootRegisterQuorumResolution::CommittedConflict {
                anchor_record: b"conflicting".to_vec(),
            }
        );
    }

    #[test]
    fn authoritative_cohort_requires_exact_distinct_physical_replicas() {
        assert_eq!(
            validate_authoritative_cohort(&["node-a".into(), "node-b".into(), "node-c".into()], 3,)
                .unwrap(),
            nodes(&["node-a", "node-b", "node-c"])
        );
        assert!(
            validate_authoritative_cohort(&["node-a".into(), "node-a".into(), "node-c".into()], 3,)
                .is_err()
        );
        assert!(
            validate_authoritative_cohort(
                &[
                    "local-control-node-1".into(),
                    "node-b".into(),
                    "node-c".into(),
                ],
                3,
            )
            .is_err()
        );
    }
}
