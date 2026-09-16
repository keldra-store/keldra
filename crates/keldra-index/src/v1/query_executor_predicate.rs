//! Boolean execution retains dense segment provenance through current-gate admission.

use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_predicate<
    'a,
    L: QueryArtifactLoader + 'static,
    X: QueryPartitionExecutor,
>(
    loader: &'a mut L,
    executor: &'a X,
    manifest: &'a PartitionManifest,
    universe: &'a BTreeMap<StableDocumentKey, QueryDocumentGate>,
    contracts: &'a BTreeMap<FieldId, QueryFieldBinding>,
    membership_recipe: RecipeIdentity,
    predicate: &'a Predicate,
    minimum_document_exclusive: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &'a mut QueryBlockCredits,
    budget: &'a mut Budget,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<DenseCandidateSet, IndexError>> + Send + 'a>,
> {
    Box::pin(async move {
        let result = match predicate {
            Predicate::And(children) => {
                let mut children = children.iter();
                let first = children.next().ok_or_else(|| {
                    IndexError::InvalidQuery("Boolean predicate requires a child".into())
                })?;
                let mut output = evaluate_predicate(
                    loader,
                    executor,
                    manifest,
                    universe,
                    contracts,
                    membership_recipe,
                    first,
                    minimum_document_exclusive,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                for child in children {
                    let next = evaluate_predicate(
                        loader,
                        executor,
                        manifest,
                        universe,
                        contracts,
                        membership_recipe,
                        child,
                        minimum_document_exclusive,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?;
                    output.intersect(next, credits, budget)?;
                }
                output
            }
            Predicate::Or(children) => {
                let mut output = DenseCandidateSet::new();
                for child in children {
                    let next = evaluate_predicate(
                        loader,
                        executor,
                        manifest,
                        universe,
                        contracts,
                        membership_recipe,
                        child,
                        minimum_document_exclusive,
                        block_limits,
                        credits,
                        budget,
                    )
                    .await?;
                    output.union(next, credits, budget)?;
                    budget.candidates(output.len())?;
                }
                output
            }
            Predicate::Not(child) => {
                let excluded = evaluate_predicate(
                    loader,
                    executor,
                    manifest,
                    universe,
                    contracts,
                    membership_recipe,
                    child,
                    minimum_document_exclusive,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                budget.reserve_heap(credits, candidate_set_bytes(universe.len())?)?;
                let output: DenseCandidateSet = universe
                    .iter()
                    .filter_map(|(key, gate)| {
                        (gate.live
                            && minimum_document_exclusive.is_none_or(|resume| *key > resume)
                            && !excluded.contains(key))
                        .then_some(*key)
                    })
                    .collect();
                budget.release_heap(
                    credits,
                    candidate_set_bytes(universe.len())?
                        .checked_sub(candidate_set_bytes(output.len())?)
                        .ok_or(IndexError::Integrity)?,
                )?;
                excluded.release(credits, budget)?;
                output
            }
            leaf => {
                evaluate_leaf(
                    loader,
                    executor,
                    manifest,
                    universe,
                    contracts,
                    membership_recipe,
                    leaf,
                    minimum_document_exclusive,
                    block_limits,
                    credits,
                    budget,
                )
                .await?
            }
        };
        budget.candidates(result.len())?;
        Ok(result)
    })
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_leaf<L: QueryArtifactLoader + 'static, X: QueryPartitionExecutor>(
    loader: &mut L,
    executor: &X,
    manifest: &PartitionManifest,
    universe: &BTreeMap<StableDocumentKey, QueryDocumentGate>,
    contracts: &BTreeMap<FieldId, QueryFieldBinding>,
    membership_recipe: RecipeIdentity,
    predicate: &Predicate,
    minimum_document_exclusive: Option<StableDocumentKey>,
    block_limits: QueryBlockLimits,
    credits: &mut QueryBlockCredits,
    budget: &mut Budget,
) -> Result<DenseCandidateSet, IndexError> {
    let field_id =
        leaf_field(predicate).ok_or_else(|| IndexError::InvalidQuery("expected leaf".into()))?;
    let binding = contracts
        .get(&field_id)
        .ok_or_else(|| IndexError::InvalidQuery("query field is not bound".into()))?;
    validate_leaf_capability(&binding.field, predicate)?;
    if matches!(predicate, Predicate::Exists { .. }) {
        let presence = load_latest_gates(
            loader,
            executor,
            manifest,
            binding.recipe,
            QueryBlockKind::Presence,
            block_limits,
            credits,
            budget,
        )
        .await?;
        let key_bytes = document_key_set_bytes(presence.len())?;
        budget.reserve_heap(credits, key_bytes)?;
        let mut keys = presence
            .iter()
            .filter_map(|(key, gate)| gate.live.then_some(*key))
            .collect::<BTreeSet<_>>();
        if let Some(resume) = minimum_document_exclusive {
            keys.retain(|key| *key > resume);
        }
        let memberships = if universe.is_empty() {
            Some(
                load_latest_gates_for_keys(
                    loader,
                    executor,
                    manifest,
                    membership_recipe,
                    QueryBlockKind::Gate,
                    &keys,
                    block_limits,
                    credits,
                    budget,
                )
                .await?,
            )
        } else {
            None
        };
        budget.reserve_heap(credits, candidate_set_bytes(keys.len())?)?;
        let output_bound = keys.len();
        let presence_bytes = presence.values().try_fold(0usize, |total, gate| {
            total
                .checked_add(resident_gate_bytes(gate)?)
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let (output, membership_bytes): (DenseCandidateSet, usize) =
            if let Some(memberships) = memberships {
                let (memberships, membership_bytes) = memberships.into_parts();
                let output = keys
                    .into_iter()
                    .zip(memberships)
                    .filter_map(|(key, membership)| {
                        membership
                            .is_some_and(|membership| membership.live)
                            .then_some(key)
                    })
                    .collect();
                (output, membership_bytes)
            } else {
                let output = keys
                    .into_iter()
                    .filter(|key| universe.get(key).is_some_and(|membership| membership.live))
                    .collect();
                (output, 0)
            };
        budget.release_heap(
            credits,
            candidate_set_bytes(output_bound)?
                .checked_sub(candidate_set_bytes(output.len())?)
                .ok_or(IndexError::Integrity)?,
        )?;
        budget.release_heap(credits, key_bytes)?;
        drop(presence);
        budget.release_heap(credits, presence_bytes)?;
        budget.release_heap(credits, membership_bytes)?;
        return Ok(output);
    }
    let mut candidates = match predicate {
        Predicate::Equal { value, .. } => {
            seek_terms(
                loader,
                executor,
                manifest,
                binding.recipe,
                std::slice::from_ref(value),
                None,
                minimum_document_exclusive,
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::In { values, .. } => {
            seek_terms(
                loader,
                executor,
                manifest,
                binding.recipe,
                values,
                None,
                minimum_document_exclusive,
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::Prefix { prefix, .. } => {
            seek_terms(
                loader,
                executor,
                manifest,
                binding.recipe,
                &[],
                Some(prefix),
                minimum_document_exclusive,
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        Predicate::FullText { text, .. } | Predicate::Phrase { text, .. } => {
            budget.reserve_heap(credits, text.len())?;
            let terms = analyze_typed_json_text(text)
                .into_iter()
                .map(ScalarValue::String)
                .collect::<Vec<_>>();
            if terms.is_empty() {
                budget.release_heap(credits, text.len())?;
                return Ok(DenseCandidateSet::new());
            }
            let postings = seek_term_postings(
                loader,
                executor,
                manifest,
                binding.recipe,
                &terms,
                None,
                minimum_document_exclusive,
                block_limits,
                credits,
                budget,
            )
            .await?;
            let mut found = intersect_term_candidates(&postings, &terms, credits, budget)?;
            if matches!(predicate, Predicate::Phrase { .. }) {
                budget.reserve_heap(
                    credits,
                    found
                        .len()
                        .checked_mul(80)
                        .and_then(|bytes| bytes.checked_add(512))
                        .ok_or(IndexError::OffsetOverflow)?,
                )?;
                let mut phrase_candidates: BTreeSet<_> = found.keys().copied().collect();
                let phrase_candidate_bytes = found
                    .len()
                    .checked_mul(80)
                    .and_then(|bytes| bytes.checked_add(512))
                    .ok_or(IndexError::OffsetOverflow)?;
                verify_phrase(
                    loader,
                    executor,
                    manifest,
                    binding.recipe,
                    &terms,
                    &postings,
                    &mut phrase_candidates,
                    block_limits,
                    credits,
                    budget,
                )
                .await?;
                let before = found.len();
                found.retain(|key, _| phrase_candidates.contains(key));
                budget.release_heap(
                    credits,
                    candidate_set_bytes(before)?
                        .checked_sub(candidate_set_bytes(found.len())?)
                        .ok_or(IndexError::Integrity)?,
                )?;
                drop(phrase_candidates);
                budget.release_heap(credits, phrase_candidate_bytes)?;
            }
            let posting_bytes = term_posting_map_bytes(&postings)?;
            drop(postings);
            budget.release_heap(credits, posting_bytes)?;
            budget.release_heap(credits, text.len())?;
            found
        }
        Predicate::Range { lower, upper, .. } => {
            seek_range(
                loader,
                executor,
                manifest,
                binding.recipe,
                lower.as_ref(),
                upper.as_ref(),
                block_limits,
                credits,
                budget,
            )
            .await?
        }
        _ => return Err(IndexError::InvalidQuery("expected Typed JSON leaf".into())),
    };
    if let Some(resume) = minimum_document_exclusive {
        let before = candidates.len();
        candidates.retain(|key, _| *key > resume);
        budget.release_heap(
            credits,
            candidate_set_bytes(before)?
                .checked_sub(candidate_set_bytes(candidates.len())?)
                .ok_or(IndexError::Integrity)?,
        )?;
    }
    let candidate_key_bytes = document_key_set_bytes(candidates.len())?;
    budget.reserve_heap(credits, candidate_key_bytes)?;
    let candidate_keys = candidates.keys().copied().collect::<BTreeSet<_>>();
    let presence = load_latest_gates_for_keys(
        loader,
        executor,
        manifest,
        binding.recipe,
        QueryBlockKind::Presence,
        &candidate_keys,
        block_limits,
        credits,
        budget,
    )
    .await?;
    let memberships = if universe.is_empty() {
        Some(
            load_latest_gates_for_keys(
                loader,
                executor,
                manifest,
                membership_recipe,
                QueryBlockKind::Gate,
                &candidate_keys,
                block_limits,
                credits,
                budget,
            )
            .await?,
        )
    } else {
        None
    };
    let (presence, presence_bytes) = presence.into_parts();
    let (memberships, membership_bytes) = memberships
        .map(AlignedGates::into_parts)
        .map_or((None, 0), |(gates, bytes)| (Some(gates), bytes));
    if presence.len() != candidates.len()
        || memberships
            .as_ref()
            .is_some_and(|memberships| memberships.len() != candidates.len())
    {
        return Err(IndexError::Integrity);
    }
    let mut ordinal = 0usize;
    let before = candidates.len();
    candidates.retain(|key, candidate| {
        let current = candidate_is_current(
            memberships
                .as_ref()
                .map_or_else(|| universe.get(key), |gates| gates[ordinal].as_ref()),
            presence[ordinal].as_ref(),
            candidate.material_source_version,
        );
        ordinal += 1;
        current
    });
    budget.release_heap(
        credits,
        candidate_set_bytes(before)?
            .checked_sub(candidate_set_bytes(candidates.len())?)
            .ok_or(IndexError::Integrity)?,
    )?;
    drop(presence);
    drop(memberships);
    budget.release_heap(credits, presence_bytes)?;
    budget.release_heap(credits, membership_bytes)?;
    drop(candidate_keys);
    budget.release_heap(credits, candidate_key_bytes)?;
    Ok(DenseCandidateSet {
        entries: candidates,
    })
}
