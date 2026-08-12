//! Bounded pure-CPU ranking chunks for full-text and hybrid queries.

use std::collections::{BTreeMap, VecDeque};

use super::{Candidate, FullTextHit, TermDocumentMatch, phrase_matches, sort_hits};

pub(super) const RANK_CHUNK_DOCUMENTS: usize = 128;

pub(super) struct RawCandidate {
    pub(super) ordinal: u64,
    pub(super) matched: BTreeMap<String, TermDocumentMatch>,
}

pub(super) fn rank_candidates(
    candidates: Vec<RawCandidate>,
    phrase_terms: &[String],
    unique_term_count: usize,
    phrase: bool,
    match_all_terms: bool,
) -> VecDeque<Candidate> {
    candidates
        .into_iter()
        .filter_map(|candidate| {
            let accepted = if phrase {
                phrase_matches(phrase_terms, &candidate.matched)
            } else if match_all_terms {
                candidate.matched.len() == unique_term_count
            } else {
                !candidate.matched.is_empty()
            };
            accepted.then(|| Candidate {
                ordinal: candidate.ordinal,
                score: candidate
                    .matched
                    .values()
                    .flat_map(|entry| entry.fields.values())
                    .map(|field| {
                        let frequency = field.frequency as f32;
                        frequency * 2.2
                            / (frequency + 1.2 * (0.25 + 0.75 * field.length as f32 / 100.0))
                    })
                    .sum(),
                matched_terms: u32::try_from(candidate.matched.len()).unwrap_or(u32::MAX),
            })
        })
        .collect()
}

pub(super) fn merge_full_text_hits(
    mut retained: Vec<FullTextHit>,
    candidates: Vec<FullTextHit>,
    limit: usize,
) -> Vec<FullTextHit> {
    for hit in candidates {
        if retained
            .iter()
            .all(|existing| existing.document != hit.document)
        {
            retained.push(hit);
        }
    }
    sort_hits(&mut retained);
    retained.truncate(limit);
    retained
}
