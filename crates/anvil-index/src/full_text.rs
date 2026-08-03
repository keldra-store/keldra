//! Minimal Unicode-aware full-text index with positions and BM25-style scores.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    DocumentRef, IndexArtifacts, IndexDirectoryRead, IndexError, PagedMap, PagedMapBuilder,
};

pub const FULL_TEXT_FILE: &str = "full-text/postings.map";
const MAX_TOKEN_CHARS: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FullTextDocument {
    pub document: DocumentRef,
    pub fields: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TextPosting {
    document: DocumentRef,
    field: String,
    positions: Vec<u32>,
    field_length: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullTextQuery<'a> {
    pub text: &'a str,
    pub fields: &'a [String],
    pub phrase: bool,
    pub match_all_terms: bool,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FullTextHit {
    pub document: DocumentRef,
    pub score: f32,
    pub matched_terms: u32,
}

pub struct FullTextEngine;

impl FullTextEngine {
    pub fn build(
        documents: impl IntoIterator<Item = FullTextDocument>,
    ) -> Result<IndexArtifacts, IndexError> {
        let mut terms = BTreeMap::<String, Vec<TextPosting>>::new();
        for document in documents {
            for (field, text) in &document.fields {
                if field.is_empty() || field.contains('\0') {
                    return Err(IndexError::InvalidDefinition(
                        "full-text field names must be non-empty".into(),
                    ));
                }
                let tokens = tokenize(text);
                let field_length = u32::try_from(tokens.len()).unwrap_or(u32::MAX);
                let mut by_term = BTreeMap::<String, Vec<u32>>::new();
                for (term, position) in tokens {
                    by_term.entry(term).or_default().push(position);
                }
                for (term, positions) in by_term {
                    terms.entry(term).or_default().push(TextPosting {
                        document: document.document.clone(),
                        field: field.clone(),
                        positions,
                        field_length,
                    });
                }
            }
        }
        let mut map = PagedMapBuilder::default();
        for (term, mut postings) in terms {
            postings.sort_by(|left, right| {
                (&left.document, &left.field).cmp(&(&right.document, &right.field))
            });
            map.insert(
                term.into_bytes(),
                serde_json::to_vec(&postings)
                    .map_err(|error| IndexError::Encode(error.to_string()))?,
            )?;
        }
        let mut artifacts = IndexArtifacts::default();
        artifacts.insert(FULL_TEXT_FILE, map.finish()?)?;
        Ok(artifacts)
    }

    pub async fn query<D: IndexDirectoryRead>(
        directory: &D,
        query: FullTextQuery<'_>,
    ) -> Result<Vec<FullTextHit>, IndexError> {
        if query.limit == 0 {
            return Ok(Vec::new());
        }
        let tokenized_terms = tokenize(query.text)
            .into_iter()
            .map(|(term, _)| term)
            .collect::<Vec<_>>();
        let terms = if query.phrase {
            tokenized_terms
        } else {
            tokenized_terms
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        if terms.is_empty() {
            return Err(IndexError::InvalidQuery(
                "full-text query contains no indexable terms".into(),
            ));
        }
        let selected_fields = query.fields.iter().collect::<BTreeSet<_>>();
        let map = PagedMap::open(directory.open_file(FULL_TEXT_FILE).await?).await?;
        let mut by_term = Vec::with_capacity(terms.len());
        for term in &terms {
            let postings = map
                .get(term.as_bytes())
                .await?
                .map(|bytes| {
                    serde_json::from_slice::<Vec<TextPosting>>(&bytes)
                        .map_err(|error| IndexError::Decode(error.to_string()))
                })
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .filter(|posting| {
                    selected_fields.is_empty() || selected_fields.contains(&posting.field)
                })
                .collect::<Vec<_>>();
            by_term.push(postings);
        }
        if query.phrase {
            return phrase_hits(&by_term, query.limit);
        }
        ranked_hits(&by_term, query.match_all_terms, query.limit)
    }
}

fn tokenize(text: &str) -> Vec<(String, u32)> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut position = 0u32;
    let push = |current: &mut String, tokens: &mut Vec<(String, u32)>, position: &mut u32| {
        if current.is_empty() {
            return;
        }
        tokens.push((std::mem::take(current), *position));
        *position = position.saturating_add(1);
    };
    for character in text.chars().flat_map(char::to_lowercase) {
        if character.is_alphanumeric() || character == '_' {
            if current.chars().count() < MAX_TOKEN_CHARS {
                current.push(character);
            }
        } else {
            push(&mut current, &mut tokens, &mut position);
        }
    }
    push(&mut current, &mut tokens, &mut position);
    tokens
}

fn ranked_hits(
    by_term: &[Vec<TextPosting>],
    match_all_terms: bool,
    limit: usize,
) -> Result<Vec<FullTextHit>, IndexError> {
    let mut scores = BTreeMap::<DocumentRef, (f32, BTreeSet<usize>)>::new();
    let total_documents = by_term
        .iter()
        .flatten()
        .map(|posting| posting.document.clone())
        .collect::<BTreeSet<_>>()
        .len()
        .max(1) as f32;
    for (term_index, postings) in by_term.iter().enumerate() {
        let document_frequency = postings
            .iter()
            .map(|posting| posting.document.clone())
            .collect::<BTreeSet<_>>()
            .len()
            .max(1) as f32;
        let inverse_frequency =
            ((total_documents - document_frequency + 0.5) / (document_frequency + 0.5) + 1.0).ln();
        for posting in postings {
            let term_frequency = posting.positions.len() as f32;
            let normalized_frequency = term_frequency * 2.2
                / (term_frequency + 1.2 * (0.25 + 0.75 * posting.field_length as f32 / 100.0));
            let entry = scores.entry(posting.document.clone()).or_default();
            entry.0 += inverse_frequency * normalized_frequency;
            entry.1.insert(term_index);
        }
    }
    let mut hits = scores
        .into_iter()
        .filter(|(_, (_, matched))| !match_all_terms || matched.len() == by_term.len())
        .map(|(document, (score, matched))| FullTextHit {
            document,
            score,
            matched_terms: u32::try_from(matched.len()).unwrap_or(u32::MAX),
        })
        .collect::<Vec<_>>();
    sort_hits(&mut hits);
    hits.truncate(limit);
    Ok(hits)
}

fn phrase_hits(by_term: &[Vec<TextPosting>], limit: usize) -> Result<Vec<FullTextHit>, IndexError> {
    if by_term.iter().any(Vec::is_empty) {
        return Ok(Vec::new());
    }
    let mut by_key = Vec::<BTreeMap<(DocumentRef, String), &TextPosting>>::new();
    for postings in by_term {
        by_key.push(
            postings
                .iter()
                .map(|posting| ((posting.document.clone(), posting.field.clone()), posting))
                .collect(),
        );
    }
    let mut hits = Vec::new();
    let mut emitted = BTreeSet::new();
    for (key, first) in &by_key[0] {
        let Some(rest) = by_key[1..]
            .iter()
            .map(|postings| postings.get(key).copied())
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let phrase = first.positions.iter().any(|start| {
            rest.iter().enumerate().all(|(offset, posting)| {
                posting
                    .positions
                    .binary_search(&start.saturating_add(offset as u32 + 1))
                    .is_ok()
            })
        });
        if phrase && emitted.insert(key.0.clone()) {
            hits.push(FullTextHit {
                document: key.0.clone(),
                score: by_term.len() as f32,
                matched_terms: u32::try_from(by_term.len()).unwrap_or(u32::MAX),
            });
        }
    }
    sort_hits(&mut hits);
    hits.truncate(limit);
    Ok(hits)
}

fn sort_hits(hits: &mut [FullTextHit]) {
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.document.cmp(&right.document))
    });
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryDirectory;

    use super::*;

    fn directory(artifacts: IndexArtifacts) -> MemoryDirectory {
        MemoryDirectory::new(artifacts.into_files().map(|file| (file.name, file.bytes)))
    }

    #[tokio::test]
    async fn text_query_scores_terms_and_checks_phrases() {
        let artifacts = FullTextEngine::build([
            FullTextDocument {
                document: DocumentRef {
                    path: "/one".into(),
                    version: 1,
                },
                fields: BTreeMap::from([("body".into(), "Anvil indexes opaque objects".into())]),
            },
            FullTextDocument {
                document: DocumentRef {
                    path: "/two".into(),
                    version: 2,
                },
                fields: BTreeMap::from([("body".into(), "Opaque storage with an index".into())]),
            },
        ])
        .unwrap();
        let directory = directory(artifacts);
        let hits = FullTextEngine::query(
            &directory,
            FullTextQuery {
                text: "anvil indexes",
                fields: &[],
                phrase: true,
                match_all_terms: true,
                limit: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document.path, "/one");

        let hits = FullTextEngine::query(
            &directory,
            FullTextQuery {
                text: "opaque",
                fields: &[],
                phrase: false,
                match_all_terms: true,
                limit: 10,
            },
        )
        .await
        .unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn tokenizer_is_unicode_aware_and_bounded() {
        assert_eq!(
            tokenize("CAFÉ—Storage")
                .into_iter()
                .map(|value| value.0)
                .collect::<Vec<_>>(),
            ["café", "storage"]
        );
    }
}
