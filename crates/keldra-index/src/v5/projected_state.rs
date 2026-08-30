use std::collections::BTreeSet;

use crate::IndexError;
use crate::v4::{INDEX_ROUTING_KEY_BYTES, ObjectIdentity};

const STABLE_DOCUMENT_KEY_DOMAIN: &[u8] = b"keldra.index.stable-document-key/v1";

/// Definition-neutral identity of one canonical membership or field recipe.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RecipeIdentity([u8; 32]);

impl RecipeIdentity {
    pub fn new(bytes: [u8; 32]) -> Result<Self, IndexError> {
        if bytes == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "physical recipe identity must be non-zero".into(),
            ));
        }
        Ok(Self(bytes))
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Stable join key used by document-head, membership, and field streams.
///
/// The complete path and record ordinal remain in [`DocumentHead`], so a hash
/// collision can be detected rather than treated as semantic equality.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StableDocumentKey([u8; 32]);

impl StableDocumentKey {
    pub fn derive(
        source_scope: [u8; 32],
        source_path: &str,
        source_record: u32,
    ) -> Result<Self, IndexError> {
        validate_path(source_path)?;
        if source_scope == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "stable document key requires a source scope".into(),
            ));
        }
        let mut hasher = blake3::Hasher::new();
        hasher.update(STABLE_DOCUMENT_KEY_DOMAIN);
        hasher.update(&source_scope);
        hasher.update(&(source_path.len() as u64).to_be_bytes());
        hasher.update(source_path.as_bytes());
        hasher.update(&source_record.to_be_bytes());
        Ok(Self(*hasher.finalize().as_bytes()))
    }

    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Exact latest identity and liveness for one stable projected record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentHead {
    pub stable_key: StableDocumentKey,
    pub source_path: String,
    pub source_record: u32,
    pub source_version: u64,
    pub result: Option<ObjectIdentity>,
    pub live: bool,
}

impl DocumentHead {
    pub fn new(
        source_scope: [u8; 32],
        source_path: String,
        source_record: u32,
        source_version: u64,
        result: Option<ObjectIdentity>,
        live: bool,
    ) -> Result<Self, IndexError> {
        let stable_key = StableDocumentKey::derive(source_scope, &source_path, source_record)?;
        let head = Self {
            stable_key,
            source_path,
            source_record,
            source_version,
            result,
            live,
        };
        head.validate(source_scope)?;
        Ok(head)
    }

    pub fn result_or_source(&self) -> ObjectIdentity {
        self.result.clone().unwrap_or_else(|| ObjectIdentity {
            path: self.source_path.clone(),
            version: self.source_version,
        })
    }

    fn validate(&self, source_scope: [u8; 32]) -> Result<(), IndexError> {
        validate_path(&self.source_path)?;
        if self.source_version == 0
            || self.stable_key
                != StableDocumentKey::derive(source_scope, &self.source_path, self.source_record)?
            || self
                .result
                .as_ref()
                .is_some_and(|result| result.validate().is_err())
        {
            return Err(IndexError::InvalidDefinition(
                "projected document head is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Exact canonical bytes for one recipe at one stable document key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalRecipeState {
    pub recipe: RecipeIdentity,
    pub digest: [u8; 32],
    pub value: Vec<u8>,
}

impl CanonicalRecipeState {
    pub fn new(recipe: RecipeIdentity, value: Vec<u8>) -> Result<Self, IndexError> {
        Ok(Self {
            recipe,
            digest: *blake3::hash(&value).as_bytes(),
            value,
        })
    }

    fn validate(&self) -> Result<(), IndexError> {
        if self.digest != *blake3::hash(&self.value).as_bytes() {
            return Err(IndexError::InvalidDefinition(
                "canonical projected recipe state is invalid".into(),
            ));
        }
        Ok(())
    }
}

/// Disposable exact projected state used to determine component-local deltas.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedDocumentState {
    pub source_scope: [u8; 32],
    pub head: DocumentHead,
    pub memberships: Vec<CanonicalRecipeState>,
    pub fields: Vec<CanonicalRecipeState>,
}

impl ProjectedDocumentState {
    pub fn new(
        source_scope: [u8; 32],
        head: DocumentHead,
        memberships: Vec<CanonicalRecipeState>,
        fields: Vec<CanonicalRecipeState>,
    ) -> Result<Self, IndexError> {
        let state = Self {
            source_scope,
            head,
            memberships,
            fields,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        if self.source_scope == [0; 32] {
            return Err(IndexError::InvalidDefinition(
                "projected document state requires a source scope".into(),
            ));
        }
        self.head.validate(self.source_scope)?;
        validate_recipe_states(&self.memberships)?;
        validate_recipe_states(&self.fields)?;
        if !self.head.live && (!self.memberships.is_empty() || !self.fields.is_empty()) {
            return Err(IndexError::InvalidDefinition(
                "a deleted projected document cannot retain live recipe state".into(),
            ));
        }
        Ok(())
    }

    /// Compute the exact independently publishable changes from `previous`.
    ///
    /// Digests are only a fast inequality check. Equal digests still compare
    /// canonical bytes before a component write may be skipped.
    pub fn delta_from(
        &self,
        previous: Option<&ProjectedDocumentState>,
    ) -> Result<ProjectedDocumentDelta, IndexError> {
        self.validate()?;
        if let Some(previous) = previous {
            previous.validate()?;
            if self.source_scope != previous.source_scope
                || self.head.stable_key != previous.head.stable_key
                || self.head.source_path != previous.head.source_path
                || self.head.source_record != previous.head.source_record
            {
                return Err(IndexError::InvalidDefinition(
                    "projected-state comparison crossed a stable document identity".into(),
                ));
            }
        }
        Ok(ProjectedDocumentDelta {
            head: (previous.map(|value| &value.head) != Some(&self.head))
                .then(|| self.head.clone()),
            memberships: diff_recipe_states(
                previous.map_or(&[], |value| value.memberships.as_slice()),
                &self.memberships,
            ),
            fields: diff_recipe_states(
                previous.map_or(&[], |value| value.fields.as_slice()),
                &self.fields,
            ),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecipeDelta {
    pub recipe: RecipeIdentity,
    /// `None` is a tombstone for a value present in the preceding generation.
    pub replacement: Option<CanonicalRecipeState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedDocumentDelta {
    pub head: Option<DocumentHead>,
    pub memberships: Vec<RecipeDelta>,
    pub fields: Vec<RecipeDelta>,
}

impl ProjectedDocumentDelta {
    pub fn is_empty(&self) -> bool {
        self.head.is_none() && self.memberships.is_empty() && self.fields.is_empty()
    }

    pub fn is_head_only(&self) -> bool {
        self.head.is_some() && self.memberships.is_empty() && self.fields.is_empty()
    }
}

fn validate_recipe_states(states: &[CanonicalRecipeState]) -> Result<(), IndexError> {
    let mut identities = BTreeSet::new();
    for state in states {
        state.validate()?;
        if !identities.insert(state.recipe) {
            return Err(IndexError::InvalidDefinition(
                "projected recipe states must be unique".into(),
            ));
        }
    }
    if states
        .windows(2)
        .any(|pair| pair[0].recipe >= pair[1].recipe)
    {
        return Err(IndexError::InvalidDefinition(
            "projected recipe states must use canonical recipe order".into(),
        ));
    }
    Ok(())
}

fn diff_recipe_states(
    previous: &[CanonicalRecipeState],
    current: &[CanonicalRecipeState],
) -> Vec<RecipeDelta> {
    let mut output = Vec::new();
    let (mut left, mut right) = (0, 0);
    while left < previous.len() || right < current.len() {
        match (previous.get(left), current.get(right)) {
            (Some(old), Some(new)) if old.recipe == new.recipe => {
                if old.digest != new.digest || old.value != new.value {
                    output.push(RecipeDelta {
                        recipe: new.recipe,
                        replacement: Some(new.clone()),
                    });
                }
                left += 1;
                right += 1;
            }
            (Some(old), Some(new)) if old.recipe < new.recipe => {
                output.push(RecipeDelta {
                    recipe: old.recipe,
                    replacement: None,
                });
                left += 1;
            }
            (_, Some(new)) => {
                output.push(RecipeDelta {
                    recipe: new.recipe,
                    replacement: Some(new.clone()),
                });
                right += 1;
            }
            (Some(old), None) => {
                output.push(RecipeDelta {
                    recipe: old.recipe,
                    replacement: None,
                });
                left += 1;
            }
            (None, None) => break,
        }
    }
    output
}

fn validate_path(path: &str) -> Result<(), IndexError> {
    if path.is_empty() || path.len() > INDEX_ROUTING_KEY_BYTES || path.contains('\0') {
        return Err(IndexError::InvalidDefinition(
            "stable document source path is invalid".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recipe(byte: u8) -> RecipeIdentity {
        RecipeIdentity::new([byte; 32]).unwrap()
    }

    fn state(version: u64, fields: &[(u8, &[u8])]) -> ProjectedDocumentState {
        let scope = [9; 32];
        ProjectedDocumentState::new(
            scope,
            DocumentHead::new(scope, "objects/a".into(), 0, version, None, true).unwrap(),
            vec![CanonicalRecipeState::new(recipe(1), vec![1]).unwrap()],
            fields
                .iter()
                .map(|(id, value)| CanonicalRecipeState::new(recipe(*id), value.to_vec()).unwrap())
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn unindexed_update_changes_only_the_exact_document_head() {
        let old = state(7, &[(2, b"stable"), (3, b"also stable")]);
        let new = state(8, &[(2, b"stable"), (3, b"also stable")]);
        let delta = new.delta_from(Some(&old)).unwrap();
        assert!(delta.is_head_only());
        assert_eq!(delta.head.unwrap().source_version, 8);
    }

    #[test]
    fn one_changed_field_does_not_rewrite_unchanged_recipes() {
        let old = state(7, &[(2, b"old"), (3, b"stable")]);
        let new = state(8, &[(2, b"new"), (3, b"stable")]);
        let delta = new.delta_from(Some(&old)).unwrap();
        assert_eq!(delta.fields.len(), 1);
        assert_eq!(delta.fields[0].recipe, recipe(2));
        assert_eq!(delta.fields[0].replacement.as_ref().unwrap().value, b"new");
    }

    #[test]
    fn removed_recipe_is_an_explicit_tombstone() {
        let old = state(7, &[(2, b"old"), (3, b"removed")]);
        let new = state(8, &[(2, b"old")]);
        let delta = new.delta_from(Some(&old)).unwrap();
        assert_eq!(delta.fields.len(), 1);
        assert_eq!(delta.fields[0].recipe, recipe(3));
        assert!(delta.fields[0].replacement.is_none());
    }

    #[test]
    fn equal_digest_cannot_skip_a_byte_difference() {
        let old = state(7, &[(2, b"old")]);
        let mut new = state(8, &[(2, b"new")]);
        new.fields[0].digest = old.fields[0].digest;
        assert!(new.delta_from(Some(&old)).is_err());
    }

    #[test]
    fn stable_key_is_definition_neutral_and_record_specific() {
        let scope = [7; 32];
        let first = StableDocumentKey::derive(scope, "objects/a", 0).unwrap();
        assert_eq!(
            first,
            StableDocumentKey::derive(scope, "objects/a", 0).unwrap()
        );
        assert_ne!(
            first,
            StableDocumentKey::derive(scope, "objects/a", 1).unwrap()
        );
        assert_ne!(
            first,
            StableDocumentKey::derive([8; 32], "objects/a", 0).unwrap()
        );
    }
}
