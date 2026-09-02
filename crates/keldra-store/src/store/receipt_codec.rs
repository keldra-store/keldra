//! Compact, versioned encoding for durable object mutation receipts.

use keldra_atomic_program::MAX_OBJECT_PATH_BYTES;

use super::{CF_RECEIPTS, Store, StoredReceipt};
use crate::{
    DefinitionKind, DefinitionOperation, DefinitionTransition, MutationError, ObjectMutation,
    VersionId,
};

const MAGIC: &[u8; 4] = b"KDRC";
const FORMAT: u16 = 1;
const HEADER_BYTES: usize = 4 + 2 + 1 + 1 + 4;
const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
const LOCAL_KIND: u8 = 1;
const MUTATION_KIND: u8 = 2;
const NO_TRANSITION: u8 = 0;
const HAS_TRANSITION: u8 = 1;

pub(super) fn encode_stored_receipt(receipt: &StoredReceipt) -> Result<Vec<u8>, MutationError> {
    let (kind, body) = match receipt.object_mutation.as_ref() {
        Some(mutation) => {
            validate_mutation_backed_receipt(receipt, mutation)?;
            (
                MUTATION_KIND,
                super::object_mutation_codec::encode_object_mutation(mutation)
                    .map_err(receipt_storage)?,
            )
        }
        None => {
            let mut body = Vec::new();
            body.extend_from_slice(&receipt.fingerprint);
            body.extend_from_slice(&receipt.version.0.to_be_bytes());
            body.push(u8::from(receipt.deleted));
            body.extend_from_slice(&receipt.expires_at_unix_millis.to_be_bytes());
            put_definition_transition(&mut body, receipt.definition_transition.as_ref())?;
            (LOCAL_KIND, body)
        }
    };
    if body.len() > MAX_BODY_BYTES {
        return Err(receipt_storage(
            "stored receipt body exceeds its format bound",
        ));
    }
    let body_length = u32::try_from(body.len())
        .map_err(|_| receipt_storage("stored receipt body length is exhausted"))?;
    let mut encoded = Vec::with_capacity(HEADER_BYTES + body.len());
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FORMAT.to_be_bytes());
    encoded.push(kind);
    encoded.push(0);
    encoded.extend_from_slice(&body_length.to_be_bytes());
    encoded.extend_from_slice(&body);
    Ok(encoded)
}

pub(super) fn decode_stored_receipt(encoded: &[u8]) -> Result<StoredReceipt, MutationError> {
    if encoded.len() < HEADER_BYTES || encoded.len() > HEADER_BYTES + MAX_BODY_BYTES {
        return Err(receipt_storage(
            "stored receipt length is outside its format bound",
        ));
    }
    if encoded[..4] != *MAGIC {
        return Err(receipt_storage("stored receipt magic is invalid"));
    }
    if u16::from_be_bytes(
        encoded[4..6]
            .try_into()
            .expect("receipt format has a fixed width"),
    ) != FORMAT
    {
        return Err(receipt_storage("stored receipt format is unsupported"));
    }
    if encoded[7] != 0 {
        return Err(receipt_storage(
            "stored receipt reserved header byte is non-zero",
        ));
    }
    let body_length = usize::try_from(u32::from_be_bytes(
        encoded[8..12]
            .try_into()
            .expect("receipt body length has a fixed width"),
    ))
    .map_err(|_| receipt_storage("stored receipt body length does not fit this platform"))?;
    let body = &encoded[HEADER_BYTES..];
    if body_length != body.len() {
        return Err(receipt_storage(
            "stored receipt body length disagrees with its record length",
        ));
    }
    let mut decoder = Decoder::new(body);
    let receipt = match encoded[6] {
        LOCAL_KIND => decode_local_receipt(&mut decoder)?,
        MUTATION_KIND => {
            let mutation = super::object_mutation_codec::decode_object_mutation(body)
                .map_err(receipt_storage)?;
            decoder.finish();
            stored_receipt_from_mutation(mutation)
        }
        _ => return Err(receipt_storage("stored receipt kind is unknown")),
    };
    if !decoder.is_finished() {
        return Err(receipt_storage("stored receipt has trailing bytes"));
    }
    Ok(receipt)
}

impl Store {
    pub(super) fn read_stored_receipt(
        &self,
        key: &[u8],
    ) -> Result<Option<StoredReceipt>, MutationError> {
        self.db
            .get_cf(self.cf(CF_RECEIPTS)?, key)
            .map_err(super::storage_error)?
            .map(|encoded| decode_stored_receipt(&encoded))
            .transpose()
    }
}

fn decode_local_receipt(decoder: &mut Decoder<'_>) -> Result<StoredReceipt, MutationError> {
    let fingerprint = decoder.array()?;
    let version = VersionId(decoder.u64()?);
    let deleted = match decoder.byte()? {
        0 => false,
        1 => true,
        _ => return Err(receipt_storage("stored receipt deleted flag is invalid")),
    };
    let expires_at_unix_millis = decoder.u64()?;
    let definition_transition = decode_definition_transition(decoder)?;
    Ok(StoredReceipt {
        fingerprint,
        version,
        deleted,
        expires_at_unix_millis,
        object_mutation: None,
        definition_transition,
    })
}

fn validate_mutation_backed_receipt(
    receipt: &StoredReceipt,
    mutation: &ObjectMutation,
) -> Result<(), MutationError> {
    if receipt.fingerprint != mutation.input_fingerprint
        || receipt.version != mutation.version.id
        || receipt.deleted != mutation.version.deleted
        || receipt.expires_at_unix_millis != mutation.receipt_expires_at_unix_millis
        || receipt.definition_transition != mutation.definition_transition
    {
        return Err(receipt_storage(
            "mutation-backed receipt disagrees with its object mutation",
        ));
    }
    Ok(())
}

fn stored_receipt_from_mutation(mutation: ObjectMutation) -> StoredReceipt {
    StoredReceipt {
        fingerprint: mutation.input_fingerprint,
        version: mutation.version.id,
        deleted: mutation.version.deleted,
        expires_at_unix_millis: mutation.receipt_expires_at_unix_millis,
        definition_transition: mutation.definition_transition.clone(),
        object_mutation: Some(mutation),
    }
}

fn put_definition_transition(
    encoded: &mut Vec<u8>,
    transition: Option<&DefinitionTransition>,
) -> Result<(), MutationError> {
    let Some(transition) = transition else {
        encoded.push(NO_TRANSITION);
        return Ok(());
    };
    transition.validate().map_err(receipt_storage)?;
    let path = transition.path.as_bytes();
    let path_length = u32::try_from(path.len())
        .map_err(|_| receipt_storage("definition transition path is too long"))?;
    encoded.push(HAS_TRANSITION);
    encoded.push(match transition.kind {
        DefinitionKind::Index => 1,
        DefinitionKind::Accounting => 2,
    });
    encoded.extend_from_slice(&transition.tenant_id.to_be_bytes());
    encoded.extend_from_slice(&transition.bucket_id.to_be_bytes());
    encoded.extend_from_slice(&transition.definition_id.to_be_bytes());
    encoded.extend_from_slice(&path_length.to_be_bytes());
    encoded.extend_from_slice(path);
    encoded.extend_from_slice(&transition.object_version.0.to_be_bytes());
    encoded.push(match transition.operation {
        DefinitionOperation::Upsert => 1,
        DefinitionOperation::Delete => 2,
    });
    Ok(())
}

fn decode_definition_transition(
    decoder: &mut Decoder<'_>,
) -> Result<Option<DefinitionTransition>, MutationError> {
    match decoder.byte()? {
        NO_TRANSITION => Ok(None),
        HAS_TRANSITION => {
            let kind = match decoder.byte()? {
                1 => DefinitionKind::Index,
                2 => DefinitionKind::Accounting,
                _ => return Err(receipt_storage("definition transition kind is unknown")),
            };
            let tenant_id = decoder.u64()?;
            let bucket_id = decoder.u64()?;
            let definition_id = decoder.u64()?;
            let path_length = decoder.u32()? as usize;
            if path_length > MAX_OBJECT_PATH_BYTES {
                return Err(receipt_storage("definition transition path is too long"));
            }
            let path = std::str::from_utf8(decoder.take(path_length)?)
                .map_err(receipt_storage)?
                .to_owned();
            let object_version = VersionId(decoder.u64()?);
            let operation = match decoder.byte()? {
                1 => DefinitionOperation::Upsert,
                2 => DefinitionOperation::Delete,
                _ => {
                    return Err(receipt_storage(
                        "definition transition operation is unknown",
                    ));
                }
            };
            let transition = DefinitionTransition {
                kind,
                tenant_id,
                bucket_id,
                definition_id,
                path,
                object_version,
                operation,
            };
            transition.validate().map_err(receipt_storage)?;
            Ok(Some(transition))
        }
        _ => Err(receipt_storage(
            "stored receipt definition transition flag is invalid",
        )),
    }
}

struct Decoder<'a> {
    encoded: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self {
            encoded,
            position: 0,
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], MutationError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| receipt_storage("stored receipt length overflow"))?;
        let value = self
            .encoded
            .get(self.position..end)
            .ok_or_else(|| receipt_storage("stored receipt is truncated"))?;
        self.position = end;
        Ok(value)
    }

    fn byte(&mut self) -> Result<u8, MutationError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MutationError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .expect("decoder returned four bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, MutationError> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .expect("decoder returned eight bytes"),
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MutationError> {
        Ok(self
            .take(N)?
            .try_into()
            .expect("decoder returned the requested byte count"))
    }

    fn finish(&mut self) {
        self.position = self.encoded.len();
    }

    fn is_finished(&self) -> bool {
        self.position == self.encoded.len()
    }
}

fn receipt_storage(error: impl std::fmt::Display) -> MutationError {
    MutationError::Storage(format!("stored receipt is malformed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BatchOperation, Durability, ObjectKey, ObjectMutationContext, PlacementLogId, PutMode,
        PutRequest, StoreOptions,
    };

    fn local_receipt() -> StoredReceipt {
        StoredReceipt {
            fingerprint: [7; 32],
            version: VersionId(41),
            deleted: false,
            expires_at_unix_millis: 99,
            object_mutation: None,
            definition_transition: Some(DefinitionTransition {
                kind: DefinitionKind::Index,
                tenant_id: 3,
                bucket_id: 5,
                definition_id: 7,
                path: "indexes/by-name".into(),
                object_version: VersionId(41),
                operation: DefinitionOperation::Upsert,
            }),
        }
    }

    #[test]
    fn local_receipt_round_trips_without_json() {
        let receipt = local_receipt();
        let encoded = encode_stored_receipt(&receipt).unwrap();
        assert_eq!(&encoded[..8], b"KDRC\0\x01\x01\0");
        assert_eq!(
            u32::from_be_bytes(encoded[8..12].try_into().unwrap()) as usize,
            encoded.len() - HEADER_BYTES
        );
        assert_ne!(encoded.first(), Some(&b'{'));
        assert_eq!(decode_stored_receipt(&encoded).unwrap(), receipt);
    }

    #[test]
    fn local_receipt_rejects_truncation_and_trailing_bytes() {
        let mut encoded = encode_stored_receipt(&local_receipt()).unwrap();
        assert!(decode_stored_receipt(&encoded[..encoded.len() - 1]).is_err());
        encoded.push(0);
        assert!(decode_stored_receipt(&encoded).is_err());
    }

    #[test]
    fn receipt_rejects_json_and_unknown_envelope_values() {
        assert!(decode_stored_receipt(br#"{"fingerprint":[]}"#).is_err());
        let mut encoded = encode_stored_receipt(&local_receipt()).unwrap();
        encoded[5] = 2;
        assert!(decode_stored_receipt(&encoded).is_err());
        encoded[5] = FORMAT as u8;
        encoded[6] = 9;
        assert!(decode_stored_receipt(&encoded).is_err());
    }

    #[tokio::test]
    async fn mutation_backed_receipt_derives_duplicated_fields() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(StoreOptions::new(temporary.path(), 1))
            .await
            .unwrap();
        let coordinated = store
            .coordinate_object_mutation(
                BatchOperation::Put(PutRequest {
                    key: ObjectKey::new("tenant", "bucket", "objects/one").unwrap(),
                    bytes: b"receipt payload".to_vec(),
                    content_type: Some("application/octet-stream".into()),
                    mode: PutMode::PutIfAbsent,
                    command_id: Some("receipt-command".into()),
                    durability: Durability::Local,
                }),
                ObjectMutationContext {
                    active_placement_log_id: PlacementLogId { term: 3, index: 5 },
                    serving_fence_term: 3,
                },
            )
            .await
            .unwrap();
        let mutation = coordinated.mutation.unwrap();
        let receipt = stored_receipt_from_mutation(mutation.clone());
        let encoded = encode_stored_receipt(&receipt).unwrap();
        let legacy_json = serde_json::to_vec(&receipt).unwrap();
        assert_eq!(encoded[6], MUTATION_KIND);
        assert!(encoded.len() < legacy_json.len());
        assert_eq!(decode_stored_receipt(&encoded).unwrap(), receipt);

        let mut inconsistent = stored_receipt_from_mutation(mutation);
        inconsistent.version = VersionId(inconsistent.version.0 + 1);
        assert!(encode_stored_receipt(&inconsistent).is_err());
    }
}
