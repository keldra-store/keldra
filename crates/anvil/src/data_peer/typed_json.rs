use super::*;

pub(super) fn require_typed_bound(encoded: &[u8]) -> Result<(), Status> {
    if encoded.len() > MAX_TYPED_MUTATION_BYTES {
        return Err(Status::resource_exhausted(
            "typed mutation exceeds the private peer limit",
        ));
    }
    Ok(())
}

pub(super) fn decode_typed<T: serde::de::DeserializeOwned>(encoded: &[u8]) -> Result<T, Status> {
    serde_json::from_slice(encoded)
        .map_err(|error| Status::invalid_argument(format!("invalid typed peer payload: {error}")))
}

pub(super) fn encode_typed<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value)
        .map_err(|error| Status::internal(format!("encode typed peer payload: {error}")))
}

pub(super) fn encode_page(changes: Vec<LocalChange>) -> Result<Vec<Vec<u8>>, Status> {
    let mut encoded = Vec::with_capacity(changes.len());
    let mut total = 0_usize;
    for change in changes {
        let item = encode_typed(&change)?;
        total = total
            .checked_add(item.len())
            .filter(|total| *total <= MAX_TYPED_MUTATION_BYTES)
            .ok_or_else(|| Status::resource_exhausted("source journal page exceeds peer limit"))?;
        encoded.push(item);
    }
    Ok(encoded)
}
