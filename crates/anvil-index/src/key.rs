use crate::IndexError;

pub(crate) fn push_component(key: &mut Vec<u8>, component: &[u8]) -> Result<(), IndexError> {
    let length = u32::try_from(component.len()).map_err(|_| {
        IndexError::InvalidDefinition("index key component exceeds four GiB".into())
    })?;
    key.extend_from_slice(&length.to_be_bytes());
    key.extend_from_slice(component);
    Ok(())
}

pub(crate) fn component_prefix(components: &[&[u8]]) -> Result<Vec<u8>, IndexError> {
    let mut key = Vec::new();
    for component in components {
        push_component(&mut key, component)?;
    }
    Ok(key)
}

pub(crate) fn composite_key(components: &[&[u8]]) -> Result<Vec<u8>, IndexError> {
    component_prefix(components)
}

#[cfg(test)]
fn decode_components(mut key: &[u8]) -> Result<Vec<&[u8]>, IndexError> {
    let mut components = Vec::new();
    while !key.is_empty() {
        if key.len() < 4 {
            return Err(IndexError::InvalidFormat("truncated composite key length"));
        }
        let length = u32::from_be_bytes(key[..4].try_into().unwrap()) as usize;
        let end = 4usize
            .checked_add(length)
            .ok_or(IndexError::OffsetOverflow)?;
        if key.len() < end {
            return Err(IndexError::InvalidFormat(
                "truncated composite key component",
            ));
        }
        components.push(&key[4..end]);
        key = &key[end..];
    }
    Ok(components)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn components_are_unambiguous() {
        let encoded = composite_key(&[b"a/b", b"c", b""]).unwrap();
        assert_eq!(
            decode_components(&encoded).unwrap(),
            vec![&b"a/b"[..], &b"c"[..], &b""[..]]
        );
    }
}
