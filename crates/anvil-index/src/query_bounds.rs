use crate::{IndexError, MAX_INDEX_DECODED_BLOCK_BYTES};

/// Complete projection records retained by one query are limited to one
/// decoded-block allowance. Individual source blocks use the same bound, so a
/// large public page cannot multiply one maximum-sized record by its hit count.
pub(crate) const MAX_RETAINED_QUERY_RESULT_BYTES: usize = MAX_INDEX_DECODED_BLOCK_BYTES;

pub(crate) fn replace_retained_bytes(
    current: usize,
    added: usize,
    removed: usize,
) -> Result<usize, IndexError> {
    let needed = if added >= removed {
        current.checked_add(added - removed)
    } else {
        current.checked_sub(removed - added)
    }
    .ok_or(IndexError::OffsetOverflow)?;
    if needed > MAX_RETAINED_QUERY_RESULT_BYTES {
        return Err(IndexError::ResourceLimit {
            needed,
            limit: MAX_RETAINED_QUERY_RESULT_BYTES,
        });
    }
    Ok(needed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replacement_is_charged_after_releasing_the_evicted_result() {
        assert_eq!(
            replace_retained_bytes(MAX_RETAINED_QUERY_RESULT_BYTES, 7, 7).unwrap(),
            MAX_RETAINED_QUERY_RESULT_BYTES
        );
        assert_eq!(replace_retained_bytes(3, 7, 7).unwrap(), 3);
        assert!(matches!(
            replace_retained_bytes(MAX_RETAINED_QUERY_RESULT_BYTES, 1, 0),
            Err(IndexError::ResourceLimit { .. })
        ));
    }
}
