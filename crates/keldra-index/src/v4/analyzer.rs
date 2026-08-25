use crate::IndexError;

use super::INDEX_TERM_BYTES;

/// Apply the format-v4 Unicode-alphanumeric-lowercase analyzer.
///
/// A lexical token is never split or truncated. After Unicode lowercase
/// expansion, a token larger than the format's 32,766-byte term bound fails
/// precisely so a commit cannot claim a complete barrier while omitting
/// part of a source value.
pub fn analyze_unicode_alphanumeric_lowercase(
    text: &str,
    maximum_tokens: usize,
) -> Result<Vec<(String, u32)>, IndexError> {
    let mut output = Vec::new();
    let mut token = String::new();

    for character in text.chars() {
        if !character.is_alphanumeric() {
            flush(&mut output, &mut token, maximum_tokens)?;
            continue;
        }
        for lower in character.to_lowercase() {
            token.push(lower);
            if token.len() > INDEX_TERM_BYTES {
                return Err(IndexError::ResourceLimit {
                    needed: token.len(),
                    limit: INDEX_TERM_BYTES,
                });
            }
        }
    }
    flush(&mut output, &mut token, maximum_tokens)?;
    Ok(output)
}

fn flush(
    output: &mut Vec<(String, u32)>,
    token: &mut String,
    maximum_tokens: usize,
) -> Result<(), IndexError> {
    if token.is_empty() {
        return Ok(());
    }
    if output.len() == maximum_tokens {
        return Err(IndexError::ResourceLimit {
            needed: output.len().saturating_add(1),
            limit: maximum_tokens,
        });
    }
    let position = u32::try_from(output.len()).map_err(|_| IndexError::OffsetOverflow)?;
    output.push((std::mem::take(token), position));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_one_oversized_token_without_splitting_it() {
        let input = "A".repeat(INDEX_TERM_BYTES + 1);
        assert_eq!(
            analyze_unicode_alphanumeric_lowercase(&input, 2),
            Err(IndexError::ResourceLimit {
                needed: INDEX_TERM_BYTES + 1,
                limit: INDEX_TERM_BYTES,
            })
        );
    }

    #[test]
    fn lowercases_unicode_and_enforces_the_expansion_limit() {
        assert_eq!(
            analyze_unicode_alphanumeric_lowercase("RUST café", 2).unwrap(),
            [("rust".to_owned(), 0), ("café".to_owned(), 1)]
        );
        assert!(matches!(
            analyze_unicode_alphanumeric_lowercase("one two", 1),
            Err(IndexError::ResourceLimit {
                needed: 2,
                limit: 1
            })
        ));
    }
}
