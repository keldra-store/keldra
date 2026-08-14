use crate::IndexError;

/// Maximum Unicode scalar values in one analyzed term.
pub const MAX_ANALYZED_TOKEN_CHARS: usize = 128;

/// Apply the format-v4 Unicode-alphanumeric-lowercase analyzer.
///
/// Long alphanumeric runs are split into consecutive positional terms. The
/// split happens after Unicode lowercase expansion, so every emitted term is
/// bounded and no normalized character is dropped.
pub fn analyze_unicode_alphanumeric_lowercase(
    text: &str,
    maximum_tokens: usize,
) -> Result<Vec<(String, u32)>, IndexError> {
    let mut output = Vec::new();
    let mut token = String::new();
    let mut token_chars = 0usize;

    for character in text.chars() {
        if !character.is_alphanumeric() {
            flush(&mut output, &mut token, &mut token_chars, maximum_tokens)?;
            continue;
        }
        for lower in character.to_lowercase() {
            if token_chars == MAX_ANALYZED_TOKEN_CHARS {
                flush(&mut output, &mut token, &mut token_chars, maximum_tokens)?;
            }
            token.push(lower);
            token_chars += 1;
        }
    }
    flush(&mut output, &mut token, &mut token_chars, maximum_tokens)?;
    Ok(output)
}

fn flush(
    output: &mut Vec<(String, u32)>,
    token: &mut String,
    token_chars: &mut usize,
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
    *token_chars = 0;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_long_runs_without_dropping_normalized_characters() {
        let input = format!("{}B", "A".repeat(MAX_ANALYZED_TOKEN_CHARS));
        let tokens = analyze_unicode_alphanumeric_lowercase(&input, 2).unwrap();

        assert_eq!(tokens.len(), 2);
        assert_eq!(tokens[0], ("a".repeat(MAX_ANALYZED_TOKEN_CHARS), 0));
        assert_eq!(tokens[1], ("b".to_owned(), 1));
        assert_eq!(
            tokens.iter().map(|(token, _)| token.len()).sum::<usize>(),
            input.len()
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
