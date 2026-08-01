use serde_json::Value;

pub(crate) fn validate_pointer(pointer: &str) -> Result<(), String> {
    parse_pointer(pointer).map(|_| ())
}

pub(crate) fn select<'a>(value: &'a Value, pointer: &str) -> Result<&'a Value, String> {
    validate_pointer(pointer)?;
    value
        .pointer(pointer)
        .ok_or_else(|| format!("JSON pointer `{pointer}` does not exist"))
}

pub(crate) fn set(value: &mut Value, pointer: &str, replacement: Value) -> Result<(), String> {
    let tokens = parse_pointer(pointer)?;
    if tokens.is_empty() {
        *value = replacement;
        return Ok(());
    }

    let (last, parents) = tokens.split_last().expect("non-empty pointer");
    let parent = traverse_mut(value, parents, pointer)?;
    match parent {
        Value::Object(object) => {
            object.insert(last.clone(), replacement);
            Ok(())
        }
        Value::Array(array) => {
            let index = parse_array_index(last, pointer)?;
            let Some(element) = array.get_mut(index) else {
                return Err(format!(
                    "array index {index} in JSON pointer `{pointer}` is out of bounds"
                ));
            };
            *element = replacement;
            Ok(())
        }
        _ => Err(format!(
            "parent of JSON pointer `{pointer}` is not an object or array"
        )),
    }
}

pub(crate) fn remove(value: &mut Value, pointer: &str) -> Result<(), String> {
    let tokens = parse_pointer(pointer)?;
    if tokens.is_empty() {
        return Err("root removal is handled as a document tombstone".into());
    }

    let (last, parents) = tokens.split_last().expect("non-empty pointer");
    let parent = traverse_mut(value, parents, pointer)?;
    match parent {
        Value::Object(object) => object
            .remove(last)
            .map(|_| ())
            .ok_or_else(|| format!("JSON pointer `{pointer}` does not exist")),
        Value::Array(array) => {
            let index = parse_array_index(last, pointer)?;
            if index >= array.len() {
                return Err(format!(
                    "array index {index} in JSON pointer `{pointer}` is out of bounds"
                ));
            }
            array.remove(index);
            Ok(())
        }
        _ => Err(format!(
            "parent of JSON pointer `{pointer}` is not an object or array"
        )),
    }
}

fn traverse_mut<'a>(
    mut value: &'a mut Value,
    tokens: &[String],
    original_pointer: &str,
) -> Result<&'a mut Value, String> {
    for token in tokens {
        value = match value {
            Value::Object(object) => object
                .get_mut(token)
                .ok_or_else(|| format!("JSON pointer `{original_pointer}` has a missing parent"))?,
            Value::Array(array) => {
                let index = parse_array_index(token, original_pointer)?;
                array.get_mut(index).ok_or_else(|| {
                    format!(
                        "array index {index} in JSON pointer `{original_pointer}` is out of bounds"
                    )
                })?
            }
            _ => {
                return Err(format!(
                    "JSON pointer `{original_pointer}` traverses a scalar value"
                ));
            }
        };
    }
    Ok(value)
}

fn parse_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(format!(
            "JSON pointer `{pointer}` must be empty or start with `/`"
        ));
    }
    pointer[1..].split('/').map(decode_token).collect()
}

fn decode_token(token: &str) -> Result<String, String> {
    let mut decoded = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(character) = chars.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match chars.next() {
            Some('0') => decoded.push('~'),
            Some('1') => decoded.push('/'),
            _ => return Err("JSON pointer contains an invalid `~` escape".into()),
        }
    }
    Ok(decoded)
}

fn parse_array_index(token: &str, pointer: &str) -> Result<usize, String> {
    if token.is_empty()
        || token == "-"
        || (token.len() > 1 && token.starts_with('0'))
        || !token.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!(
            "JSON pointer `{pointer}` contains invalid array index `{token}`"
        ));
    }
    token
        .parse()
        .map_err(|_| format!("array index in JSON pointer `{pointer}` is too large"))
}
