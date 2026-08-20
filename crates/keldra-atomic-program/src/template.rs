use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::model::ObjectPath;

/// A deliberately small template language: literal text plus `{name}` fields.
///
/// Substitutions are restricted to one safe path segment. A program template
/// may contain `/` in its literal text, but invocation values may not introduce
/// separators, braces, or NUL bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StringTemplate(pub String);

impl StringTemplate {
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    pub fn validate(&self) -> Result<(), String> {
        self.parts().map(|_| ())
    }

    pub fn placeholders(&self) -> Result<BTreeSet<String>, String> {
        Ok(self
            .parts()?
            .into_iter()
            .filter_map(|part| match part {
                TemplatePart::Literal(_) => None,
                TemplatePart::Field(name) => Some(name.to_owned()),
            })
            .collect())
    }

    pub fn expand(&self, values: &BTreeMap<String, String>) -> Result<String, String> {
        let mut expanded = String::with_capacity(self.0.len());
        for part in self.parts()? {
            match part {
                TemplatePart::Literal(value) => expanded.push_str(value),
                TemplatePart::Field(name) => {
                    let value = values
                        .get(name)
                        .ok_or_else(|| format!("missing template value `{name}`"))?;
                    validate_substitution(name, value)?;
                    expanded.push_str(value);
                }
            }
        }
        Ok(expanded)
    }

    fn parts(&self) -> Result<Vec<TemplatePart<'_>>, String> {
        if self.0.is_empty() {
            return Err("template must not be empty".into());
        }

        let bytes = self.0.as_bytes();
        let mut parts = Vec::new();
        let mut literal_start = 0;
        let mut cursor = 0;

        while cursor < bytes.len() {
            match bytes[cursor] {
                b'{' => {
                    if literal_start < cursor {
                        parts.push(TemplatePart::Literal(&self.0[literal_start..cursor]));
                    }
                    let close = self.0[cursor + 1..]
                        .find('}')
                        .map(|offset| cursor + 1 + offset)
                        .ok_or_else(|| "template contains an unclosed `{`".to_owned())?;
                    let name = &self.0[cursor + 1..close];
                    validate_field_name(name)?;
                    parts.push(TemplatePart::Field(name));
                    cursor = close + 1;
                    literal_start = cursor;
                }
                b'}' => return Err("template contains an unmatched `}`".into()),
                b'\0' => return Err("template contains a NUL byte".into()),
                _ => cursor += 1,
            }
        }

        if literal_start < self.0.len() {
            parts.push(TemplatePart::Literal(&self.0[literal_start..]));
        }
        Ok(parts)
    }
}

#[derive(Debug, Clone, Copy)]
enum TemplatePart<'a> {
    Literal(&'a str),
    Field(&'a str),
}

fn validate_field_name(name: &str) -> Result<(), String> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("template field name must not be empty".into());
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        return Err(format!("invalid template field name `{name}`"));
    }
    Ok(())
}

fn validate_substitution(name: &str, value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.contains('/')
        || value.contains('{')
        || value.contains('}')
        || value.contains('\0')
    {
        return Err(format!(
            "template value `{name}` must be one non-empty safe path segment"
        ));
    }
    Ok(())
}

/// Tenant/bucket/path templates for a concrete object address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathTemplate {
    pub tenant: StringTemplate,
    pub bucket: StringTemplate,
    pub path: StringTemplate,
}

impl PathTemplate {
    pub fn new(
        tenant: impl Into<String>,
        bucket: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            tenant: StringTemplate::new(tenant),
            bucket: StringTemplate::new(bucket),
            path: StringTemplate::new(path),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.tenant.validate()?;
        self.bucket.validate()?;
        self.path.validate()?;
        if self.tenant.0.contains('/') || self.bucket.0.contains('/') {
            return Err("tenant and bucket templates must not contain `/`".into());
        }
        Ok(())
    }

    pub fn expand(&self, values: &BTreeMap<String, String>) -> Result<ObjectPath, String> {
        ObjectPath::new(
            self.tenant.expand(values)?,
            self.bucket.expand(values)?,
            self.path.expand(values)?,
        )
    }
}
