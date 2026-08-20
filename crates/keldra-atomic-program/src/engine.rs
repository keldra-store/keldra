use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Number, Value};

use crate::{
    error::EngineError,
    json,
    locks::{LocalLockGuard, LocalLockManager},
    model::*,
};

/// One bulk read issued only after all paths are expanded, bounded, and locally
/// locked by the nominated executor.
#[allow(async_fn_in_trait)]
pub trait StateReader: Send + Sync {
    async fn read_snapshot(&self, document_paths: &[ObjectPath])
    -> Result<ProgramSnapshot, String>;
}

/// A prepared result plus the executor-local locks protecting its preimage.
///
/// Keep this value alive until the caller commits or abandons the `Prepared`
/// bundle. The bundle itself is storage-neutral; exact head preconditions are
/// the durable correctness boundary across executor failover.
#[derive(Debug)]
pub struct ExecutionLease {
    bundle: Box<AtomicWriteBundle>,
    _locks: LocalLockGuard,
}

impl ExecutionLease {
    pub fn bundle(&self) -> &AtomicWriteBundle {
        &self.bundle
    }

    /// Releases the local locks and returns the storage-neutral result. This is
    /// useful for inspection and tests; orchestration code should normally
    /// commit or abandon the result first.
    pub fn release(self) -> Box<AtomicWriteBundle> {
        self.bundle
    }
}

/// Evaluates one immutable program object. It has no clock, random source,
/// network capability, storage writer, or user-code escape hatch.
pub struct AtomicProgramEngine<S> {
    definition: ProgramDefinition,
    reader: S,
    locks: LocalLockManager,
}

impl<S> AtomicProgramEngine<S>
where
    S: StateReader,
{
    pub fn new(definition: ProgramDefinition, reader: S) -> Result<Self, EngineError> {
        definition.validate()?;
        Ok(Self {
            definition,
            reader,
            locks: LocalLockManager::default(),
        })
    }

    pub fn with_lock_manager(
        definition: ProgramDefinition,
        reader: S,
        locks: LocalLockManager,
    ) -> Result<Self, EngineError> {
        definition.validate()?;
        Ok(Self {
            definition,
            reader,
            locks,
        })
    }

    pub fn definition(&self) -> &ProgramDefinition {
        &self.definition
    }

    /// Expands the complete bounded path inventory for authorization. Calling
    /// `prepare` repeats the deterministic expansion, then locks the same
    /// canonical paths. This method performs no reads and takes no locks.
    pub fn expanded_paths(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<Vec<ExpandedProgramPath>, EngineError> {
        let resolved = self.resolve(context, invocation)?;
        let mut paths = resolved
            .documents
            .into_iter()
            .map(|document| {
                let intent = self.definition.path_intent(&document.reference);
                ExpandedProgramPath {
                    path: document.path,
                    intent,
                }
            })
            .collect::<Vec<_>>();
        paths.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(paths)
    }

    /// Resolves and validates the complete finite path set before any read.
    pub async fn prepare(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<ExecutionLease, EngineError> {
        let resolved = self.resolve(context, invocation)?;
        let locks = self.locks.acquire(&resolved.all_paths).await;
        let snapshot = self
            .reader
            .read_snapshot(&resolved.document_paths)
            .await
            .map_err(EngineError::Read)?;
        let bundle = self.evaluate(invocation, &resolved, snapshot)?;
        Ok(ExecutionLease {
            bundle,
            _locks: locks,
        })
    }

    fn resolve(
        &self,
        context: &InvocationContext,
        invocation: &ProgramInvocation,
    ) -> Result<ResolvedInvocation, EngineError> {
        validate_invocation_identity(invocation)?;
        validate_invocation_size(&self.definition, invocation)?;
        validate_context(context)?;

        let mut template_values = invocation.arguments.clone();
        if template_values.contains_key("command_id") || template_values.contains_key("tenant") {
            return Err(invalid_invocation(
                "`tenant` and `command_id` come from request context and are reserved",
            ));
        }
        template_values.insert("tenant".into(), context.tenant.clone());
        template_values.insert("command_id".into(), invocation.command_id.clone());

        let known_slots: BTreeSet<_> = self
            .definition
            .documents
            .iter()
            .map(|document| document.name.as_str())
            .collect();
        if let Some(unknown) = invocation
            .bindings
            .keys()
            .find(|slot| !known_slots.contains(slot.as_str()))
        {
            return Err(invalid_invocation(format!(
                "binding supplied for unknown document slot `{unknown}`"
            )));
        }

        let mut documents = Vec::new();
        let mut document_paths = Vec::new();
        let mut by_reference = BTreeMap::new();
        let mut unique_paths = BTreeSet::new();

        for spec in &self.definition.documents {
            let bindings = invocation
                .bindings
                .get(&spec.name)
                .map_or(&[][..], Vec::as_slice);
            if !spec.cardinality.accepts(bindings.len()) {
                return Err(invalid_invocation(format!(
                    "slot `{}` received {} paths but has cardinality {:?}",
                    spec.name,
                    bindings.len(),
                    spec.cardinality
                )));
            }

            let mut path_fields = spec
                .path
                .tenant
                .placeholders()
                .map_err(invalid_invocation)?;
            path_fields.extend(
                spec.path
                    .bucket
                    .placeholders()
                    .map_err(invalid_invocation)?,
            );
            path_fields.extend(spec.path.path.placeholders().map_err(invalid_invocation)?);

            for (index, binding) in bindings.iter().enumerate() {
                let mut values = template_values.clone();
                for (name, value) in &binding.template_values {
                    if matches!(name.as_str(), "command_id" | "tenant")
                        || invocation.arguments.contains_key(name)
                    {
                        return Err(invalid_invocation(format!(
                            "binding value `{name}` collides with an invocation argument"
                        )));
                    }
                    if !path_fields.contains(name) {
                        return Err(invalid_invocation(format!(
                            "binding value `{name}` is not used by slot `{}`",
                            spec.name
                        )));
                    }
                    values.insert(name.clone(), value.clone());
                }

                let expanded = spec.path.expand(&values).map_err(invalid_invocation)?;
                ensure_tenant(context, &expanded)?;
                if expanded != binding.path {
                    return Err(invalid_invocation(format!(
                        "path for slot `{}` index {index} does not match its program template",
                        spec.name
                    )));
                }
                if binding.initial_json.is_some()
                    && (!spec.allow_initial_json || spec.access != DocumentAccess::ReadWrite)
                {
                    return Err(invalid_invocation(format!(
                        "slot `{}` does not permit an initial JSON value",
                        spec.name
                    )));
                }
                validate_expected_head(&binding.expected_head)?;
                if !unique_paths.insert(expanded.clone()) {
                    return Err(invalid_invocation(format!(
                        "object path {expanded:?} is bound more than once"
                    )));
                }

                let reference = DocumentRef {
                    slot: spec.name.clone(),
                    index,
                };
                by_reference.insert(reference.clone(), documents.len());
                document_paths.push(expanded.clone());
                documents.push(ResolvedDocument {
                    reference,
                    path: expanded,
                    expected_head: binding.expected_head.clone(),
                    initial_json: binding.initial_json.clone(),
                });
            }
        }

        if unique_paths.len() > self.definition.caps.max_paths {
            return Err(invalid_invocation(format!(
                "resolved {} paths, exceeding max_paths {}",
                unique_paths.len(),
                self.definition.caps.max_paths
            )));
        }

        let all_paths = unique_paths.into_iter().collect();
        document_paths.sort();
        Ok(ResolvedInvocation {
            documents,
            document_paths,
            by_reference,
            all_paths,
        })
    }

    fn evaluate(
        &self,
        invocation: &ProgramInvocation,
        resolved: &ResolvedInvocation,
        snapshot: ProgramSnapshot,
    ) -> Result<Box<AtomicWriteBundle>, EngineError> {
        validate_snapshot(resolved, &snapshot)?;

        let mut working = Vec::with_capacity(resolved.documents.len());
        for document in &resolved.documents {
            let stored = snapshot.documents.get(&document.path);
            check_expected_head(&document.path, &document.expected_head, stored)?;
            working.push(WorkingDocument::new(document, stored));
        }
        check_document_limits(&self.definition.caps, &working)?;

        for (index, assertion) in self.definition.assertions.iter().enumerate() {
            evaluate_assertion(assertion, invocation, resolved, &working)
                .map_err(|reason| EngineError::Assertion { index, reason })?;
        }

        for (index, operation) in self.definition.operations.iter().enumerate() {
            evaluate_operation(operation, invocation, resolved, &mut working)
                .map_err(|reason| EngineError::Operation { index, reason })?;
        }
        check_document_limits(&self.definition.caps, &working)?;

        let write_count = working.iter().filter(|document| document.dirty).count();
        if write_count > self.definition.caps.max_writes {
            return Err(EngineError::InvalidDefinition(format!(
                "program produced {write_count} writes, exceeding max_writes {}",
                self.definition.caps.max_writes
            )));
        }

        let mut outputs = BTreeMap::new();
        for returned in &self.definition.returns {
            let value = document_json_value(&returned.value, resolved, &working)
                .map_err(|reason| EngineError::Return {
                    name: returned.name.clone(),
                    reason,
                })?
                .clone();
            outputs.insert(returned.name.clone(), value);
        }

        let receipt = CommandReceipt {
            program_path_hash: invocation.program_path_hash,
            command_id: invocation.command_id.clone(),
            input_fingerprint: invocation.input_fingerprint.clone(),
            outputs: outputs.clone(),
        };

        let head_preconditions = working
            .iter()
            .map(|document| HeadPrecondition {
                path: document.path.clone(),
                expected: document.observed.clone(),
            })
            .collect();
        let writes = working
            .into_iter()
            .filter(|document| document.dirty)
            .map(|document| VersionedWrite {
                path: document.path,
                expected: document.observed,
                value: document.current,
                content_type: document.current_content_type,
            })
            .collect();

        Ok(Box::new(AtomicWriteBundle {
            head_preconditions,
            writes,
            receipt,
            outputs,
        }))
    }
}

impl ProgramDefinition {
    fn path_intent(&self, reference: &DocumentRef) -> ProgramPathIntent {
        let mut intent = ProgramPathIntent {
            // Every expanded document participates in the one bulk snapshot
            // and in the bundle's old-head preconditions.
            get: true,
            put: self
                .documents
                .iter()
                .find(|document| document.name == reference.slot)
                .is_some_and(|document| document.allow_initial_json),
            delete: false,
        };

        for operation in &self.operations {
            match operation {
                Operation::SetValue { target, .. }
                | Operation::CheckedIntegerAdd { target, .. }
                | Operation::CopyValue { target, .. }
                    if target.document == *reference =>
                {
                    intent.put = true;
                }
                Operation::RemoveValue { target } if target.document == *reference => {
                    if target.pointer.is_empty() {
                        intent.delete = true;
                    } else {
                        // Removing a non-root JSON pointer publishes the
                        // edited document rather than a tombstone.
                        intent.put = true;
                    }
                }
                Operation::ReplaceOpaque { document, .. } if document == reference => {
                    intent.put = true;
                }
                _ => {}
            }
        }
        intent
    }

    pub fn validate(&self) -> Result<(), EngineError> {
        if self.schema_version != DEFINITION_SCHEMA_VERSION {
            return Err(invalid_definition(format!(
                "schema version {} is unsupported; expected {DEFINITION_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        if self.caps.max_paths == 0
            || self.caps.max_input_bytes == 0
            || self.caps.max_document_bytes == 0
        {
            return Err(invalid_definition(
                "max_paths, max_input_bytes, and max_document_bytes must be non-zero",
            ));
        }
        let instruction_count = self
            .assertions
            .len()
            .checked_add(self.operations.len())
            .and_then(|count| count.checked_add(self.returns.len()))
            .ok_or_else(|| invalid_definition("instruction count overflow"))?;
        if instruction_count > self.caps.max_operations {
            return Err(invalid_definition(format!(
                "{instruction_count} assertions/operations/returns exceed max_operations {}",
                self.caps.max_operations
            )));
        }
        let mut slots = BTreeMap::new();
        let mut maximum_paths = 0usize;
        let mut maximum_writes = 0usize;
        for document in &self.documents {
            validate_identifier("document slot", &document.name)?;
            document.path.validate().map_err(invalid_definition)?;
            if matches!(document.cardinality, Cardinality::Repeated { max: 0 }) {
                return Err(invalid_definition(format!(
                    "repeated slot `{}` must permit at least one path",
                    document.name
                )));
            }
            if document.allow_initial_json && document.access != DocumentAccess::ReadWrite {
                return Err(invalid_definition(format!(
                    "read-only slot `{}` cannot permit an initial value",
                    document.name
                )));
            }
            if slots.insert(document.name.clone(), document).is_some() {
                return Err(invalid_definition(format!(
                    "duplicate document slot `{}`",
                    document.name
                )));
            }
            maximum_paths = maximum_paths
                .checked_add(document.cardinality.maximum())
                .ok_or_else(|| invalid_definition("maximum path count overflow"))?;
            if document.access == DocumentAccess::ReadWrite {
                maximum_writes = maximum_writes
                    .checked_add(document.cardinality.maximum())
                    .ok_or_else(|| invalid_definition("maximum write count overflow"))?;
            }
        }
        if maximum_paths > self.caps.max_paths {
            return Err(invalid_definition(format!(
                "declared cardinality can resolve {maximum_paths} paths, exceeding max_paths {}",
                self.caps.max_paths
            )));
        }
        if maximum_writes > self.caps.max_writes {
            return Err(invalid_definition(format!(
                "read-write cardinality can produce {maximum_writes} writes, exceeding max_writes {}",
                self.caps.max_writes
            )));
        }

        let check_reference = |reference: &DocumentRef, writable: bool| {
            let Some(spec) = slots.get(&reference.slot) else {
                return Err(invalid_definition(format!(
                    "reference uses unknown document slot `{}`",
                    reference.slot
                )));
            };
            if reference.index >= spec.cardinality.maximum() {
                return Err(invalid_definition(format!(
                    "reference to `{}` index {} exceeds its declared cardinality",
                    reference.slot, reference.index
                )));
            }
            if writable && spec.access != DocumentAccess::ReadWrite {
                return Err(invalid_definition(format!(
                    "operation writes read-only slot `{}`",
                    reference.slot
                )));
            }
            Ok(())
        };

        for assertion in &self.assertions {
            match assertion {
                Assertion::Exists { document } | Assertion::Absent { document } => {
                    check_reference(document, false)?
                }
                Assertion::JsonEqual { actual, expected } => {
                    check_reference(&actual.document, false)?;
                    json::validate_pointer(&actual.pointer).map_err(invalid_definition)?;
                    validate_input_value(expected)?;
                }
                Assertion::IntegerCompare {
                    actual, expected, ..
                } => {
                    check_reference(&actual.document, false)?;
                    json::validate_pointer(&actual.pointer).map_err(invalid_definition)?;
                    validate_input_value(expected)?;
                }
            }
            if let Assertion::IntegerCompare { numeric_type, .. } = assertion {
                validate_integer_type(*numeric_type)?;
            }
        }
        for operation in &self.operations {
            match operation {
                Operation::SetValue { target, value } => {
                    check_reference(&target.document, true)?;
                    json::validate_pointer(&target.pointer).map_err(invalid_definition)?;
                    validate_value_source(value, &check_reference)?;
                }
                Operation::RemoveValue { target } => {
                    check_reference(&target.document, true)?;
                    json::validate_pointer(&target.pointer).map_err(invalid_definition)?;
                }
                Operation::CheckedIntegerAdd {
                    target,
                    delta,
                    numeric_type,
                } => {
                    check_reference(&target.document, true)?;
                    json::validate_pointer(&target.pointer).map_err(invalid_definition)?;
                    validate_input_value(delta)?;
                    validate_integer_type(*numeric_type)?;
                }
                Operation::CopyValue { source, target } => {
                    check_reference(&source.value.document, false)?;
                    json::validate_pointer(&source.value.pointer).map_err(invalid_definition)?;
                    check_reference(&target.document, true)?;
                    json::validate_pointer(&target.pointer).map_err(invalid_definition)?;
                }
                Operation::ReplaceOpaque {
                    document,
                    input,
                    content_type,
                } => {
                    check_reference(document, true)?;
                    validate_input_name(input)?;
                    validate_content_type(content_type)?;
                }
            }
        }

        let mut return_names = BTreeSet::new();
        for returned in &self.returns {
            validate_identifier("return name", &returned.name)?;
            if !return_names.insert(&returned.name) {
                return Err(invalid_definition(format!(
                    "duplicate return name `{}`",
                    returned.name
                )));
            }
            check_reference(&returned.value.value.document, false)?;
            json::validate_pointer(&returned.value.value.pointer).map_err(invalid_definition)?;
        }
        Ok(())
    }
}

fn validate_value_source(
    source: &ValueSource,
    check_reference: &impl Fn(&DocumentRef, bool) -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    match source {
        ValueSource::Literal { .. } => Ok(()),
        ValueSource::Input { name } => validate_input_name(name),
        ValueSource::Document { source } => {
            check_reference(&source.value.document, false)?;
            json::validate_pointer(&source.value.pointer).map_err(invalid_definition)
        }
    }
}

fn validate_input_value(source: &InputValue) -> Result<(), EngineError> {
    match source {
        InputValue::Literal { .. } => Ok(()),
        InputValue::Input { name } => validate_input_name(name),
    }
}

fn validate_integer_type(numeric_type: IntegerType) -> Result<(), EngineError> {
    match numeric_type {
        IntegerType::I64 {
            min: Some(min),
            max: Some(max),
        } if min > max => Err(invalid_definition("i64 minimum exceeds maximum")),
        IntegerType::U64 {
            min: Some(min),
            max: Some(max),
        } if min > max => Err(invalid_definition("u64 minimum exceeds maximum")),
        _ => Ok(()),
    }
}

fn validate_identifier(kind: &str, value: &str) -> Result<(), EngineError> {
    if value.is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
        })
    {
        return Err(invalid_definition(format!(
            "{kind} must be 1..=128 safe ASCII characters"
        )));
    }
    Ok(())
}

fn validate_input_name(value: &str) -> Result<(), EngineError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(invalid_definition(
            "input name must be 1..=128 safe ASCII characters",
        ));
    }
    Ok(())
}

fn validate_content_type(value: &str) -> Result<(), EngineError> {
    if value.is_empty() || value.len() > 255 || value.contains('\0') {
        return Err(invalid_definition(
            "content type must be 1..=255 characters without NUL",
        ));
    }
    Ok(())
}

fn validate_invocation_identity(invocation: &ProgramInvocation) -> Result<(), EngineError> {
    if invocation.program_path_hash == [0; 32] {
        return Err(invalid_invocation("program_path_hash must be non-zero"));
    }
    if invocation.command_id.is_empty()
        || invocation.command_id.len() > 256
        || invocation.command_id.contains('\0')
        || invocation.command_id.contains('/')
        || invocation.command_id.contains('{')
        || invocation.command_id.contains('}')
    {
        return Err(invalid_invocation(
            "command_id must be a non-empty safe path segment of at most 256 bytes",
        ));
    }
    if invocation.input_fingerprint.len() != 64
        || !invocation
            .input_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid_invocation(
            "input_fingerprint must be lowercase hex for a 32-byte digest",
        ));
    }
    Ok(())
}

fn validate_context(context: &InvocationContext) -> Result<(), EngineError> {
    InvocationContext::new(context.tenant.clone())
        .map(|_| ())
        .map_err(invalid_invocation)
}

fn ensure_tenant(context: &InvocationContext, path: &ObjectPath) -> Result<(), EngineError> {
    if path.tenant != context.tenant {
        return Err(invalid_invocation(
            "expanded object tenant does not match authenticated request context",
        ));
    }
    Ok(())
}

fn validate_invocation_size(
    definition: &ProgramDefinition,
    invocation: &ProgramInvocation,
) -> Result<(), EngineError> {
    let encoded = serde_json::to_vec(invocation)
        .map_err(|error| invalid_invocation(format!("cannot encode invocation: {error}")))?;
    if encoded.len() > definition.caps.max_input_bytes {
        return Err(invalid_invocation(format!(
            "invocation is {} bytes, exceeding max_input_bytes {}",
            encoded.len(),
            definition.caps.max_input_bytes
        )));
    }
    Ok(())
}

fn validate_expected_head(expected: &ExpectedHead) -> Result<(), EngineError> {
    if matches!(expected, ExpectedHead::Version { version } if version.is_empty()) {
        return Err(invalid_invocation(
            "expected head version must not be empty",
        ));
    }
    Ok(())
}

fn validate_snapshot(
    resolved: &ResolvedInvocation,
    snapshot: &ProgramSnapshot,
) -> Result<(), EngineError> {
    let requested: BTreeSet<_> = resolved.document_paths.iter().collect();
    if let Some(path) = snapshot
        .documents
        .keys()
        .find(|path| !requested.contains(path))
    {
        return Err(EngineError::InvalidSnapshot(format!(
            "unrequested document {path:?} was returned"
        )));
    }
    if let Some((path, _)) = snapshot
        .documents
        .iter()
        .find(|(_, document)| document.version.is_empty())
    {
        return Err(EngineError::InvalidSnapshot(format!(
            "document {path:?} has an empty version"
        )));
    }
    if let Some((path, _)) = snapshot.documents.iter().find(|(_, document)| {
        matches!(
            (&document.value, &document.content_type),
            (Some(_), None) | (None, Some(_))
        )
    }) {
        return Err(EngineError::InvalidSnapshot(format!(
            "document {path:?} must pair a live value with content type or a tombstone with neither"
        )));
    }
    Ok(())
}

fn check_expected_head(
    path: &ObjectPath,
    expected: &ExpectedHead,
    stored: Option<&VersionedDocument>,
) -> Result<(), EngineError> {
    let failure = match (expected, stored) {
        (ExpectedHead::Any, _)
        | (ExpectedHead::Absent, None)
        | (ExpectedHead::Absent, Some(VersionedDocument { value: None, .. })) => None,
        (ExpectedHead::Version { version }, Some(document)) if version == &document.version => None,
        (ExpectedHead::Absent, Some(document)) => Some(format!(
            "expected absent, found version {}",
            document.version
        )),
        (ExpectedHead::Version { version }, None) => {
            Some(format!("expected version {version}, found absent"))
        }
        (ExpectedHead::Version { version }, Some(document)) => Some(format!(
            "expected version {version}, found version {}",
            document.version
        )),
    };
    if let Some(reason) = failure {
        return Err(EngineError::HeadPrecondition {
            path: path.clone(),
            reason,
        });
    }
    Ok(())
}

fn evaluate_assertion(
    assertion: &Assertion,
    invocation: &ProgramInvocation,
    resolved: &ResolvedInvocation,
    working: &[WorkingDocument],
) -> Result<(), String> {
    match assertion {
        Assertion::Exists { document } => {
            if working_document(document, resolved, working)?
                .before
                .is_some()
            {
                Ok(())
            } else {
                Err(format!("document {document:?} is absent"))
            }
        }
        Assertion::Absent { document } => {
            if working_document(document, resolved, working)?
                .before
                .is_none()
            {
                Ok(())
            } else {
                Err(format!("document {document:?} exists"))
            }
        }
        Assertion::JsonEqual { actual, expected } => {
            let actual = json_from_stored(
                working_document(&actual.document, resolved, working)?
                    .before
                    .as_ref(),
                &actual.pointer,
            )?;
            let expected = input_value(expected, invocation)?;
            if actual == expected {
                Ok(())
            } else {
                Err("JSON values are not equal".into())
            }
        }
        Assertion::IntegerCompare {
            actual,
            comparison,
            expected,
            numeric_type,
        } => {
            let actual = json_from_stored(
                working_document(&actual.document, resolved, working)?
                    .before
                    .as_ref(),
                &actual.pointer,
            )?;
            let expected = input_value(expected, invocation)?;
            let matches = match numeric_type {
                IntegerType::I64 { min, max } => compare(
                    checked_i64(actual, *min, *max)?,
                    checked_i64(expected, *min, *max)?,
                    *comparison,
                ),
                IntegerType::U64 { min, max } => compare(
                    checked_u64(actual, *min, *max)?,
                    checked_u64(expected, *min, *max)?,
                    *comparison,
                ),
            };
            if matches {
                Ok(())
            } else {
                Err(format!("integer comparison {comparison:?} is false"))
            }
        }
    }
}

fn evaluate_operation(
    operation: &Operation,
    invocation: &ProgramInvocation,
    resolved: &ResolvedInvocation,
    working: &mut [WorkingDocument],
) -> Result<(), String> {
    match operation {
        Operation::SetValue { target, value } => {
            let value = value_source(value, invocation, resolved, working)?;
            set_document_json(target, value, resolved, working)
        }
        Operation::RemoveValue { target } => {
            let document = working_document_mut(&target.document, resolved, working)?;
            if target.pointer.is_empty() {
                document.current = None;
                document.current_content_type = None;
            } else {
                let value = current_json_mut(document)?;
                json::remove(value, &target.pointer)?;
            }
            document.dirty = true;
            Ok(())
        }
        Operation::CheckedIntegerAdd {
            target,
            delta,
            numeric_type,
        } => {
            let delta = input_value(delta, invocation)?;
            let document = working_document_mut(&target.document, resolved, working)?;
            let value = current_json_mut(document)?;
            let current = json::select(value, &target.pointer)?;
            let replacement = match numeric_type {
                IntegerType::I64 { min, max } => {
                    let current = checked_i64(current, *min, *max)?;
                    let delta = exact_i64(delta)?;
                    let result = current
                        .checked_add(delta)
                        .ok_or_else(|| "i64 addition overflowed".to_owned())?;
                    check_i64_bounds(result, *min, *max)?;
                    Value::Number(Number::from(result))
                }
                IntegerType::U64 { min, max } => {
                    let current = checked_u64(current, *min, *max)?;
                    let result = add_u64_delta(current, delta)?;
                    check_u64_bounds(result, *min, *max)?;
                    Value::Number(Number::from(result))
                }
            };
            json::set(value, &target.pointer, replacement)?;
            document.dirty = true;
            Ok(())
        }
        Operation::CopyValue { source, target } => {
            let value = document_json_value(source, resolved, working)?.clone();
            set_document_json(target, value, resolved, working)
        }
        Operation::ReplaceOpaque {
            document,
            input,
            content_type,
        } => {
            let value = invocation
                .blobs
                .get(input)
                .cloned()
                .ok_or_else(|| format!("opaque input `{input}` is missing"))?;
            let document = working_document_mut(document, resolved, working)?;
            document.current = Some(StoredValue::Opaque(value));
            document.current_content_type = Some(content_type.clone());
            document.dirty = true;
            Ok(())
        }
    }
}

fn set_document_json(
    target: &JsonPointerRef,
    replacement: Value,
    resolved: &ResolvedInvocation,
    working: &mut [WorkingDocument],
) -> Result<(), String> {
    let document = working_document_mut(&target.document, resolved, working)?;
    if target.pointer.is_empty() {
        document.current = Some(StoredValue::Json(replacement));
        document.current_content_type = Some("application/json".into());
    } else {
        let value = current_json_mut(document)?;
        json::set(value, &target.pointer, replacement)?;
    }
    document.dirty = true;
    Ok(())
}

fn current_json_mut(document: &mut WorkingDocument) -> Result<&mut Value, String> {
    match document.current.as_mut() {
        Some(StoredValue::Json(value)) => Ok(value),
        Some(StoredValue::Opaque(_)) => Err(format!(
            "document {:?} contains opaque bytes, not JSON",
            document.reference
        )),
        None => Err(format!("document {:?} is absent", document.reference)),
    }
}

fn value_source(
    source: &ValueSource,
    invocation: &ProgramInvocation,
    resolved: &ResolvedInvocation,
    working: &[WorkingDocument],
) -> Result<Value, String> {
    match source {
        ValueSource::Literal { value } => Ok(value.clone()),
        ValueSource::Input { name } => invocation
            .inputs
            .get(name)
            .cloned()
            .ok_or_else(|| format!("JSON input `{name}` is missing")),
        ValueSource::Document { source } => {
            Ok(document_json_value(source, resolved, working)?.clone())
        }
    }
}

fn input_value<'a>(
    source: &'a InputValue,
    invocation: &'a ProgramInvocation,
) -> Result<&'a Value, String> {
    match source {
        InputValue::Literal { value } => Ok(value),
        InputValue::Input { name } => invocation
            .inputs
            .get(name)
            .ok_or_else(|| format!("JSON input `{name}` is missing")),
    }
}

fn document_json_value<'a>(
    source: &DocumentValueRef,
    resolved: &ResolvedInvocation,
    working: &'a [WorkingDocument],
) -> Result<&'a Value, String> {
    let document = working_document(&source.value.document, resolved, working)?;
    let value = match source.view {
        DocumentView::Before => document.before.as_ref(),
        DocumentView::Current => document.current.as_ref(),
    };
    json_from_stored(value, &source.value.pointer)
}

fn json_from_stored<'a>(
    value: Option<&'a StoredValue>,
    pointer: &str,
) -> Result<&'a Value, String> {
    match value {
        Some(StoredValue::Json(value)) => json::select(value, pointer),
        Some(StoredValue::Opaque(_)) => Err("document contains opaque bytes, not JSON".into()),
        None => Err("document is absent".into()),
    }
}

fn check_document_limits(
    caps: &ProgramCaps,
    working: &[WorkingDocument],
) -> Result<(), EngineError> {
    for document in working {
        for value in [document.before.as_ref(), document.current.as_ref()]
            .into_iter()
            .flatten()
        {
            let size = stored_value_size(value)?;
            if size > caps.max_document_bytes {
                return Err(EngineError::LimitExceeded(format!(
                    "document {:?} is {size} bytes; max_document_bytes is {}",
                    document.reference, caps.max_document_bytes
                )));
            }
        }
    }
    Ok(())
}

fn stored_value_size(value: &StoredValue) -> Result<usize, EngineError> {
    match value {
        StoredValue::Json(value) => serde_json::to_vec(value)
            .map(|encoded| encoded.len())
            .map_err(|error| EngineError::LimitExceeded(error.to_string())),
        StoredValue::Opaque(value) => Ok(value.len()),
    }
}

fn working_document<'a>(
    reference: &DocumentRef,
    resolved: &ResolvedInvocation,
    working: &'a [WorkingDocument],
) -> Result<&'a WorkingDocument, String> {
    let index = resolved
        .by_reference
        .get(reference)
        .ok_or_else(|| format!("document reference {reference:?} was not bound"))?;
    Ok(&working[*index])
}

fn working_document_mut<'a>(
    reference: &DocumentRef,
    resolved: &ResolvedInvocation,
    working: &'a mut [WorkingDocument],
) -> Result<&'a mut WorkingDocument, String> {
    let index = resolved
        .by_reference
        .get(reference)
        .ok_or_else(|| format!("document reference {reference:?} was not bound"))?;
    Ok(&mut working[*index])
}

fn exact_i64(value: &Value) -> Result<i64, String> {
    value
        .as_i64()
        .ok_or_else(|| "value must be an exact JSON i64 integer".into())
}

fn checked_i64(value: &Value, min: Option<i64>, max: Option<i64>) -> Result<i64, String> {
    let value = exact_i64(value)?;
    check_i64_bounds(value, min, max)?;
    Ok(value)
}

fn check_i64_bounds(value: i64, min: Option<i64>, max: Option<i64>) -> Result<(), String> {
    if min.is_some_and(|min| value < min) || max.is_some_and(|max| value > max) {
        return Err(format!("i64 value {value} is outside configured bounds"));
    }
    Ok(())
}

fn checked_u64(value: &Value, min: Option<u64>, max: Option<u64>) -> Result<u64, String> {
    let value = value
        .as_u64()
        .ok_or_else(|| "value must be an exact JSON u64 integer".to_owned())?;
    check_u64_bounds(value, min, max)?;
    Ok(value)
}

fn check_u64_bounds(value: u64, min: Option<u64>, max: Option<u64>) -> Result<(), String> {
    if min.is_some_and(|min| value < min) || max.is_some_and(|max| value > max) {
        return Err(format!("u64 value {value} is outside configured bounds"));
    }
    Ok(())
}

fn add_u64_delta(current: u64, delta: &Value) -> Result<u64, String> {
    if let Some(signed) = delta.as_i64() {
        if signed < 0 {
            return current
                .checked_sub(signed.unsigned_abs())
                .ok_or_else(|| "u64 subtraction underflowed".into());
        }
        return current
            .checked_add(signed as u64)
            .ok_or_else(|| "u64 addition overflowed".into());
    }
    let unsigned = delta
        .as_u64()
        .ok_or_else(|| "u64 delta must be an exact JSON integer".to_owned())?;
    current
        .checked_add(unsigned)
        .ok_or_else(|| "u64 addition overflowed".into())
}

fn compare<T: Ord>(actual: T, expected: T, comparison: Comparison) -> bool {
    match comparison {
        Comparison::Eq => actual == expected,
        Comparison::Ne => actual != expected,
        Comparison::Lt => actual < expected,
        Comparison::Le => actual <= expected,
        Comparison::Gt => actual > expected,
        Comparison::Ge => actual >= expected,
    }
}

fn invalid_definition(message: impl Into<String>) -> EngineError {
    EngineError::InvalidDefinition(message.into())
}

fn invalid_invocation(message: impl Into<String>) -> EngineError {
    EngineError::InvalidInvocation(message.into())
}

#[derive(Debug)]
struct ResolvedInvocation {
    documents: Vec<ResolvedDocument>,
    document_paths: Vec<ObjectPath>,
    by_reference: BTreeMap<DocumentRef, usize>,
    all_paths: Vec<ObjectPath>,
}

#[derive(Debug)]
struct ResolvedDocument {
    reference: DocumentRef,
    path: ObjectPath,
    expected_head: ExpectedHead,
    initial_json: Option<Value>,
}

#[derive(Debug)]
struct WorkingDocument {
    reference: DocumentRef,
    path: ObjectPath,
    observed: ObservedHead,
    before: Option<StoredValue>,
    current: Option<StoredValue>,
    current_content_type: Option<String>,
    dirty: bool,
}

impl WorkingDocument {
    fn new(resolved: &ResolvedDocument, stored: Option<&VersionedDocument>) -> Self {
        let observed = stored.map_or(ObservedHead::NeverExisted, |document| {
            ObservedHead::Version {
                version: document.version.clone(),
            }
        });
        let before = stored.and_then(|document| document.value.clone());
        let (current, current_content_type, dirty) = match stored {
            Some(document) if document.value.is_some() => {
                (document.value.clone(), document.content_type.clone(), false)
            }
            Some(_) | None => match &resolved.initial_json {
                Some(value) => (
                    Some(StoredValue::Json(value.clone())),
                    Some("application/json".into()),
                    true,
                ),
                None => (None, None, false),
            },
        };
        Self {
            reference: resolved.reference.clone(),
            path: resolved.path.clone(),
            observed,
            before,
            current,
            current_content_type,
            dirty,
        }
    }
}
