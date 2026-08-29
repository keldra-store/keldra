//! Fail-closed boundary for addressed access to Keldra's reserved object paths.
//!
//! The capability below exists only in a tonic request's in-process extension
//! map. It has no protobuf representation and is never inferred from caller
//! input, bearer claims, or an ordinary routed-public peer request.

use std::collections::BTreeMap;

use keldra_store::{DefinitionMutationIntent, ObjectKey};
use tonic::{Request, Status};

const PROGRAM_DEFINITION_PREFIX: &str = "_keldra/programs/";
const PLUGIN_BINDING_PREFIX: &str = "_keldra/plugins/";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectPathClass {
    Public,
    ProgramDefinition,
    PluginBinding,
    Internal,
}

#[derive(Clone, Copy, Debug)]
struct InternalObjectRequest;

/// Opaque access evidence captured before a streaming request is consumed.
/// Its fields are private so another module cannot manufacture internal access.
#[derive(Clone)]
pub(crate) struct ObjectPathAccess {
    internal: bool,
    definition_intents: BTreeMap<usize, DefinitionMutationIntent>,
}

pub(crate) fn access_for<T>(request: &Request<T>) -> ObjectPathAccess {
    ObjectPathAccess {
        internal: request
            .extensions()
            .get::<InternalObjectRequest>()
            .is_some(),
        definition_intents: request
            .extensions()
            .get::<DefinitionMutationIntents>()
            .map(|intents| intents.0.clone())
            .unwrap_or_default(),
    }
}

#[derive(Clone, Debug, Default)]
struct DefinitionMutationIntents(BTreeMap<usize, DefinitionMutationIntent>);

/// Marks one request created by the index lifecycle adapter inside this process.
pub(crate) fn mark_index<T>(request: &mut Request<T>) {
    request.extensions_mut().insert(InternalObjectRequest);
}

/// Marks one exact operation issued by the trusted index lifecycle service.
///
/// The evidence is process-local and cannot be supplied by a public client.
/// When the path coordinator is remote, the authenticated private peer route
/// carries the same typed fields and reconstructs this evidence there.
pub(crate) fn mark_index_definition<T>(
    request: &mut Request<T>,
    operation_index: usize,
    intent: DefinitionMutationIntent,
) {
    request.extensions_mut().insert(InternalObjectRequest);
    let mut intents = BTreeMap::new();
    intents.insert(operation_index, intent);
    request
        .extensions_mut()
        .insert(DefinitionMutationIntents(intents));
}

/// Marks one request created by the in-process PersonalDB adapter. The marker
/// has no wire representation; the mandatory-mTLS internal bulk route restores
/// the same capability after independently authenticating the peer.
pub(crate) fn mark_personaldb<T>(request: &mut Request<T>) {
    request.extensions_mut().insert(InternalObjectRequest);
}

/// Marks a request issued by a protocol gateway after that gateway has
/// validated its exact reserved namespace and retained Zanzibar authorization.
pub(crate) fn mark_gateway<T>(request: &mut Request<T>) {
    request.extensions_mut().insert(InternalObjectRequest);
}

/// Restores the in-process marker only on the dedicated mandatory-mTLS
/// internal route. Ordinary routed-public calls never invoke this function.
pub(crate) fn mark_internal_peer_route<T>(request: &mut Request<T>) {
    request.extensions_mut().insert(InternalObjectRequest);
}

/// Restores typed definition evidence only after the private route has passed
/// mandatory peer authentication and bounded protobuf validation.
pub(crate) fn mark_internal_peer_definition_route<T>(
    request: &mut Request<T>,
    intents: impl IntoIterator<Item = (usize, DefinitionMutationIntent)>,
) {
    request.extensions_mut().insert(InternalObjectRequest);
    request
        .extensions_mut()
        .insert(DefinitionMutationIntents(intents.into_iter().collect()));
}

pub(crate) fn require_key(access: &ObjectPathAccess, key: &ObjectKey) -> Result<(), Status> {
    require_path(access, key.path())
}

pub(crate) fn require_public_key(key: &ObjectKey) -> Result<(), Status> {
    require_path(
        &ObjectPathAccess {
            internal: false,
            definition_intents: BTreeMap::new(),
        },
        key.path(),
    )
}

pub(crate) fn require_path(access: &ObjectPathAccess, path: &str) -> Result<(), Status> {
    match classify(path) {
        ObjectPathClass::Public
        | ObjectPathClass::ProgramDefinition
        | ObjectPathClass::PluginBinding => Ok(()),
        ObjectPathClass::Internal if access.internal => Ok(()),
        ObjectPathClass::Internal => Err(Status::permission_denied(
            "the addressed object path is reserved for an internal Keldra capability",
        )),
    }
}

pub(crate) fn is_internal(access: &ObjectPathAccess) -> bool {
    access.internal
}

pub(crate) fn definition_intent(
    access: &ObjectPathAccess,
    operation_index: usize,
) -> Option<DefinitionMutationIntent> {
    access.definition_intents.get(&operation_index).copied()
}

pub(crate) fn validate_definition_intents(
    access: &ObjectPathAccess,
    operation_count: usize,
) -> Result<(), Status> {
    if !access.internal && !access.definition_intents.is_empty() {
        return Err(Status::permission_denied(
            "definition mutation evidence requires internal object access",
        ));
    }
    if access
        .definition_intents
        .keys()
        .any(|index| *index >= operation_count)
    {
        return Err(Status::invalid_argument(
            "definition mutation evidence names an invalid bulk operation",
        ));
    }
    Ok(())
}

fn classify(path: &str) -> ObjectPathClass {
    if is_program_definition(path) {
        ObjectPathClass::ProgramDefinition
    } else if is_plugin_binding(path) {
        ObjectPathClass::PluginBinding
    } else if path.split('/').any(|segment| segment == "_keldra") {
        ObjectPathClass::Internal
    } else {
        ObjectPathClass::Public
    }
}

pub(crate) fn is_plugin_binding(path: &str) -> bool {
    let Some(name_and_version) = path.strip_prefix(PLUGIN_BINDING_PREFIX) else {
        return false;
    };
    !name_and_version.contains('/')
        && name_and_version.matches('@').count() == 1
        && name_and_version
            .split_once('@')
            .is_some_and(|(name, version)| {
                canonical_plugin_component(name) && canonical_plugin_component(version)
            })
}

fn canonical_plugin_component(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_program_definition(path: &str) -> bool {
    let Some(name_and_version) = path.strip_prefix(PROGRAM_DEFINITION_PREFIX) else {
        return false;
    };
    if name_and_version.contains('/') || name_and_version.matches('@').count() != 1 {
        return false;
    }
    name_and_version
        .split_once('@')
        .is_some_and(|(name, version)| !name.is_empty() && !version.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_service::definition_path;

    fn key(path: &str) -> ObjectKey {
        ObjectKey::new("tenant", "bucket", path).unwrap()
    }

    #[test]
    fn only_canonical_program_definitions_are_public_reserved_paths() {
        let public = access_for(&Request::new(()));
        assert!(require_key(&public, &key("objects/value")).is_ok());
        assert!(require_key(&public, &key("_keldra/programs/import_osv@1")).is_ok());
        let index_definition = definition_path("by-path").unwrap();

        for path in [
            "_keldra/programs/import_osv",
            "_keldra/programs/@1",
            "_keldra/programs/import_osv@",
            "_keldra/programs/nested/import_osv@1",
            "_keldra/programs/import_osv@1@copy",
            index_definition.as_str(),
            "_keldra/indices/v3/7/current",
            "_keldra/internal/00",
            "objects/_keldra/meta.json",
        ] {
            assert_eq!(
                require_key(&public, &key(path)).unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "{path}"
            );
        }
    }

    #[test]
    fn only_exact_versioned_plugin_bindings_are_public_reserved_paths() {
        let public = access_for(&Request::new(()));
        assert!(require_key(&public, &key("_keldra/plugins/oci@1")).is_ok());

        for path in [
            "_keldra/plugins/oci",
            "_keldra/plugins/@1",
            "_keldra/plugins/oci@",
            "_keldra/plugins/oci@1/config",
            "_keldra/plugins/oci@1@copy",
            "_keldra/plugins/oci plugin@1",
        ] {
            assert_eq!(
                require_key(&public, &key(path)).unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "{path}"
            );
        }
    }

    #[test]
    fn index_marker_allows_definition_and_artifact_access() {
        let mut request = Request::new(());
        mark_index(&mut request);
        let access = access_for(&request);
        assert!(require_key(&access, &key(&definition_path("by-path").unwrap())).is_ok());
        assert!(require_key(&access, &key("_keldra/indices/v4/7/current")).is_ok());
    }
}
