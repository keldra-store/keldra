//! Fail-closed boundary for addressed access to Anvil's reserved object paths.
//!
//! The capability below exists only in a tonic request's in-process extension
//! map. It has no protobuf representation and is never inferred from caller
//! input, bearer claims, or an ordinary routed-public peer request.

use anvil_store::ObjectKey;
use tonic::{Request, Status};

const PROGRAM_DEFINITION_PREFIX: &str = "_anvil/programs/";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectPathClass {
    Public,
    ProgramDefinition,
    Internal,
}

#[derive(Clone, Copy, Debug)]
struct InternalObjectRequest;

/// Opaque access evidence captured before a streaming request is consumed.
/// Its fields are private so another module cannot manufacture internal access.
pub(crate) struct ObjectPathAccess {
    internal: bool,
}

pub(crate) fn access_for<T>(request: &Request<T>) -> ObjectPathAccess {
    ObjectPathAccess {
        internal: request
            .extensions()
            .get::<InternalObjectRequest>()
            .is_some(),
    }
}

/// Marks one request created by the index lifecycle adapter inside this process.
pub(crate) fn mark_index<T>(request: &mut Request<T>) {
    request.extensions_mut().insert(InternalObjectRequest);
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

pub(crate) fn require_key(access: &ObjectPathAccess, key: &ObjectKey) -> Result<(), Status> {
    require_path(access, key.path())
}

pub(crate) fn require_public_key(key: &ObjectKey) -> Result<(), Status> {
    require_path(&ObjectPathAccess { internal: false }, key.path())
}

pub(crate) fn require_path(access: &ObjectPathAccess, path: &str) -> Result<(), Status> {
    match classify(path) {
        ObjectPathClass::Public | ObjectPathClass::ProgramDefinition => Ok(()),
        ObjectPathClass::Internal if access.internal => Ok(()),
        ObjectPathClass::Internal => Err(Status::permission_denied(
            "the addressed object path is reserved for an internal Anvil capability",
        )),
    }
}

pub(crate) fn is_internal(access: &ObjectPathAccess) -> bool {
    access.internal
}

fn classify(path: &str) -> ObjectPathClass {
    if is_program_definition(path) {
        ObjectPathClass::ProgramDefinition
    } else if path.split('/').any(|segment| segment == "_anvil") {
        ObjectPathClass::Internal
    } else {
        ObjectPathClass::Public
    }
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

    fn key(path: &str) -> ObjectKey {
        ObjectKey::new("tenant", "bucket", path).unwrap()
    }

    #[test]
    fn only_canonical_program_definitions_are_public_reserved_paths() {
        let public = access_for(&Request::new(()));
        assert!(require_key(&public, &key("objects/value")).is_ok());
        assert!(require_key(&public, &key("_anvil/programs/import_osv@1")).is_ok());

        for path in [
            "_anvil/programs/import_osv",
            "_anvil/programs/@1",
            "_anvil/programs/import_osv@",
            "_anvil/programs/nested/import_osv@1",
            "_anvil/programs/import_osv@1@copy",
            "_anvil/indexes/v2/definitions/by-path",
            "_anvil/indexes/v2/7/current",
            "_anvil/internal/00",
            "objects/_anvil/meta.json",
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
        assert!(require_key(&access, &key("_anvil/indexes/v2/definitions/by-path")).is_ok());
        assert!(require_key(&access, &key("_anvil/indexes/v2/7/current")).is_ok());
    }
}
