//! Authorization policies used by the public index qualification.

use keldra_storage::v1::index_result_authorization;
use keldra_storage::v1::{ApplicationIndexResultAuthorization, IndexResultAuthorization};

pub(super) fn application_result_authorization() -> IndexResultAuthorization {
    IndexResultAuthorization {
        policy: Some(index_result_authorization::Policy::Application(
            ApplicationIndexResultAuthorization {},
        )),
    }
}
