use crate::anvil_api::{NativeMutationContext, PublicMutationContext, WriteOptions, write_options};
use tonic::Status;

pub(crate) fn write_options_transaction_id(
    options: Option<&WriteOptions>,
) -> Result<Option<&str>, Status> {
    let Some(options) = options else {
        return Ok(None);
    };
    options
        .execution
        .as_ref()
        .map(|execution| match execution {
            write_options::Execution::TransactionId(transaction_id) => {
                validate_transaction_id(transaction_id)
            }
        })
        .transpose()
}

pub(crate) fn write_options_is_transactional(options: Option<&WriteOptions>) -> bool {
    matches!(
        options.and_then(|options| options.execution.as_ref()),
        Some(write_options::Execution::TransactionId(_))
    )
}

pub(crate) fn native_context_transaction_id(
    context: Option<&NativeMutationContext>,
) -> Result<Option<&str>, Status> {
    let Some(context) = context else {
        return Ok(None);
    };
    match context.transaction_id.as_deref() {
        None => Ok(None),
        Some(transaction_id) => validate_transaction_id(transaction_id).map(Some),
    }
}

pub(crate) fn public_context_transaction_id(
    context: &PublicMutationContext,
) -> Result<Option<&str>, Status> {
    match context.transaction_id.as_deref() {
        None => Ok(None),
        Some(transaction_id) => validate_transaction_id(transaction_id).map(Some),
    }
}

fn validate_transaction_id(transaction_id: &str) -> Result<&str, Status> {
    if transaction_id.trim().is_empty() {
        return Err(Status::invalid_argument("transaction_id must not be empty"));
    }
    Ok(transaction_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anvil_api::{
        NativeMutationContext, PublicMutationContext, WriteOptions, write_options,
    };

    #[test]
    fn write_options_extracts_transaction_id() {
        let options = WriteOptions {
            idempotency_key: String::new(),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: Some(write_options::Execution::TransactionId("tx-1".to_string())),
        };

        assert_eq!(
            write_options_transaction_id(Some(&options)).unwrap(),
            Some("tx-1")
        );
    }

    #[test]
    fn mutation_contexts_extract_transaction_ids() {
        let native = NativeMutationContext {
            tenant_id: 1,
            bucket_id: 2,
            principal: "principal".to_string(),
            request_id: "request".to_string(),
            precondition: "none".to_string(),
            authz_zookie_optional: String::new(),
            idempotency_key: "idem".to_string(),
            transaction_id: Some("tx-native".to_string()),
            write_visibility: None,
        };
        let public = PublicMutationContext {
            request_id: "public-request".to_string(),
            idempotency_key: "public-idem".to_string(),
            expected_generation: 1,
            transaction_id: Some("tx-public".to_string()),
        };

        assert_eq!(
            native_context_transaction_id(Some(&native)).unwrap(),
            Some("tx-native")
        );
        assert_eq!(
            public_context_transaction_id(&public).unwrap(),
            Some("tx-public")
        );
    }
}
