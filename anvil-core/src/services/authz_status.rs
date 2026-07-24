use tonic::Status;

pub(crate) fn consistency_status(error: anyhow::Error) -> Status {
    let message = format!("{error:#}");
    if message.contains(crate::error_codes::AnvilErrorCode::AuthzRevisionUnavailable.as_str()) {
        Status::failed_precondition(message)
    } else {
        Status::internal(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revision_unavailable_is_a_consistency_precondition() {
        let status = consistency_status(anyhow::anyhow!(
            "AuthzRevisionUnavailable: requested revision is not materialized"
        ));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(status.message().contains("AuthzRevisionUnavailable"));
    }

    #[test]
    fn unrelated_failures_remain_internal() {
        let status = consistency_status(anyhow::anyhow!("corrupt projection row"));
        assert_eq!(status.code(), tonic::Code::Internal);
    }
}
