#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AnvilErrorCode {
    Unauthorized,
    UnauthorizedReservedNamespace,
    ForbiddenByPolicy,
    BucketNotFound,
    ObjectNotFound,
    VersionNotFound,
    PreconditionFailed,
    PartitionNotOwned,
    StaleFenceToken,
    ManifestInvalid,
    SegmentInvalid,
    IndexUnavailable,
    IndexDoesNotSupportQuery,
    AuthzRevisionUnavailable,
    CoreMetaQuorumUnavailable,
    ObjectShardQuorumUnavailable,
    WatchCursorExpired,
    PersonalDbGroupNotFound,
    PersonalDbStaleBase,
    PersonalDbInvalidChangeset,
    PersonalDbUnauthorizedMutation,
    PersonalDbProjectionWriteBackRejected,
    PersonalDbDivergentReplicaRequiresSnapshot,
    PersonalDbSnapshotInvalid,
    PayloadHashMismatch,
    UnsupportedFormatVersion,
    LeaseHeld,
    LeaseExpired,
    StaleFence,
    LeaseOwnerMismatch,
    LeaseCasConflict,
    ResourceExhaustedMetadataBacklog,
    BoundaryRequiredMissing,
    BoundaryBlockLimitUnsatisfied,
    BoundaryRequiredSingleValueViolation,
    BoundaryTypeMismatch,
    BoundaryExtractorUnsupportedContentType,
    BoundaryExtractorBodyTooLarge,
    BoundarySchemaGenerationConflict,
    BoundarySchemaIncompatibleChange,
    BoundaryMigrationRequired,
    BoundaryMigrationInProgress,
    BoundaryMigrationFailed,
}

impl AnvilErrorCode {
    pub const ALL: [Self; 43] = [
        Self::Unauthorized,
        Self::UnauthorizedReservedNamespace,
        Self::ForbiddenByPolicy,
        Self::BucketNotFound,
        Self::ObjectNotFound,
        Self::VersionNotFound,
        Self::PreconditionFailed,
        Self::PartitionNotOwned,
        Self::StaleFenceToken,
        Self::ManifestInvalid,
        Self::SegmentInvalid,
        Self::IndexUnavailable,
        Self::IndexDoesNotSupportQuery,
        Self::AuthzRevisionUnavailable,
        Self::CoreMetaQuorumUnavailable,
        Self::ObjectShardQuorumUnavailable,
        Self::WatchCursorExpired,
        Self::PersonalDbGroupNotFound,
        Self::PersonalDbStaleBase,
        Self::PersonalDbInvalidChangeset,
        Self::PersonalDbUnauthorizedMutation,
        Self::PersonalDbProjectionWriteBackRejected,
        Self::PersonalDbDivergentReplicaRequiresSnapshot,
        Self::PersonalDbSnapshotInvalid,
        Self::PayloadHashMismatch,
        Self::UnsupportedFormatVersion,
        Self::LeaseHeld,
        Self::LeaseExpired,
        Self::StaleFence,
        Self::LeaseOwnerMismatch,
        Self::LeaseCasConflict,
        Self::ResourceExhaustedMetadataBacklog,
        Self::BoundaryRequiredMissing,
        Self::BoundaryBlockLimitUnsatisfied,
        Self::BoundaryRequiredSingleValueViolation,
        Self::BoundaryTypeMismatch,
        Self::BoundaryExtractorUnsupportedContentType,
        Self::BoundaryExtractorBodyTooLarge,
        Self::BoundarySchemaGenerationConflict,
        Self::BoundarySchemaIncompatibleChange,
        Self::BoundaryMigrationRequired,
        Self::BoundaryMigrationInProgress,
        Self::BoundaryMigrationFailed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthorized => "Unauthorized",
            Self::UnauthorizedReservedNamespace => "UnauthorizedReservedNamespace",
            Self::ForbiddenByPolicy => "ForbiddenByPolicy",
            Self::BucketNotFound => "BucketNotFound",
            Self::ObjectNotFound => "ObjectNotFound",
            Self::VersionNotFound => "VersionNotFound",
            Self::PreconditionFailed => "PreconditionFailed",
            Self::PartitionNotOwned => "PartitionNotOwned",
            Self::StaleFenceToken => "StaleFenceToken",
            Self::ManifestInvalid => "ManifestInvalid",
            Self::SegmentInvalid => "SegmentInvalid",
            Self::IndexUnavailable => "IndexUnavailable",
            Self::IndexDoesNotSupportQuery => "IndexDoesNotSupportQuery",
            Self::AuthzRevisionUnavailable => "AuthzRevisionUnavailable",
            Self::CoreMetaQuorumUnavailable => "CoreMetaQuorumUnavailable",
            Self::ObjectShardQuorumUnavailable => "ObjectShardQuorumUnavailable",
            Self::WatchCursorExpired => "WatchCursorExpired",
            Self::PersonalDbGroupNotFound => "PersonalDbGroupNotFound",
            Self::PersonalDbStaleBase => "PersonalDbStaleBase",
            Self::PersonalDbInvalidChangeset => "PersonalDbInvalidChangeset",
            Self::PersonalDbUnauthorizedMutation => "PersonalDbUnauthorizedMutation",
            Self::PersonalDbProjectionWriteBackRejected => "PersonalDbProjectionWriteBackRejected",
            Self::PersonalDbDivergentReplicaRequiresSnapshot => {
                "PersonalDbDivergentReplicaRequiresSnapshot"
            }
            Self::PersonalDbSnapshotInvalid => "PersonalDbSnapshotInvalid",
            Self::PayloadHashMismatch => "PayloadHashMismatch",
            Self::UnsupportedFormatVersion => "UnsupportedFormatVersion",
            Self::LeaseHeld => "LeaseHeld",
            Self::LeaseExpired => "LeaseExpired",
            Self::StaleFence => "StaleFence",
            Self::LeaseOwnerMismatch => "LeaseOwnerMismatch",
            Self::LeaseCasConflict => "LeaseCasConflict",
            Self::ResourceExhaustedMetadataBacklog => "ResourceExhaustedMetadataBacklog",
            Self::BoundaryRequiredMissing => "BoundaryRequiredMissing",
            Self::BoundaryBlockLimitUnsatisfied => "BoundaryBlockLimitUnsatisfied",
            Self::BoundaryRequiredSingleValueViolation => "BoundaryRequiredSingleValueViolation",
            Self::BoundaryTypeMismatch => "BoundaryTypeMismatch",
            Self::BoundaryExtractorUnsupportedContentType => {
                "BoundaryExtractorUnsupportedContentType"
            }
            Self::BoundaryExtractorBodyTooLarge => "BoundaryExtractorBodyTooLarge",
            Self::BoundarySchemaGenerationConflict => "BoundarySchemaGenerationConflict",
            Self::BoundarySchemaIncompatibleChange => "BoundarySchemaIncompatibleChange",
            Self::BoundaryMigrationRequired => "BoundaryMigrationRequired",
            Self::BoundaryMigrationInProgress => "BoundaryMigrationInProgress",
            Self::BoundaryMigrationFailed => "BoundaryMigrationFailed",
        }
    }
}

impl std::fmt::Display for AnvilErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AnvilErrorCode {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|code| code.as_str() == value)
            .ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn error_code_strings_are_the_required_stable_contract() {
        let actual = AnvilErrorCode::ALL
            .into_iter()
            .map(AnvilErrorCode::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                "Unauthorized",
                "UnauthorizedReservedNamespace",
                "ForbiddenByPolicy",
                "BucketNotFound",
                "ObjectNotFound",
                "VersionNotFound",
                "PreconditionFailed",
                "PartitionNotOwned",
                "StaleFenceToken",
                "ManifestInvalid",
                "SegmentInvalid",
                "IndexUnavailable",
                "IndexDoesNotSupportQuery",
                "AuthzRevisionUnavailable",
                "CoreMetaQuorumUnavailable",
                "ObjectShardQuorumUnavailable",
                "WatchCursorExpired",
                "PersonalDbGroupNotFound",
                "PersonalDbStaleBase",
                "PersonalDbInvalidChangeset",
                "PersonalDbUnauthorizedMutation",
                "PersonalDbProjectionWriteBackRejected",
                "PersonalDbDivergentReplicaRequiresSnapshot",
                "PersonalDbSnapshotInvalid",
                "PayloadHashMismatch",
                "UnsupportedFormatVersion",
                "LeaseHeld",
                "LeaseExpired",
                "StaleFence",
                "LeaseOwnerMismatch",
                "LeaseCasConflict",
                "ResourceExhaustedMetadataBacklog",
                "BoundaryRequiredMissing",
                "BoundaryBlockLimitUnsatisfied",
                "BoundaryRequiredSingleValueViolation",
                "BoundaryTypeMismatch",
                "BoundaryExtractorUnsupportedContentType",
                "BoundaryExtractorBodyTooLarge",
                "BoundarySchemaGenerationConflict",
                "BoundarySchemaIncompatibleChange",
                "BoundaryMigrationRequired",
                "BoundaryMigrationInProgress",
                "BoundaryMigrationFailed",
            ]
        );
    }

    #[test]
    fn error_code_parse_round_trips_every_code() {
        for code in AnvilErrorCode::ALL {
            assert_eq!(AnvilErrorCode::from_str(code.as_str()), Ok(code));
            assert_eq!(code.to_string(), code.as_str());
        }
        assert_eq!(AnvilErrorCode::from_str("NotAStableCode"), Err(()));
    }
}
