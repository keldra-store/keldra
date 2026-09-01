//! Metadata-only estimate of reusable immutable reads during one replay selection.

use std::collections::BTreeSet;

use keldra_store::VersionId;
use tonic::Status;

use super::v6_publication::V6ProjectionPublisher;

const MAX_SHADOW_IDENTITIES: usize = 4_096;
const BTREE_IDENTITY_ACCOUNTING_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShadowArtifactKind {
    Page,
    Pack,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ShadowArtifactIdentity {
    storage_tenant: String,
    bucket: String,
    tenant_id: u64,
    bucket_id: u64,
    path: String,
    expected_hash: [u8; 32],
    length: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReuseShadowCounters {
    pub(crate) requests: u64,
    pub(crate) requested_bytes: u64,
    pub(crate) unique_identities: u64,
    pub(crate) unique_bytes: u64,
    pub(crate) pack_requests: u64,
    pub(crate) pack_requested_bytes: u64,
    pub(crate) unique_pack_identities: u64,
    pub(crate) unique_pack_bytes: u64,
    pub(crate) oversize_bypasses: u64,
    pub(crate) oversize_bypass_bytes: u64,
    pub(crate) metadata_limit_bypasses: u64,
    pub(crate) metadata_limit_bypass_bytes: u64,
    pub(crate) peak_simulated_resident_bytes: u64,
}

pub(crate) struct SelectionArtifactReuseShadow {
    // This is only the existing ReplayInput permit bound: the shadow neither
    // acquires credits nor retains artifact payloads. Simulated admission
    // conservatively charges payload plus identity metadata against it.
    capacity: usize,
    resident: usize,
    identities: BTreeSet<ShadowArtifactIdentity>,
    counters: ReuseShadowCounters,
}

impl SelectionArtifactReuseShadow {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            resident: 0,
            identities: BTreeSet::new(),
            counters: ReuseShadowCounters::default(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read(
        &mut self,
        publisher: &V6ProjectionPublisher,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        expected_hash: [u8; 32],
        maximum_bytes: usize,
        kind: ShadowArtifactKind,
    ) -> Result<Option<(Vec<u8>, VersionId)>, Status> {
        let result = publisher
            .read_object(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                path,
                Some(expected_hash),
                maximum_bytes,
            )
            .await?;
        if let Some((bytes, _)) = &result {
            self.observe(
                storage_tenant,
                bucket,
                tenant_id,
                bucket_id,
                path,
                expected_hash,
                bytes.len(),
                kind,
            );
        }
        Ok(result)
    }

    fn observe(
        &mut self,
        storage_tenant: &str,
        bucket: &str,
        tenant_id: u64,
        bucket_id: u64,
        path: &str,
        expected_hash: [u8; 32],
        length: usize,
        kind: ShadowArtifactKind,
    ) {
        let length_u64 = u64::try_from(length).unwrap_or(u64::MAX);
        self.counters.requests = self.counters.requests.saturating_add(1);
        self.counters.requested_bytes = self.counters.requested_bytes.saturating_add(length_u64);
        if kind == ShadowArtifactKind::Pack {
            self.counters.pack_requests = self.counters.pack_requests.saturating_add(1);
            self.counters.pack_requested_bytes = self
                .counters
                .pack_requested_bytes
                .saturating_add(length_u64);
        }
        let identity = ShadowArtifactIdentity {
            storage_tenant: storage_tenant.to_owned(),
            bucket: bucket.to_owned(),
            tenant_id,
            bucket_id,
            path: path.to_owned(),
            expected_hash,
            length: length_u64,
        };
        if self.identities.contains(&identity) {
            return;
        }
        let identity_bytes = std::mem::size_of::<ShadowArtifactIdentity>()
            .saturating_add(storage_tenant.len())
            .saturating_add(bucket.len())
            .saturating_add(path.len())
            .saturating_add(BTREE_IDENTITY_ACCOUNTING_BYTES);
        if self.identities.len() >= MAX_SHADOW_IDENTITIES {
            self.counters.metadata_limit_bypasses =
                self.counters.metadata_limit_bypasses.saturating_add(1);
            self.counters.metadata_limit_bypass_bytes = self
                .counters
                .metadata_limit_bypass_bytes
                .saturating_add(length_u64);
            return;
        }
        let needed = length.saturating_add(identity_bytes);
        if needed > self.capacity.saturating_sub(self.resident) {
            self.counters.oversize_bypasses = self.counters.oversize_bypasses.saturating_add(1);
            self.counters.oversize_bypass_bytes = self
                .counters
                .oversize_bypass_bytes
                .saturating_add(length_u64);
            return;
        }
        self.resident += needed;
        self.counters.unique_identities = self.counters.unique_identities.saturating_add(1);
        self.counters.unique_bytes = self.counters.unique_bytes.saturating_add(length_u64);
        if kind == ShadowArtifactKind::Pack {
            self.counters.unique_pack_identities =
                self.counters.unique_pack_identities.saturating_add(1);
            self.counters.unique_pack_bytes =
                self.counters.unique_pack_bytes.saturating_add(length_u64);
        }
        self.counters.peak_simulated_resident_bytes =
            u64::try_from(self.resident).unwrap_or(u64::MAX);
        self.identities.insert(identity);
    }

    #[cfg(test)]
    fn counters(&self) -> ReuseShadowCounters {
        self.counters
    }
}

impl Drop for SelectionArtifactReuseShadow {
    fn drop(&mut self) {
        super::v6_telemetry::global().record_reuse_shadow(self.counters);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shadow_counts_repeated_exact_identity_once() {
        let mut shadow = SelectionArtifactReuseShadow::new(16 * 1024);
        for _ in 0..2 {
            shadow.observe(
                "tenant",
                "bucket",
                1,
                2,
                "pack",
                [7; 32],
                1024,
                ShadowArtifactKind::Pack,
            );
        }
        let counters = shadow.counters();
        assert_eq!((counters.requests, counters.requested_bytes), (2, 2048));
        assert_eq!(
            (counters.unique_identities, counters.unique_bytes),
            (1, 1024)
        );
        assert_eq!(
            (counters.pack_requests, counters.pack_requested_bytes),
            (2, 2048)
        );
        assert_eq!(
            (counters.unique_pack_identities, counters.unique_pack_bytes),
            (1, 1024)
        );
        assert!(counters.peak_simulated_resident_bytes >= 1024);
    }

    #[test]
    fn identity_includes_scope_path_hash_and_length() {
        let mut shadow = SelectionArtifactReuseShadow::new(64 * 1024);
        for (tenant, bucket, tenant_id, bucket_id, path, hash, length) in [
            ("one", "bucket", 1, 2, "path", [1; 32], 10),
            ("two", "bucket", 1, 2, "path", [1; 32], 10),
            ("one", "other", 1, 2, "path", [1; 32], 10),
            ("one", "bucket", 3, 2, "path", [1; 32], 10),
            ("one", "bucket", 1, 4, "path", [1; 32], 10),
            ("one", "bucket", 1, 2, "other", [1; 32], 10),
            ("one", "bucket", 1, 2, "path", [2; 32], 10),
            ("one", "bucket", 1, 2, "path", [1; 32], 11),
        ] {
            shadow.observe(
                tenant,
                bucket,
                tenant_id,
                bucket_id,
                path,
                hash,
                length,
                ShadowArtifactKind::Page,
            );
        }
        assert_eq!(shadow.counters().unique_identities, 8);
    }

    #[test]
    fn oversize_requests_bypass_the_simulated_residency() {
        let mut shadow = SelectionArtifactReuseShadow::new(512);
        for _ in 0..2 {
            shadow.observe(
                "tenant",
                "bucket",
                1,
                2,
                "pack",
                [3; 32],
                1024,
                ShadowArtifactKind::Pack,
            );
        }
        let counters = shadow.counters();
        assert_eq!((counters.requests, counters.requested_bytes), (2, 2048));
        assert_eq!((counters.unique_identities, counters.unique_bytes), (0, 0));
        assert_eq!(
            (counters.oversize_bypasses, counters.oversize_bypass_bytes),
            (2, 2048)
        );
        assert_eq!(counters.peak_simulated_resident_bytes, 0);
    }

    #[test]
    fn unique_residency_never_exceeds_the_replay_input_capacity() {
        let identity_bytes = std::mem::size_of::<ShadowArtifactIdentity>()
            + "tenant".len()
            + "bucket".len()
            + "page-a".len()
            + BTREE_IDENTITY_ACCOUNTING_BYTES;
        let first_resident = 128 + identity_bytes;
        let mut shadow = SelectionArtifactReuseShadow::new(first_resident);
        shadow.observe(
            "tenant",
            "bucket",
            1,
            2,
            "page-a",
            [1; 32],
            128,
            ShadowArtifactKind::Page,
        );
        shadow.observe(
            "tenant",
            "bucket",
            1,
            2,
            "page-b",
            [2; 32],
            128,
            ShadowArtifactKind::Page,
        );

        let counters = shadow.counters();
        assert_eq!(
            (counters.unique_identities, counters.unique_bytes),
            (1, 128)
        );
        assert_eq!(counters.oversize_bypasses, 1);
        assert_eq!(
            counters.peak_simulated_resident_bytes,
            u64::try_from(first_resident).unwrap()
        );
    }
}
