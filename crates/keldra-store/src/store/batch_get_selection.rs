use super::BatchGetSelection;

impl BatchGetSelection {
    /// Sum of the declared lengths of structurally valid, present payloads.
    ///
    /// Missing versions, tombstones, selection errors and malformed version
    /// descriptors contribute no bytes. Those retain their existing per-item
    /// outcomes when the selection is materialised.
    pub fn declared_present_payload_bytes(&self) -> u64 {
        self.entries.iter().fold(0_u64, |total, (_, selected)| {
            let length = match selected {
                Ok(Some(version)) => match (&version.blob, version.deleted) {
                    (Some(blob), false) => blob.length,
                    (None, true) => 0,
                    _ => 0,
                },
                Ok(None) | Err(_) => 0,
            };
            total.saturating_add(length)
        })
    }
}
