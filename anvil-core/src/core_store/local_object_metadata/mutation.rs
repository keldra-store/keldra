use super::*;

impl CoreStore {
    pub(crate) fn object_metadata_current_tuple_key(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Vec<u8> {
        object_current_key(bucket, object_key)
    }
}

impl CoreStore {
    pub(super) async fn current_object_metadata_root_generation(
        &self,
        bucket: &Bucket,
    ) -> Result<u64> {
        let counter_key = object_id_counter_key(bucket);
        let Some(payload) = self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )?
        else {
            return Ok(0);
        };
        let counter = decode_object_metadata_counter_for_bucket(&payload, bucket)?;
        Ok(counter
            .common
            .expect("counter decoder requires CoreMeta common")
            .root_generation)
    }
}
