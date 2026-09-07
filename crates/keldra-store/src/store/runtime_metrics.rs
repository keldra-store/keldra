use super::*;

impl Store {
    pub(super) fn column_family_property_sum(
        &self,
        property: &rocksdb::properties::PropName,
        signal: &'static str,
        excluded_column_family: Option<&str>,
        metrics: &mut MetadataRuntimeMetrics,
    ) -> Option<u64> {
        let mut total = 0_u64;
        for name in std::iter::once(DEFAULT_COLUMN_FAMILY_NAME)
            .chain(COLUMN_FAMILIES.iter().copied())
            .filter(|name| Some(*name) != excluded_column_family)
        {
            let Some(column_family) = self.db.cf_handle(name) else {
                metrics.note_failure(format!("missing metadata column family {name}"));
                return None;
            };
            let value = match self.db.property_int_value_cf(column_family, property) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    metrics.note_unavailable(signal);
                    return None;
                }
                Err(error) => {
                    metrics.note_failure(format!(
                        "read RocksDB property {signal} for {name}: {error}"
                    ));
                    return None;
                }
            };
            let Some(next) = total.checked_add(value) else {
                metrics.note_failure(format!("RocksDB metric {signal} overflowed u64"));
                return None;
            };
            total = next;
        }
        Some(total)
    }
}
