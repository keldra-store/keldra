use std::path::Path;

use super::*;

// One cache and write-buffer manager are shared across every column family.
#[cfg(test)]
pub(super) const METADATA_BLOCK_CACHE_BYTES: usize = DEFAULT_ROCKSDB_BLOCK_CACHE_BYTES as usize;
#[cfg(test)]
pub(super) const METADATA_WRITE_BUFFER_MANAGER_BYTES: usize =
    DEFAULT_ROCKSDB_WRITE_BUFFER_MANAGER_BYTES as usize;
#[cfg(test)]
pub(super) const METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES: usize =
    DEFAULT_ROCKSDB_COLUMN_FAMILY_WRITE_BUFFER_BYTES as usize;
const PAYLOAD_BLOB_FILE_BYTES: u64 = 256 * 1024 * 1024;

impl MetadataMemoryResources {
    pub(super) fn new(resources: RocksDbResourceBudget) -> Self {
        let block_cache_bytes = usize::try_from(resources.block_cache_bytes)
            .expect("validated RocksDB block cache fits usize");
        let write_buffer_manager_bytes = usize::try_from(resources.write_buffer_manager_bytes)
            .expect("validated RocksDB write buffer manager fits usize");
        let column_family_write_buffer_bytes =
            usize::try_from(resources.column_family_write_buffer_bytes)
                .expect("validated RocksDB column-family write buffer fits usize");
        Self {
            block_cache: Cache::new_lru_cache(block_cache_bytes),
            write_buffer_manager: WriteBufferManager::new_write_buffer_manager(
                write_buffer_manager_bytes,
                true,
            ),
            block_cache_capacity_bytes: resources.block_cache_bytes,
            column_family_write_buffer_bytes,
        }
    }

    pub(super) fn column_family_options(&self) -> Options {
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&self.block_cache);

        let mut options = Options::default();
        options.set_block_based_table_factory(&table);
        options.set_write_buffer_manager(&self.write_buffer_manager);
        options.set_write_buffer_size(self.column_family_write_buffer_bytes);
        options
    }

    pub(super) fn payload_column_family_options(
        &self,
        payload_directory: &Path,
    ) -> Result<Options> {
        let mut options = self.column_family_options();
        let path = DBPath::new(payload_directory, u64::MAX)
            .with_context(|| format!("configure payload path {}", payload_directory.display()))?;
        options.set_cf_paths(&[path]);
        options.set_enable_blob_files(true);
        options.set_min_blob_size(PAYLOAD_BLOB_MIN_BYTES);
        options.set_blob_file_size(PAYLOAD_BLOB_FILE_BYTES);
        // Payloads and immutable index artifacts are already bounded and
        // content-addressed by Keldra. Blob compression would add CPU work,
        // while even an uncompressed BlobDB miss materializes an owned value.
        // Reuse the bounded shared cache so independently repeated local reads
        // can reuse the same blob instead of repeating its pread and copy.
        options.set_blob_compression_type(rocksdb::DBCompressionType::None);
        options.set_blob_cache(&self.block_cache);
        options.set_enable_blob_gc(true);
        options.set_blob_gc_age_cutoff(0.25);
        options.set_blob_gc_force_threshold(0.75);
        options.set_periodic_compaction_seconds(60 * 60);
        Ok(options)
    }
}

impl MetadataRuntimeMetrics {
    pub(super) fn note_unavailable(&mut self, property: &'static str) {
        self.unavailable_properties = self.unavailable_properties.saturating_add(1);
        self.first_unavailable_property.get_or_insert(property);
    }

    pub(super) fn note_failure(&mut self, error: String) {
        self.property_collection_failures = self.property_collection_failures.saturating_add(1);
        self.first_collection_error.get_or_insert(error);
    }
}
