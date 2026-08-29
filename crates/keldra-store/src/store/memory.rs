use std::path::Path;

use super::*;

// One cache and write-buffer manager are shared across every column family.
pub(super) const METADATA_BLOCK_CACHE_BYTES: usize = 64 * 1024 * 1024;
pub(super) const METADATA_WRITE_BUFFER_MANAGER_BYTES: usize = 128 * 1024 * 1024;
pub(super) const METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES: usize = 16 * 1024 * 1024;
const PAYLOAD_BLOB_FILE_BYTES: u64 = 256 * 1024 * 1024;

impl MetadataMemoryResources {
    pub(super) fn new() -> Self {
        Self {
            block_cache: Cache::new_lru_cache(METADATA_BLOCK_CACHE_BYTES),
            write_buffer_manager: WriteBufferManager::new_write_buffer_manager(
                METADATA_WRITE_BUFFER_MANAGER_BYTES,
                true,
            ),
        }
    }

    pub(super) fn column_family_options(&self) -> Options {
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&self.block_cache);

        let mut options = Options::default();
        options.set_block_based_table_factory(&table);
        options.set_write_buffer_manager(&self.write_buffer_manager);
        options.set_write_buffer_size(METADATA_COLUMN_FAMILY_WRITE_BUFFER_BYTES);
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
