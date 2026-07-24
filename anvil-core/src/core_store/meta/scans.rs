use anyhow::{Context, Result, bail};
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::time::Instant;

use super::{
    CoreMetaEncodedOwnedRow, CoreMetaEncodedRowsCursor, CoreMetaEncodedRowsPage,
    CoreMetaReadSnapshot, CoreMetaRecord, CoreMetaStore, column_families, core_meta_key,
    decode_core_meta_table_id, decode_core_meta_tuple_key, decode_envelope,
    decode_envelope_with_common, encode_envelope_with_common, exclusive_prefix_successor,
    table_spec, validate_meta_payload, validate_scan_limit,
};

impl CoreMetaReadSnapshot<'_> {
    pub(crate) fn scan_encoded_rows_page(
        &self,
        after: Option<&CoreMetaEncodedRowsCursor>,
        limit: usize,
    ) -> Result<CoreMetaEncodedRowsPage> {
        validate_scan_limit(limit)?;
        let start_cf_index = match after {
            Some(cursor) => {
                if cursor.core_meta_key.is_empty() {
                    bail!("CoreMeta encoded-row cursor key must not be empty");
                }
                let table_id = decode_core_meta_table_id(&cursor.core_meta_key)?;
                if table_spec(table_id)?.cf != cursor.cf.as_str() {
                    bail!("CoreMeta encoded-row cursor key is outside its column family");
                }
                column_families()
                    .iter()
                    .position(|cf| *cf == cursor.cf.as_str())
                    .context("CoreMeta encoded-row cursor column family is unknown")?
            }
            None => 0,
        };
        let mut rows = Vec::with_capacity(limit + 1);
        let started_at = Instant::now();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;

        'column_families: for (cf_index, cf_name) in
            column_families().iter().enumerate().skip(start_cf_index)
        {
            let column_family = self.store.cf(cf_name)?;
            let after_key = after
                .filter(|_| cf_index == start_cf_index)
                .map(|cursor| cursor.core_meta_key.as_slice());
            let start_key = after_key.unwrap_or_default();
            let mut read_options = ReadOptions::default();
            read_options.set_iterate_lower_bound(start_key.to_vec());
            let iter = self.snapshot.iterator_cf_opt(
                &column_family,
                read_options,
                IteratorMode::From(start_key, Direction::Forward),
            );
            for item in iter {
                let (key, value) = item?;
                if after_key.is_some_and(|after_key| key.as_ref() <= after_key) {
                    continue;
                }
                scanned = scanned.saturating_add(1);
                bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                let table_id = decode_core_meta_table_id(&key)?;
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                let value_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaEncodedOwnedRow {
                    cf: (*cf_name).to_string(),
                    core_meta_key: key.to_vec(),
                    value_envelope,
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
                if rows.len() > limit {
                    break 'column_families;
                }
            }
        }

        crate::perf::record_coremeta_duration(
            "snapshot_scan_encoded_rows_page",
            "multi",
            0,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        let has_more = rows.len() > limit;
        if has_more {
            rows.truncate(limit);
        }
        let next_cursor = has_more.then(|| {
            let row = rows
                .last()
                .expect("a full CoreMeta encoded-row page must have a final row");
            CoreMetaEncodedRowsCursor {
                cf: row.cf.clone(),
                core_meta_key: row.core_meta_key.clone(),
            }
        });
        Ok(CoreMetaEncodedRowsPage { rows, next_cursor })
    }

    pub(crate) fn scan_prefix_page(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_scan_limit(limit)?;
        validate_meta_payload(cf, table_id, 0)?;
        let prefix = core_meta_key(table_id, 0, tuple_prefix)?;
        let upper_bound = exclusive_prefix_successor(&prefix)
            .context("CoreMeta prefix has no finite exclusive upper bound")?;
        let after_key = after_tuple_key
            .map(|tuple_key| core_meta_key(table_id, 0, tuple_key))
            .transpose()?;
        if after_key
            .as_ref()
            .is_some_and(|after_key| !after_key.starts_with(&prefix))
        {
            bail!("CoreMeta page position is outside the requested prefix");
        }

        let start_key = after_key.as_ref().unwrap_or(&prefix);
        let column_family = self.store.cf(cf)?;
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(start_key.clone());
        read_options.set_iterate_upper_bound(upper_bound);
        let iter = self.snapshot.iterator_cf_opt(
            &column_family,
            read_options,
            IteratorMode::From(start_key, Direction::Forward),
        );
        let mut records = Vec::with_capacity(limit);
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                bail!("bounded CoreMeta snapshot iterator returned an out-of-range key");
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            if after_key
                .as_ref()
                .is_some_and(|after_key| key.as_ref() <= after_key.as_slice())
            {
                continue;
            }
            let _ = decode_core_meta_tuple_key(&key)?;
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf, table_id, &value)?,
            });
            if records.len() == limit {
                break;
            }
        }
        crate::perf::record_coremeta_duration(
            "snapshot_scan_prefix_page",
            cf,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }
}

impl CoreMetaStore {
    pub fn scan_prefix_page(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
        after_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_scan_limit(limit)?;
        validate_meta_payload(cf, table_id, 0)?;
        let prefix = core_meta_key(table_id, 0, tuple_prefix)?;
        let upper_bound = exclusive_prefix_successor(&prefix)
            .context("CoreMeta prefix has no finite exclusive upper bound")?;
        let after_key = after_tuple_key
            .map(|tuple_key| core_meta_key(table_id, 0, tuple_key))
            .transpose()?;
        if after_key
            .as_ref()
            .is_some_and(|after_key| !after_key.starts_with(&prefix))
        {
            bail!("CoreMeta page position is outside the requested prefix");
        }

        let start_key = after_key.as_ref().unwrap_or(&prefix);
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(start_key.clone());
        read_options.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_cf_opt(
            &cf,
            read_options,
            IteratorMode::From(start_key, Direction::Forward),
        );
        let mut records = Vec::with_capacity(limit);
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                bail!("bounded CoreMeta prefix iterator returned an out-of-range key");
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            if after_key
                .as_ref()
                .is_some_and(|after_key| key.as_ref() <= after_key.as_slice())
            {
                continue;
            }
            let _ = decode_core_meta_tuple_key(&key)?;
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf_name, table_id, &value)?,
            });
            if records.len() == limit {
                break;
            }
        }
        crate::perf::record_coremeta_duration(
            "scan_prefix_page",
            cf_name,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }

    pub fn scan_prefix_reverse_page(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
        before_tuple_key: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_scan_limit(limit)?;
        validate_meta_payload(cf, table_id, 0)?;
        let prefix = core_meta_key(table_id, 0, tuple_prefix)?;
        let upper_bound = exclusive_prefix_successor(&prefix)
            .context("CoreMeta prefix has no finite exclusive upper bound")?;
        let before_key = before_tuple_key
            .map(|tuple_key| core_meta_key(table_id, 0, tuple_key))
            .transpose()?;
        if before_key
            .as_ref()
            .is_some_and(|before_key| !before_key.starts_with(&prefix))
        {
            bail!("CoreMeta reverse page position is outside the requested prefix");
        }

        let start_key = before_key.clone().unwrap_or_else(|| upper_bound.clone());
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(prefix.clone());
        read_options.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_cf_opt(
            &cf,
            read_options,
            IteratorMode::From(&start_key, Direction::Reverse),
        );
        let mut records = Vec::with_capacity(limit);
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                bail!("bounded CoreMeta reverse prefix iterator returned an out-of-range key");
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            if before_key
                .as_ref()
                .is_some_and(|before_key| key.as_ref() >= before_key.as_slice())
            {
                continue;
            }
            let _ = decode_core_meta_tuple_key(&key)?;
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf_name, table_id, &value)?,
            });
            if records.len() == limit {
                break;
            }
        }
        crate::perf::record_coremeta_duration(
            "scan_prefix_reverse_page",
            cf_name,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }

    pub fn scan_range_inclusive(
        &self,
        cf: &'static str,
        table_id: u16,
        start_tuple_key: &[u8],
        end_tuple_key: &[u8],
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_scan_limit(limit)?;
        validate_meta_payload(cf, table_id, 0)?;
        let start_key = core_meta_key(table_id, 0, start_tuple_key)?;
        let end_key = core_meta_key(table_id, 0, end_tuple_key)?;
        if start_key > end_key {
            bail!("CoreMeta scan range start key exceeds end key");
        }
        let upper_bound = exclusive_prefix_successor(&end_key)
            .context("CoreMeta range has no finite exclusive upper bound")?;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(start_key.clone());
        read_options.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_cf_opt(
            &cf,
            read_options,
            IteratorMode::From(&start_key, Direction::Forward),
        );
        let mut records = Vec::new();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if key.as_ref() > end_key.as_slice() {
                break;
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf_name, table_id, &value)?,
            });
            if records.len() >= limit {
                break;
            }
        }
        crate::perf::record_coremeta_duration(
            "scan_range",
            cf_name,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }

    pub fn scan_range_reverse_inclusive(
        &self,
        cf: &'static str,
        table_id: u16,
        start_tuple_key: &[u8],
        end_tuple_key: &[u8],
        limit: usize,
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_scan_limit(limit)?;
        validate_meta_payload(cf, table_id, 0)?;
        let start_key = core_meta_key(table_id, 0, start_tuple_key)?;
        let end_key = core_meta_key(table_id, 0, end_tuple_key)?;
        if start_key > end_key {
            bail!("CoreMeta reverse scan range start key exceeds end key");
        }
        let upper_bound = exclusive_prefix_successor(&end_key)
            .context("CoreMeta reverse range has no finite exclusive upper bound")?;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let mut read_options = ReadOptions::default();
        read_options.set_iterate_lower_bound(start_key.clone());
        read_options.set_iterate_upper_bound(upper_bound);
        let iter = self.db.iterator_cf_opt(
            &cf,
            read_options,
            IteratorMode::From(&end_key, Direction::Reverse),
        );
        let mut records = Vec::new();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if key.as_ref() < start_key.as_slice() {
                break;
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf_name, table_id, &value)?,
            });
            if records.len() >= limit {
                break;
            }
        }
        crate::perf::record_coremeta_duration(
            "scan_range_reverse",
            cf_name,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }
}
