use std::collections::BTreeMap;

use crate::io::read_exact_at;
use crate::{IndexError, IndexFileRead};

const FILE_MAGIC: &[u8; 8] = b"ANVXMAP\0";
const PAGE_MAGIC: &[u8; 8] = b"ANVPG001";
const FORMAT_VERSION: u16 = 1;
const HEADER_BYTES: usize = 64;
const PAGE_HEADER_BYTES: usize = 48;
const PAGE_HEADER_BYTES_U32: u32 = 48;
const DIRECTORY_ENTRY_FIXED_BYTES: usize = 24;
pub const DEFAULT_PAGE_BYTES: usize = 64 * 1024;
pub type MapRecord = (Vec<u8>, Vec<u8>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct PageDirectoryEntry {
    offset: u64,
    length: u32,
    record_count: u32,
    minimum_key: Vec<u8>,
    maximum_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PagedMapBuilder {
    target_page_bytes: usize,
    records: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Default for PagedMapBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_PAGE_BYTES)
    }
}

impl PagedMapBuilder {
    pub fn new(target_page_bytes: usize) -> Self {
        Self {
            target_page_bytes: target_page_bytes.max(PAGE_HEADER_BYTES + 16),
            records: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), IndexError> {
        if self.records.insert(key, value).is_some() {
            return Err(IndexError::InvalidDefinition(
                "an index generation contains a duplicate key".into(),
            ));
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn finish(self) -> Result<Vec<u8>, IndexError> {
        let mut pages = Vec::<EncodedPage>::new();
        let mut current = Vec::<MapRecord>::new();
        let mut current_bytes = PAGE_HEADER_BYTES;
        for (key, value) in self.records {
            let record_bytes = encoded_record_length(&key, &value)?;
            if !current.is_empty()
                && current_bytes.saturating_add(record_bytes) > self.target_page_bytes
            {
                pages.push(encode_page(std::mem::take(&mut current))?);
                current_bytes = PAGE_HEADER_BYTES;
            }
            current_bytes = current_bytes
                .checked_add(record_bytes)
                .ok_or(IndexError::OffsetOverflow)?;
            current.push((key, value));
        }
        if !current.is_empty() {
            pages.push(encode_page(current)?);
        }

        let directory_len = pages.iter().try_fold(0usize, |total, page| {
            total
                .checked_add(DIRECTORY_ENTRY_FIXED_BYTES)
                .and_then(|value| value.checked_add(page.minimum_key.len()))
                .and_then(|value| value.checked_add(page.maximum_key.len()))
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let first_page_offset = HEADER_BYTES
            .checked_add(directory_len)
            .ok_or(IndexError::OffsetOverflow)?;
        let mut next_offset =
            u64::try_from(first_page_offset).map_err(|_| IndexError::OffsetOverflow)?;
        let mut directory = Vec::with_capacity(directory_len);
        for page in &pages {
            let length = u32::try_from(page.bytes.len()).map_err(|_| IndexError::OffsetOverflow)?;
            directory.extend_from_slice(&next_offset.to_le_bytes());
            directory.extend_from_slice(&length.to_le_bytes());
            directory.extend_from_slice(&page.record_count.to_le_bytes());
            directory.extend_from_slice(
                &u32::try_from(page.minimum_key.len())
                    .map_err(|_| IndexError::OffsetOverflow)?
                    .to_le_bytes(),
            );
            directory.extend_from_slice(
                &u32::try_from(page.maximum_key.len())
                    .map_err(|_| IndexError::OffsetOverflow)?
                    .to_le_bytes(),
            );
            directory.extend_from_slice(&page.minimum_key);
            directory.extend_from_slice(&page.maximum_key);
            next_offset = next_offset
                .checked_add(u64::from(length))
                .ok_or(IndexError::OffsetOverflow)?;
        }

        let record_count = pages.iter().try_fold(0u64, |total, page| {
            total
                .checked_add(u64::from(page.record_count))
                .ok_or(IndexError::OffsetOverflow)
        })?;
        let mut output = Vec::with_capacity(
            usize::try_from(next_offset).map_err(|_| IndexError::OffsetOverflow)?,
        );
        output.extend_from_slice(FILE_MAGIC);
        output.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        output.extend_from_slice(&0u16.to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(pages.len())
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        output.extend_from_slice(&record_count.to_le_bytes());
        output.extend_from_slice(
            &u64::try_from(directory.len())
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        output.extend_from_slice(blake3::hash(&directory).as_bytes());
        debug_assert_eq!(output.len(), HEADER_BYTES);
        output.extend_from_slice(&directory);
        for page in pages {
            output.extend_from_slice(&page.bytes);
        }
        Ok(output)
    }
}

#[derive(Clone, Debug)]
struct EncodedPage {
    bytes: Vec<u8>,
    record_count: u32,
    minimum_key: Vec<u8>,
    maximum_key: Vec<u8>,
}

fn encoded_record_length(key: &[u8], value: &[u8]) -> Result<usize, IndexError> {
    let _ = u32::try_from(key.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let _ = u32::try_from(value.len()).map_err(|_| IndexError::OffsetOverflow)?;
    8usize
        .checked_add(key.len())
        .and_then(|length| length.checked_add(value.len()))
        .ok_or(IndexError::OffsetOverflow)
}

fn encode_page(records: Vec<MapRecord>) -> Result<EncodedPage, IndexError> {
    let minimum_key = records
        .first()
        .ok_or(IndexError::InvalidFormat("empty map page"))?
        .0
        .clone();
    let maximum_key = records.last().unwrap().0.clone();
    let record_count = u32::try_from(records.len()).map_err(|_| IndexError::OffsetOverflow)?;
    let mut body = Vec::new();
    for (key, value) in records {
        body.extend_from_slice(
            &u32::try_from(key.len())
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        body.extend_from_slice(
            &u32::try_from(value.len())
                .map_err(|_| IndexError::OffsetOverflow)?
                .to_le_bytes(),
        );
        body.extend_from_slice(&key);
        body.extend_from_slice(&value);
    }
    let mut bytes = Vec::with_capacity(PAGE_HEADER_BYTES + body.len());
    bytes.extend_from_slice(PAGE_MAGIC);
    bytes.extend_from_slice(&record_count.to_le_bytes());
    bytes.extend_from_slice(
        &u32::try_from(body.len())
            .map_err(|_| IndexError::OffsetOverflow)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(blake3::hash(&body).as_bytes());
    bytes.extend_from_slice(&body);
    Ok(EncodedPage {
        bytes,
        record_count,
        minimum_key,
        maximum_key,
    })
}

#[derive(Debug)]
pub struct PagedMap<F> {
    file: F,
    record_count: u64,
    pages: Vec<PageDirectoryEntry>,
}

impl<F: IndexFileRead> PagedMap<F> {
    pub async fn open(file: F) -> Result<Self, IndexError> {
        let header = read_exact_at(&file, 0, HEADER_BYTES).await?;
        if &header[..8] != FILE_MAGIC {
            return Err(IndexError::InvalidFormat("paged map magic"));
        }
        if u16::from_le_bytes(header[8..10].try_into().unwrap()) != FORMAT_VERSION {
            return Err(IndexError::InvalidFormat("unsupported paged map version"));
        }
        let page_count = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
        let record_count = u64::from_le_bytes(header[16..24].try_into().unwrap());
        let directory_len = usize::try_from(u64::from_le_bytes(header[24..32].try_into().unwrap()))
            .map_err(|_| IndexError::OffsetOverflow)?;
        let directory = read_exact_at(&file, HEADER_BYTES as u64, directory_len).await?;
        if blake3::hash(&directory).as_bytes() != &header[32..64] {
            return Err(IndexError::Integrity);
        }
        let first_page_offset = u64::try_from(HEADER_BYTES)
            .ok()
            .and_then(|header| {
                u64::try_from(directory_len)
                    .ok()
                    .and_then(|length| header.checked_add(length))
            })
            .ok_or(IndexError::OffsetOverflow)?;
        let pages = decode_directory(&directory, page_count, first_page_offset)?;
        if pages.iter().try_fold(0u64, |total, page| {
            total.checked_add(u64::from(page.record_count))
        }) != Some(record_count)
        {
            return Err(IndexError::InvalidFormat("paged map record count"));
        }
        Ok(Self {
            file,
            record_count,
            pages,
        })
    }

    pub fn len(&self) -> u64 {
        self.record_count
    }

    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IndexError> {
        let Some(page_index) = self.pages.iter().position(|page| {
            page.minimum_key.as_slice() <= key && key <= page.maximum_key.as_slice()
        }) else {
            return Ok(None);
        };
        let records = self.page(page_index).await?;
        Ok(records
            .binary_search_by(|record| record.0.as_slice().cmp(key))
            .ok()
            .map(|index| records[index].1.clone()))
    }

    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<MapRecord>, IndexError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut result = Vec::with_capacity(limit.min(128));
        for page_index in 0..self.pages.len() {
            let page = &self.pages[page_index];
            if page.maximum_key.as_slice() < prefix {
                continue;
            }
            if !page.minimum_key.starts_with(prefix)
                && page.minimum_key.as_slice() > prefix
                && !could_share_prefix(prefix, &page.minimum_key)
            {
                break;
            }
            for (key, value) in self.page(page_index).await? {
                if !key.starts_with(prefix) {
                    if key.as_slice() > prefix && !result.is_empty() {
                        return Ok(result);
                    }
                    continue;
                }
                if after.is_some_and(|after| key.as_slice() <= after) {
                    continue;
                }
                result.push((key, value));
                if result.len() == limit {
                    return Ok(result);
                }
            }
        }
        Ok(result)
    }

    pub async fn scan_all(&self) -> Result<Vec<MapRecord>, IndexError> {
        let capacity = usize::try_from(self.record_count)
            .unwrap_or(usize::MAX)
            .min(1_000_000);
        let mut result = Vec::with_capacity(capacity);
        for page_index in 0..self.pages.len() {
            result.extend(self.page(page_index).await?);
        }
        Ok(result)
    }

    pub async fn page(&self, index: usize) -> Result<Vec<MapRecord>, IndexError> {
        let page = self
            .pages
            .get(index)
            .ok_or(IndexError::InvalidFormat("paged map page index"))?;
        let bytes = read_exact_at(&self.file, page.offset, page.length as usize).await?;
        decode_page(&bytes, page)
    }
}

fn could_share_prefix(prefix: &[u8], key: &[u8]) -> bool {
    key.starts_with(prefix) || prefix.starts_with(key)
}

fn decode_directory(
    mut bytes: &[u8],
    expected_count: usize,
    first_page_offset: u64,
) -> Result<Vec<PageDirectoryEntry>, IndexError> {
    let mut pages = Vec::with_capacity(expected_count);
    for _ in 0..expected_count {
        if bytes.len() < DIRECTORY_ENTRY_FIXED_BYTES {
            return Err(IndexError::InvalidFormat("truncated page directory"));
        }
        let offset = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let length = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let record_count = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let minimum_length = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
        let maximum_length = u32::from_le_bytes(bytes[20..24].try_into().unwrap()) as usize;
        let entry_length = DIRECTORY_ENTRY_FIXED_BYTES
            .checked_add(minimum_length)
            .and_then(|length| length.checked_add(maximum_length))
            .ok_or(IndexError::OffsetOverflow)?;
        if bytes.len() < entry_length || record_count == 0 || length < PAGE_HEADER_BYTES_U32 {
            return Err(IndexError::InvalidFormat("invalid page directory entry"));
        }
        let minimum_start = DIRECTORY_ENTRY_FIXED_BYTES;
        let maximum_start = minimum_start + minimum_length;
        let minimum_key = bytes[minimum_start..maximum_start].to_vec();
        let maximum_key = bytes[maximum_start..entry_length].to_vec();
        if minimum_key > maximum_key || offset.checked_add(u64::from(length)).is_none() {
            return Err(IndexError::InvalidFormat("invalid page range"));
        }
        pages.push(PageDirectoryEntry {
            offset,
            length,
            record_count,
            minimum_key,
            maximum_key,
        });
        bytes = &bytes[entry_length..];
    }
    if !bytes.is_empty()
        || pages
            .first()
            .is_some_and(|page| page.offset != first_page_offset)
        || pages.windows(2).any(|pair| {
            pair[0].maximum_key >= pair[1].minimum_key
                || pair[0].offset + u64::from(pair[0].length) != pair[1].offset
        })
    {
        return Err(IndexError::InvalidFormat("non-canonical page directory"));
    }
    Ok(pages)
}

fn decode_page(bytes: &[u8], expected: &PageDirectoryEntry) -> Result<Vec<MapRecord>, IndexError> {
    if bytes.len() < PAGE_HEADER_BYTES || &bytes[..8] != PAGE_MAGIC {
        return Err(IndexError::InvalidFormat("page header"));
    }
    let record_count = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let body_length = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    if record_count != expected.record_count
        || PAGE_HEADER_BYTES.checked_add(body_length) != Some(bytes.len())
    {
        return Err(IndexError::InvalidFormat("page declared length"));
    }
    let body = &bytes[PAGE_HEADER_BYTES..];
    if blake3::hash(body).as_bytes() != &bytes[16..48] {
        return Err(IndexError::Integrity);
    }
    let mut cursor = 0usize;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        if body.len().saturating_sub(cursor) < 8 {
            return Err(IndexError::InvalidFormat("truncated page record"));
        }
        let key_length = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
        let value_length =
            u32::from_le_bytes(body[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
        let key_start = cursor + 8;
        let value_start = key_start
            .checked_add(key_length)
            .ok_or(IndexError::OffsetOverflow)?;
        let end = value_start
            .checked_add(value_length)
            .ok_or(IndexError::OffsetOverflow)?;
        if end > body.len() {
            return Err(IndexError::InvalidFormat("truncated page record bytes"));
        }
        records.push((
            body[key_start..value_start].to_vec(),
            body[value_start..end].to_vec(),
        ));
        cursor = end;
    }
    if cursor != body.len()
        || records.windows(2).any(|pair| pair[0].0 >= pair[1].0)
        || records.first().map(|record| &record.0) != Some(&expected.minimum_key)
        || records.last().map(|record| &record.0) != Some(&expected.maximum_key)
    {
        return Err(IndexError::InvalidFormat("non-canonical page records"));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use crate::io::tests::MemoryFile;

    use super::*;

    #[tokio::test]
    async fn lookup_and_prefix_scan_cross_small_backing_slices() {
        let mut builder = PagedMapBuilder::new(96);
        for key in ["alpha/1", "alpha/2", "alpha/3", "beta/1"] {
            builder
                .insert(key.as_bytes().to_vec(), format!("value-{key}").into_bytes())
                .unwrap();
        }
        let bytes = builder.finish().unwrap();
        let map = PagedMap::open(MemoryFile::segmented(bytes, 7))
            .await
            .unwrap();
        assert_eq!(map.len(), 4);
        assert_eq!(
            map.get(b"alpha/2").await.unwrap().unwrap(),
            b"value-alpha/2"
        );
        assert_eq!(
            map.scan_prefix(b"alpha/", Some(b"alpha/1"), 20)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.0)
                .collect::<Vec<_>>(),
            [b"alpha/2".to_vec(), b"alpha/3".to_vec()]
        );
    }

    #[tokio::test]
    async fn page_corruption_is_rejected_when_read() {
        let mut builder = PagedMapBuilder::default();
        builder.insert(b"key".to_vec(), b"value".to_vec()).unwrap();
        let mut bytes = builder.finish().unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        let map = PagedMap::open(MemoryFile::new(bytes)).await.unwrap();
        assert_eq!(map.get(b"key").await.unwrap_err(), IndexError::Integrity);
    }

    #[test]
    fn duplicate_keys_are_rejected() {
        let mut builder = PagedMapBuilder::default();
        builder.insert(b"key".to_vec(), vec![1]).unwrap();
        assert!(builder.insert(b"key".to_vec(), vec![2]).is_err());
    }
}
