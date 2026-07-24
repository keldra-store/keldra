use crate::{
    core_store::{
        CoreByteRange, CoreObjectRef, CoreStore, GetBlobRange, decode_core_object_ref_target,
    },
    formats::{
        FileFamily, FormatError, WRITER_SEGMENT_FIXED_HEADER_LEN, WriterSegmentFixedHeader,
        WriterSegmentHeaderProto, decode_writer_segment_header,
        table::{
            TablePageRange, WriterBodyTableDirectory, WriterBodyTableDirectoryEntry,
            decode_table_page_rows, decode_writer_body_table_directory,
            decode_writer_body_table_page_ranges,
        },
    },
    index_coremeta,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const FULL_TEXT_SEGMENT_REF_PREFIX: &str = "full_text_segment:";
const TYPED_FIELD_SEGMENT_REF_PREFIX: &str = "typed_field_segment:";
const VECTOR_SEGMENT_REF_PREFIX: &str = "vector_segment:";

#[derive(Debug, Clone)]
pub struct RangeAddressedWriterSegment {
    store: CoreStore,
    pub object_ref: CoreObjectRef,
    pub envelope: WriterSegmentFixedHeader,
    pub header: WriterSegmentHeaderProto,
    pub body_offset: u64,
    pub range_index_offset: u64,
    pub trailer_offset: u64,
}

impl RangeAddressedWriterSegment {
    pub async fn open(storage: &Storage, segment_ref: &str, family: FileFamily) -> Result<Self> {
        let store = CoreStore::new(storage.clone()).await?;
        let index_id = index_id_from_segment_ref(segment_ref)?;
        let segment = index_coremeta::read_index_segment_coremeta_record_by_ref(
            storage,
            &index_id,
            segment_ref,
        )
        .await?
        .ok_or_else(|| anyhow!("writer segment CoreMeta row {segment_ref} is missing"))?;
        let object_ref = decode_core_object_ref_target(&segment.core_object_ref_target)?;
        Self::open_object_ref(store, object_ref, family).await
    }

    pub async fn open_object_ref(
        store: CoreStore,
        object_ref: CoreObjectRef,
        family: FileFamily,
    ) -> Result<Self> {
        let fixed = read_range_exact(
            &store,
            &object_ref,
            0,
            WRITER_SEGMENT_FIXED_HEADER_LEN as u64,
        )
        .await?;
        let header_len = writer_segment_header_len_from_fixed(&fixed)?;
        let header_end = (WRITER_SEGMENT_FIXED_HEADER_LEN as u64)
            .checked_add(header_len)
            .ok_or_else(|| anyhow!("writer segment header length overflow"))?;
        let envelope_bytes = read_range_exact(&store, &object_ref, 0, header_end).await?;
        let envelope = WriterSegmentFixedHeader::decode(&envelope_bytes)?;
        if envelope.family != family {
            bail!("writer segment family does not match requested range reader");
        }
        let header = decode_writer_segment_header(&envelope.header_proto)?;
        if header.writer_family != family.writer_family_name() {
            bail!("writer segment header family does not match requested range reader");
        }
        let body_offset = envelope.encoded_len() as u64;
        let range_index_offset = body_offset
            .checked_add(envelope.body_len)
            .ok_or_else(|| anyhow!("writer segment range index offset overflow"))?;
        let trailer_offset = range_index_offset
            .checked_add(envelope.range_index_len)
            .ok_or_else(|| anyhow!("writer segment trailer offset overflow"))?;
        let expected_len = trailer_offset
            .checked_add(u64::from(envelope.trailer_len))
            .and_then(|value| value.checked_add(32))
            .ok_or_else(|| anyhow!("writer segment length overflow"))?;
        if expected_len != object_ref.logical_size {
            bail!("writer segment object length does not match envelope");
        }
        Ok(Self {
            store,
            object_ref,
            envelope,
            header,
            body_offset,
            range_index_offset,
            trailer_offset,
        })
    }

    pub async fn read_segment_range(&self, start: u64, end_exclusive: u64) -> Result<Vec<u8>> {
        read_range_exact(&self.store, &self.object_ref, start, end_exclusive).await
    }

    pub async fn read_body_range(&self, start: u64, end_exclusive: u64) -> Result<Vec<u8>> {
        if start > end_exclusive || end_exclusive > self.envelope.body_len {
            bail!("writer segment body range is outside segment body");
        }
        self.read_segment_range(self.body_offset + start, self.body_offset + end_exclusive)
            .await
    }

    pub async fn read_body_table_directory(&self) -> Result<WriterBodyTableDirectory> {
        let directory_len = self.read_exact_body_directory_len().await?;
        let directory_bytes = self.read_body_range(0, directory_len).await?;
        Ok(decode_writer_body_table_directory(&directory_bytes)?.0)
    }

    pub async fn read_table_bytes(&self, entry: &WriterBodyTableDirectoryEntry) -> Result<Vec<u8>> {
        self.read_body_range(entry.offset, entry.offset + entry.length)
            .await
    }

    pub async fn read_table_pages_matching_key_prefix(
        &self,
        entry: &WriterBodyTableDirectoryEntry,
        key_prefix: &[u8],
    ) -> Result<Vec<crate::formats::table::TableRow>> {
        let page_directory_len = self.read_exact_table_page_directory_len(entry).await?;
        let page_directory = self
            .read_body_range(entry.offset, entry.offset + page_directory_len)
            .await?;
        let page_ranges = decode_writer_body_table_page_ranges(entry, &page_directory)?;
        let mut rows = Vec::new();
        for page in page_ranges
            .into_iter()
            .filter(|page| page_range_can_contain_prefix(page, key_prefix))
        {
            let bytes = self
                .read_body_range(
                    entry.offset + page.page_offset,
                    entry.offset + page.page_offset + page.page_length,
                )
                .await?;
            rows.extend(decode_table_page_rows(entry, &page, &bytes)?);
        }
        Ok(rows)
    }

    pub fn table_entry<'a>(
        directory: &'a WriterBodyTableDirectory,
        table_id: u16,
    ) -> Result<&'a WriterBodyTableDirectoryEntry> {
        directory
            .entries
            .iter()
            .find(|entry| entry.table_id == table_id)
            .ok_or_else(|| anyhow!("writer body table {table_id:#06x} missing"))
    }

    async fn read_exact_body_directory_len(&self) -> Result<u64> {
        let mut bytes = self.read_body_range(0, 12).await?;
        if &bytes[0..8] != crate::formats::table::WRITER_BODY_TABLE_DIRECTORY_MAGIC {
            bail!("writer body table directory magic mismatch");
        }
        let table_count = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
        let mut offset = 12usize;
        for _ in 0..table_count {
            ensure_buffered_body_prefix(self, &mut bytes, offset + 26).await?;
            let min_len =
                u16::from_le_bytes(bytes[offset + 24..offset + 26].try_into().unwrap()) as usize;
            ensure_buffered_body_prefix(self, &mut bytes, offset + 26 + min_len + 2).await?;
            let max_len_offset = offset + 26 + min_len;
            let max_len = u16::from_le_bytes(
                bytes[max_len_offset..max_len_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            offset = max_len_offset
                .checked_add(2 + max_len + 32)
                .ok_or_else(|| anyhow!("writer body table directory length overflow"))?;
            ensure_buffered_body_prefix(self, &mut bytes, offset).await?;
        }
        let directory_len = offset
            .checked_add(4)
            .ok_or_else(|| anyhow!("writer body table directory length overflow"))?;
        ensure_buffered_body_prefix(self, &mut bytes, directory_len).await?;
        Ok(directory_len as u64)
    }

    async fn read_exact_table_page_directory_len(
        &self,
        entry: &WriterBodyTableDirectoryEntry,
    ) -> Result<u64> {
        let mut bytes = self.read_body_range(entry.offset, entry.offset + 4).await?;
        let page_count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut offset = 4usize;
        for _ in 0..page_count {
            ensure_buffered_table_prefix(self, entry, &mut bytes, offset + 2).await?;
            let min_len =
                u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap()) as usize;
            ensure_buffered_table_prefix(self, entry, &mut bytes, offset + 2 + min_len + 2).await?;
            let max_len_offset = offset + 2 + min_len;
            let max_len = u16::from_le_bytes(
                bytes[max_len_offset..max_len_offset + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            offset = max_len_offset
                .checked_add(2 + max_len + 8 + 8 + 4 + 32)
                .ok_or_else(|| anyhow!("writer table page directory length overflow"))?;
            ensure_buffered_table_prefix(self, entry, &mut bytes, offset).await?;
        }
        Ok(offset as u64)
    }
}

fn writer_segment_header_len_from_fixed(input: &[u8]) -> Result<u64> {
    if input.len() < WRITER_SEGMENT_FIXED_HEADER_LEN {
        return Err(FormatError::TooShort {
            context: "writer segment fixed header",
            needed: WRITER_SEGMENT_FIXED_HEADER_LEN,
            actual: input.len(),
        }
        .into());
    }
    Ok(u32::from_le_bytes(input[12..16].try_into().unwrap()) as u64)
}

fn page_range_can_contain_prefix(page: &TablePageRange, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    if page.max_key.as_slice() < prefix {
        return false;
    }
    let mut upper = prefix.to_vec();
    increment_prefix_upper_bound(&mut upper).is_none_or(|_| page.min_key < upper)
}

fn increment_prefix_upper_bound(prefix: &mut [u8]) -> Option<()> {
    for byte in prefix.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return Some(());
        }
    }
    None
}

async fn ensure_buffered_body_prefix(
    segment: &RangeAddressedWriterSegment,
    bytes: &mut Vec<u8>,
    needed: usize,
) -> Result<()> {
    if bytes.len() >= needed {
        return Ok(());
    }
    let needed = needed as u64;
    if needed > segment.envelope.body_len {
        bail!("writer body table directory exceeds segment body");
    }
    *bytes = segment.read_body_range(0, needed).await?;
    Ok(())
}

async fn ensure_buffered_table_prefix(
    segment: &RangeAddressedWriterSegment,
    entry: &WriterBodyTableDirectoryEntry,
    bytes: &mut Vec<u8>,
    needed: usize,
) -> Result<()> {
    if bytes.len() >= needed {
        return Ok(());
    }
    let needed = needed as u64;
    if needed > entry.length {
        bail!("writer table page directory exceeds table range");
    }
    *bytes = segment
        .read_body_range(entry.offset, entry.offset + needed)
        .await?;
    Ok(())
}

fn index_id_from_segment_ref(segment_ref: &str) -> Result<String> {
    if let Some(rest) = segment_ref.strip_prefix(TYPED_FIELD_SEGMENT_REF_PREFIX) {
        let encoded = rest
            .split(':')
            .next()
            .ok_or_else(|| anyhow!("typed field segment ref is missing index component"))?;
        let bytes = URL_SAFE_NO_PAD.decode(encoded)?;
        return String::from_utf8(bytes)
            .map_err(|_| anyhow!("typed field segment ref index id is not UTF-8"));
    }
    for (prefix, label) in [
        (FULL_TEXT_SEGMENT_REF_PREFIX, "full text"),
        (VECTOR_SEGMENT_REF_PREFIX, "vector"),
    ] {
        if let Some(rest) = segment_ref.strip_prefix(prefix) {
            let rest = rest
                .strip_prefix("index:")
                .ok_or_else(|| anyhow!("{label} segment ref is missing index component"))?;
            let (index_id, _) = rest
                .split_once(":generation:")
                .ok_or_else(|| anyhow!("{label} segment ref is missing generation component"))?;
            if index_id.is_empty()
                || index_id == "."
                || index_id == ".."
                || index_id.contains('/')
                || index_id.contains('\\')
                || index_id.contains(':')
                || index_id.chars().any(char::is_control)
            {
                bail!("{label} segment ref index id is not a safe component");
            }
            return Ok(index_id.to_string());
        }
    }
    bail!("writer segment ref has unknown family prefix")
}

async fn read_range_exact(
    store: &CoreStore,
    object_ref: &CoreObjectRef,
    start: u64,
    end_exclusive: u64,
) -> Result<Vec<u8>> {
    if start > end_exclusive {
        bail!("CoreStore writer segment range start exceeds end");
    }
    if end_exclusive > object_ref.logical_size {
        bail!("CoreStore writer segment range exceeds object length");
    }
    store
        .get_blob_range(GetBlobRange {
            object_ref: object_ref.clone(),
            range: CoreByteRange {
                start,
                end_exclusive,
            },
        })
        .await
        .context("read CoreStore writer segment range")
}
