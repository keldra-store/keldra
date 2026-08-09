use std::future::Future;

use crate::{BlockDescriptor, GeneratedBlock, IndexError};

/// An immutable logical index file supplied by Anvil's cache manager.
///
/// A returned slice owns or pins its backing bytes. Reads may return fewer
/// bytes than requested at a cache-segment boundary and return an empty slice
/// only at EOF.
pub trait IndexFileRead: Send + Sync {
    type Slice: AsRef<[u8]> + Send + Sync + 'static;

    fn read_at(
        &self,
        offset: u64,
        max_length: usize,
    ) -> impl Future<Output = Result<Self::Slice, IndexError>> + Send;
}

/// One immutable run opened through its root and self-describing block
/// descriptors. Passing the descriptor preserves the content hash and length
/// needed by a remote or content-addressed reader without a side map.
pub trait IndexDirectoryRead: Send + Sync {
    type File: IndexFileRead;

    fn open_root(&self) -> impl Future<Output = Result<Self::File, IndexError>> + Send;

    fn open_block(
        &self,
        descriptor: &BlockDescriptor,
    ) -> impl Future<Output = Result<Self::File, IndexError>> + Send;
}

/// Storage-neutral publication boundary. Anvil durably writes and releases
/// each move-only block before this future resolves.
pub trait IndexBlockSink: Send {
    fn emit(
        &mut self,
        block: GeneratedBlock,
    ) -> impl Future<Output = Result<(), IndexError>> + Send;
}

pub(crate) enum ReadBuffer<S> {
    Pinned(S),
    Owned(Vec<u8>),
}

impl<S: AsRef<[u8]>> AsRef<[u8]> for ReadBuffer<S> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Pinned(slice) => slice.as_ref(),
            Self::Owned(bytes) => bytes,
        }
    }
}

impl<S: AsRef<[u8]>> ReadBuffer<S> {
    pub(crate) fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Pinned(slice) => slice.as_ref().to_vec(),
            Self::Owned(bytes) => bytes,
        }
    }
}

/// Reads an exact logical range. A cache slice covering the complete range is
/// returned directly; allocation and copying occur only when a read crosses
/// backing-segment boundaries.
pub(crate) async fn read_exact_at<F: IndexFileRead>(
    file: &F,
    offset: u64,
    length: usize,
) -> Result<ReadBuffer<F::Slice>, IndexError> {
    let expected_end = offset
        .checked_add(u64::try_from(length).map_err(|_| IndexError::OffsetOverflow)?)
        .ok_or(IndexError::OffsetOverflow)?;
    let first = file.read_at(offset, length).await?;
    let first_bytes = first.as_ref();
    if first_bytes.is_empty() && length != 0 {
        return Err(IndexError::UnexpectedEof {
            expected: expected_end,
            actual: offset,
        });
    }
    if first_bytes.len() > length {
        return Err(IndexError::InvalidFormat(
            "invalid file reader slice length",
        ));
    }
    if first_bytes.len() == length {
        return Ok(ReadBuffer::Pinned(first));
    }
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(first_bytes);
    let mut cursor = offset
        .checked_add(u64::try_from(first_bytes.len()).map_err(|_| IndexError::OffsetOverflow)?)
        .ok_or(IndexError::OffsetOverflow)?;
    while bytes.len() < length {
        let remaining = length - bytes.len();
        let slice = file.read_at(cursor, remaining).await?;
        let part = slice.as_ref();
        if part.is_empty() {
            return Err(IndexError::UnexpectedEof {
                expected: expected_end,
                actual: cursor,
            });
        }
        if part.len() > remaining {
            return Err(IndexError::InvalidFormat(
                "invalid file reader slice length",
            ));
        }
        bytes.extend_from_slice(part);
        cursor = cursor
            .checked_add(u64::try_from(part.len()).map_err(|_| IndexError::OffsetOverflow)?)
            .ok_or(IndexError::OffsetOverflow)?;
    }
    Ok(ReadBuffer::Owned(bytes))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Debug)]
    pub struct MemoryFile {
        bytes: Arc<[u8]>,
        maximum_slice: usize,
    }

    impl MemoryFile {
        pub fn new(bytes: Vec<u8>) -> Self {
            Self {
                bytes: bytes.into(),
                maximum_slice: usize::MAX,
            }
        }

        pub fn segmented(bytes: Vec<u8>, maximum_slice: usize) -> Self {
            Self {
                bytes: bytes.into(),
                maximum_slice,
            }
        }
    }

    impl IndexFileRead for MemoryFile {
        type Slice = Arc<[u8]>;

        async fn read_at(&self, offset: u64, max_length: usize) -> Result<Self::Slice, IndexError> {
            if max_length == 0 {
                return Ok(Arc::from([]));
            }
            let start = usize::try_from(offset).map_err(|_| IndexError::OffsetOverflow)?;
            if start >= self.bytes.len() {
                return Ok(Arc::from([]));
            }
            let length = max_length
                .min(self.maximum_slice)
                .min(self.bytes.len() - start);
            Ok(Arc::from(&self.bytes[start..start + length]))
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct MemoryDirectory {
        files: Arc<BTreeMap<String, MemoryFile>>,
    }

    impl MemoryDirectory {
        pub fn new(files: impl IntoIterator<Item = (String, Vec<u8>)>) -> Self {
            Self {
                files: Arc::new(
                    files
                        .into_iter()
                        .map(|(name, bytes)| (name, MemoryFile::new(bytes)))
                        .collect(),
                ),
            }
        }
    }

    impl IndexDirectoryRead for MemoryDirectory {
        type File = MemoryFile;

        async fn open_root(&self) -> Result<Self::File, IndexError> {
            self.files
                .get(crate::run::RUN_ROOT_FILE)
                .cloned()
                .ok_or_else(|| IndexError::FileNotFound(crate::run::RUN_ROOT_FILE.to_owned()))
        }

        async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
            let name = descriptor.logical_name();
            self.files
                .get(&name)
                .cloned()
                .ok_or(IndexError::FileNotFound(name))
        }
    }

    #[derive(Clone, Debug, Default)]
    pub struct MemoryBlockSink {
        files: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
    }

    impl MemoryBlockSink {
        pub fn directory_with_root(&self, root: GeneratedBlock) -> MemoryDirectory {
            let mut files = self.files.lock().unwrap().clone();
            let name = root.logical_name();
            let (_, bytes) = root.into_parts();
            files.insert(name, bytes.clone());
            files.insert(crate::run::RUN_ROOT_FILE.into(), bytes);
            MemoryDirectory::new(files)
        }

        pub fn directory(&self) -> MemoryDirectory {
            MemoryDirectory::new(self.files.lock().unwrap().clone())
        }

        pub fn len(&self) -> usize {
            self.files.lock().unwrap().len()
        }
    }

    impl IndexBlockSink for MemoryBlockSink {
        async fn emit(&mut self, block: GeneratedBlock) -> Result<(), IndexError> {
            let name = block.logical_name();
            let (_, bytes) = block.into_parts();
            let mut files = self.files.lock().unwrap();
            if let Some(existing) = files.get(&name) {
                if existing == &bytes {
                    return Ok(());
                }
                return Err(IndexError::Integrity);
            }
            files.insert(name, bytes);
            Ok(())
        }
    }

    impl IndexDirectoryRead for MemoryBlockSink {
        type File = MemoryFile;

        async fn open_root(&self) -> Result<Self::File, IndexError> {
            self.files
                .lock()
                .unwrap()
                .get(crate::run::RUN_ROOT_FILE)
                .cloned()
                .map(MemoryFile::new)
                .ok_or_else(|| IndexError::FileNotFound(crate::run::RUN_ROOT_FILE.to_owned()))
        }

        async fn open_block(&self, descriptor: &BlockDescriptor) -> Result<Self::File, IndexError> {
            let name = descriptor.logical_name();
            self.files
                .lock()
                .unwrap()
                .get(&name)
                .cloned()
                .map(MemoryFile::new)
                .ok_or(IndexError::FileNotFound(name))
        }
    }

    #[tokio::test]
    async fn exact_read_crosses_backing_segment_boundaries() {
        let file = MemoryFile::segmented(b"abcdefghij".to_vec(), 3);
        assert_eq!(
            read_exact_at(&file, 2, 7).await.unwrap().as_ref(),
            b"cdefghi"
        );
    }

    #[tokio::test]
    async fn exact_read_keeps_one_complete_backing_slice_pinned() {
        let file = MemoryFile::new(b"abcdefghij".to_vec());
        assert!(matches!(
            read_exact_at(&file, 2, 7).await.unwrap(),
            ReadBuffer::Pinned(_)
        ));
    }
}
