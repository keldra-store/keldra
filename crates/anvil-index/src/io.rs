use std::future::Future;

use crate::IndexError;

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

/// One immutable index generation containing named logical files.
pub trait IndexDirectoryRead: Send + Sync {
    type File: IndexFileRead;

    fn open_file(&self, name: &str) -> impl Future<Output = Result<Self::File, IndexError>> + Send;
}

pub(crate) async fn read_exact_at<F: IndexFileRead>(
    file: &F,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, IndexError> {
    let expected_end = offset
        .checked_add(u64::try_from(length).map_err(|_| IndexError::OffsetOverflow)?)
        .ok_or(IndexError::OffsetOverflow)?;
    let mut bytes = Vec::with_capacity(length);
    let mut cursor = offset;
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
    Ok(bytes)
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

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

        async fn open_file(&self, name: &str) -> Result<Self::File, IndexError> {
            self.files
                .get(name)
                .cloned()
                .ok_or_else(|| IndexError::FileNotFound(name.to_owned()))
        }
    }

    #[tokio::test]
    async fn exact_read_crosses_backing_segment_boundaries() {
        let file = MemoryFile::segmented(b"abcdefghij".to_vec(), 3);
        assert_eq!(read_exact_at(&file, 2, 7).await.unwrap(), b"cdefghi");
    }
}
