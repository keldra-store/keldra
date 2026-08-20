//! Bounded page-stream merge shared by typed ADD handoff protocols.

use std::collections::VecDeque;

use keldra_consensus::NodeId;
use tonic::Status;

use super::HandoffEndpoint;

struct Keyed<T> {
    key: Vec<u8>,
    value: T,
}

/// One peer's bounded, sorted export page.
///
/// At most one page is resident per peer. Callers fetch the next page only
/// after consuming this buffer, so memory is independent of cluster data size.
pub(super) struct MergeSource<T, C> {
    endpoint: HandoffEndpoint,
    cursor: Option<C>,
    buffered: VecDeque<Keyed<T>>,
    exhausted: bool,
    last_key: Option<Vec<u8>>,
}

impl<T, C> MergeSource<T, C> {
    pub(super) fn new(endpoint: HandoffEndpoint) -> Self {
        Self {
            endpoint,
            cursor: None,
            buffered: VecDeque::new(),
            exhausted: false,
            last_key: None,
        }
    }

    pub(super) fn node_id(&self) -> NodeId {
        self.endpoint.node_id
    }

    pub(super) fn address(&self) -> &str {
        &self.endpoint.address
    }

    pub(super) fn cursor(&self) -> Option<&C> {
        self.cursor.as_ref()
    }

    pub(super) fn needs_page(&self) -> bool {
        self.buffered.is_empty() && !self.exhausted
    }

    pub(super) fn install_page<F>(
        &mut self,
        records: Vec<T>,
        next_cursor: Option<C>,
        mut order_key: F,
    ) -> Result<(), Status>
    where
        F: FnMut(&T) -> Result<Vec<u8>, Status>,
    {
        if !self.needs_page() {
            return Err(Status::internal(
                "handoff tried to replace an unconsumed export page",
            ));
        }
        if records.is_empty() && next_cursor.is_some() {
            return Err(Status::data_loss(
                "handoff peer returned an empty page with a continuation cursor",
            ));
        }
        for value in records {
            let key = order_key(&value)?;
            if self
                .last_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(Status::data_loss(
                    "handoff peer export is not in strict canonical order",
                ));
            }
            self.last_key = Some(key.clone());
            self.buffered.push_back(Keyed { key, value });
        }
        self.exhausted = next_cursor.is_none();
        self.cursor = next_cursor;
        Ok(())
    }

    pub(super) fn front_key(&self) -> Option<&[u8]> {
        self.buffered.front().map(|entry| entry.key.as_slice())
    }

    pub(super) fn take_if(&mut self, key: &[u8]) -> Option<T> {
        if self.front_key() != Some(key) {
            return None;
        }
        self.buffered.pop_front().map(|entry| entry.value)
    }
}

pub(super) fn next_key<T, C>(sources: &[MergeSource<T, C>]) -> Option<Vec<u8>> {
    sources
        .iter()
        .filter_map(MergeSource::front_key)
        .min()
        .map(<[u8]>::to_vec)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> MergeSource<u8, u8> {
        MergeSource::new(HandoffEndpoint {
            node_id: NodeId(1),
            address: "node-1".into(),
        })
    }

    #[test]
    fn source_rejects_non_increasing_pages() {
        let mut source = source();
        assert!(
            source
                .install_page(vec![1, 2], Some(1), |value| Ok(vec![*value]))
                .is_ok()
        );
        assert_eq!(source.take_if(&[1]), Some(1));
        assert_eq!(source.take_if(&[2]), Some(2));
        assert!(
            source
                .install_page(vec![2], None, |value| Ok(vec![*value]))
                .is_err()
        );
    }

    #[test]
    fn merge_selects_one_identity_across_sources() {
        let mut first = source();
        let mut second = source();
        first
            .install_page(vec![1, 3], None, |value| Ok(vec![*value]))
            .unwrap();
        second
            .install_page(vec![1, 2], None, |value| Ok(vec![*value]))
            .unwrap();
        let mut sources = vec![first, second];
        assert_eq!(next_key(&sources), Some(vec![1]));
        assert_eq!(sources[0].take_if(&[1]), Some(1));
        assert_eq!(sources[1].take_if(&[1]), Some(1));
        assert_eq!(next_key(&sources), Some(vec![2]));
    }
}
