use crate::IndexError;

use super::{
    ArtifactDescriptor, ArtifactDirectoryRead, ComponentKind, RoutingNode, SegmentIdentity,
    read_artifact_component,
};

/// One routed data leaf with the exact immutable routing evidence required to
/// reuse its descriptor in a replacement stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamLeaf {
    pub minimum_key: Vec<u8>,
    pub maximum_key: Vec<u8>,
    pub element_count: u64,
    pub descriptor: ArtifactDescriptor,
}

/// Exact recursively visited bytes for one component stream traversal.
///
/// A complete, unbounded traversal accounts for the root, every routing node,
/// and every data leaf exactly once. A bounded or unfinished traversal reports
/// only the descriptors it actually visited.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamTotals {
    pub encoded_bytes: u64,
    pub logical_bytes: u64,
    pub component_count: u64,
}

/// Bounded depth-first traversal of one logical component stream.
///
/// Routing descriptors are expanded lazily. Only the at-most-32 children of
/// each visited routing node are resident, and key bounds prune entire child
/// ranges before a data component is opened.
pub struct ComponentStream<'a, D> {
    directory: &'a D,
    identity: SegmentIdentity,
    logical_kind: ComponentKind,
    minimum_key: Option<Vec<u8>>,
    maximum_key: Option<Vec<u8>>,
    pending: Vec<PendingArtifact>,
    traversed: StreamTotals,
}

struct PendingArtifact {
    descriptor: ArtifactDescriptor,
    /// Zero requires a data leaf. A positive value requires a routing node of
    /// exactly this height. `None` is valid only for the root.
    expected_height: Option<u8>,
    routing_evidence: Option<RoutingEvidence>,
}

struct RoutingEvidence {
    minimum_key: Vec<u8>,
    maximum_key: Vec<u8>,
    element_count: u64,
}

impl<'a, D: ArtifactDirectoryRead> ComponentStream<'a, D> {
    pub fn new(
        directory: &'a D,
        identity: SegmentIdentity,
        logical_kind: ComponentKind,
        root: ArtifactDescriptor,
        minimum_key: Option<Vec<u8>>,
        maximum_key: Option<Vec<u8>>,
    ) -> Result<Self, IndexError> {
        identity.validate()?;
        root.validate(identity.index_id)?;
        if root.component_kind != ComponentKind::ROUTING_NODE {
            return Err(IndexError::InvalidFormat(
                "format-v4 component stream requires a routing root",
            ));
        }
        if minimum_key
            .as_ref()
            .zip(maximum_key.as_ref())
            .is_some_and(|(minimum, maximum)| minimum > maximum)
        {
            return Err(IndexError::InvalidQuery(
                "component stream key range is reversed".into(),
            ));
        }
        Ok(Self {
            directory,
            identity,
            logical_kind,
            minimum_key,
            maximum_key,
            pending: vec![PendingArtifact {
                descriptor: root,
                expected_height: None,
                routing_evidence: None,
            }],
            traversed: StreamTotals::default(),
        })
    }

    pub fn traversed_totals(&self) -> StreamTotals {
        self.traversed
    }

    pub async fn next_leaf(&mut self) -> Result<Option<StreamLeaf>, IndexError> {
        while let Some(pending) = self.pending.pop() {
            self.traversed.encoded_bytes = self
                .traversed
                .encoded_bytes
                .checked_add(pending.descriptor.encoded_length)
                .ok_or(IndexError::OffsetOverflow)?;
            self.traversed.logical_bytes = self
                .traversed
                .logical_bytes
                .checked_add(pending.descriptor.logical_length)
                .ok_or(IndexError::OffsetOverflow)?;
            self.traversed.component_count = self
                .traversed
                .component_count
                .checked_add(1)
                .ok_or(IndexError::OffsetOverflow)?;
            if pending.descriptor.component_kind != ComponentKind::ROUTING_NODE {
                if pending.expected_height.is_some_and(|height| height != 0)
                    || pending.descriptor.component_kind != self.logical_kind
                {
                    return Err(IndexError::InvalidFormat(
                        "format-v4 routed leaf kind or height",
                    ));
                }
                let evidence = pending.routing_evidence.ok_or(IndexError::InvalidFormat(
                    "format-v4 data leaf has no routing evidence",
                ))?;
                return Ok(Some(StreamLeaf {
                    minimum_key: evidence.minimum_key,
                    maximum_key: evidence.maximum_key,
                    element_count: evidence.element_count,
                    descriptor: pending.descriptor,
                }));
            }

            if pending.expected_height == Some(0) {
                return Err(IndexError::InvalidFormat(
                    "format-v4 routing tree exceeds its declared height",
                ));
            }
            let component = read_artifact_component(
                self.directory,
                self.identity,
                &pending.descriptor,
                ComponentKind::ROUTING_NODE,
            )
            .await?;
            let index_id = self.identity.index_id;
            let routing = self
                .directory
                .run_query_cpu(move || RoutingNode::decode_payload(index_id, &component.payload))
                .await?;
            if pending
                .expected_height
                .is_some_and(|height| height != routing.height)
                || routing.logical_kind() != self.logical_kind
            {
                return Err(IndexError::InvalidFormat(
                    "format-v4 routing child height or logical kind",
                ));
            }
            let child_height = routing.height - 1;
            for entry in routing.entries().iter().rev() {
                if self
                    .minimum_key
                    .as_ref()
                    .is_some_and(|minimum| entry.maximum_key < *minimum)
                    || self
                        .maximum_key
                        .as_ref()
                        .is_some_and(|maximum| entry.minimum_key > *maximum)
                {
                    continue;
                }
                self.pending.push(PendingArtifact {
                    descriptor: entry.child.clone(),
                    expected_height: Some(child_height),
                    routing_evidence: Some(RoutingEvidence {
                        minimum_key: entry.minimum_key.clone(),
                        maximum_key: entry.maximum_key.clone(),
                        element_count: entry.element_count,
                    }),
                });
            }
        }
        Ok(None)
    }
}
