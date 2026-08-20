//! Bounded deterministic format-v4 segment construction.

mod merge;
mod model;
mod scratch;
mod segment_merge;
mod sink;
mod writer;

pub use merge::{MergeMutation, MergedSources, merge_mutations};
pub use model::{
    BuildLimits, ProjectedDocValue, ProjectedPoint, ProjectedRecord, ProjectedSource,
    ProjectedTerm, ProjectedVector, SourcePush,
};
pub use scratch::{MergeScratchFile, MergeScratchSpace};
pub use segment_merge::{MAXIMUM_SEGMENT_MERGE_INPUTS, merge_segments};
pub use sink::{
    ComponentBatchSink, ComponentLeaf, ComponentPack, DescriptorLeaf, ExactMemorySink,
    PublishedObject, PublishedStream, StreamingComponentPublisher, publish_descriptor_stream,
    publish_stream,
};
pub use writer::{BuiltSegment, NativeSegmentWriter};
