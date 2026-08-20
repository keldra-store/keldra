const MAX_REPLICAS: usize = 3;

/// The complete-record replication policy for one active cluster membership.
///
/// A value cannot be constructed without an active node. Callers must count
/// distinct replicas when deciding whether an acknowledgement threshold has
/// been met.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutableRecordQuorum {
    replica_count: usize,
    required_acknowledgements: usize,
}

impl MutableRecordQuorum {
    pub(crate) fn for_active_nodes(active_nodes: usize) -> Option<Self> {
        let replica_count = active_nodes.min(MAX_REPLICAS);
        let required_acknowledgements = match replica_count {
            0 => return None,
            1 => 1,
            2 => 2,
            3 => 2,
            _ => unreachable!("replica count is capped at three"),
        };

        Some(Self {
            replica_count,
            required_acknowledgements,
        })
    }

    pub(crate) const fn replica_count(self) -> usize {
        self.replica_count
    }

    pub(crate) const fn required_acknowledgements(self) -> usize {
        self.required_acknowledgements
    }

    pub(crate) const fn is_satisfied_by(self, distinct_durable_replicas: usize) -> bool {
        distinct_durable_replicas >= self.required_acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_active_nodes_have_no_quorum_policy() {
        assert_eq!(MutableRecordQuorum::for_active_nodes(0), None);
    }

    #[test]
    fn one_active_node_requires_one_of_one() {
        let policy = MutableRecordQuorum::for_active_nodes(1).unwrap();

        assert_eq!(policy.replica_count(), 1);
        assert_eq!(policy.required_acknowledgements(), 1);
        assert!(!policy.is_satisfied_by(0));
        assert!(policy.is_satisfied_by(1));
    }

    #[test]
    fn two_active_nodes_require_two_of_two() {
        let policy = MutableRecordQuorum::for_active_nodes(2).unwrap();

        assert_eq!(policy.replica_count(), 2);
        assert_eq!(policy.required_acknowledgements(), 2);
        assert!(!policy.is_satisfied_by(1));
        assert!(policy.is_satisfied_by(2));
    }

    #[test]
    fn three_or_more_active_nodes_require_two_of_three() {
        for active_nodes in [3, 4, 17, usize::MAX] {
            let policy = MutableRecordQuorum::for_active_nodes(active_nodes).unwrap();

            assert_eq!(policy.replica_count(), 3);
            assert_eq!(policy.required_acknowledgements(), 2);
            assert!(!policy.is_satisfied_by(1));
            assert!(policy.is_satisfied_by(2));
        }
    }
}
