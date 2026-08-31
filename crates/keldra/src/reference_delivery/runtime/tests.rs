use super::*;

fn status(node_id: u16, tail: u64, retention_floor: u64) -> WatchJournalStatus {
    WatchJournalStatus {
        source_id: SourceId {
            node_id,
            source_epoch: [7; 32],
        },
        tail,
        settled_through: tail,
        retention_floor,
        retained_entries: tail - retention_floor,
        retained_bytes: 0,
    }
}

#[test]
fn gc_requires_the_exact_complete_source_tail() {
    let current = status(3, 11, 4);

    assert!(source_is_fully_applied(NodeId(3), current, 11));
    assert!(!source_is_fully_applied(NodeId(3), current, 10));
    assert!(!source_is_fully_applied(NodeId(3), current, 12));
}

#[test]
fn gc_rejects_another_or_malformed_source() {
    let current = status(3, 11, 4);
    assert!(!source_is_fully_applied(NodeId(2), current, 11));

    let mut zero_epoch = current;
    zero_epoch.source_id.source_epoch = [0; 32];
    assert!(!source_is_fully_applied(NodeId(3), zero_epoch, 11));

    let mut invalid_floor = current;
    invalid_floor.retention_floor = 12;
    assert!(!source_is_fully_applied(NodeId(3), invalid_floor, 11));

    let mut invalid_count = current;
    invalid_count.retained_entries += 1;
    assert!(!source_is_fully_applied(NodeId(3), invalid_count, 11));
}

#[test]
fn replicated_acknowledgement_requires_every_selected_cursor() {
    assert!(reference_cursors_reached(&[9, 10, 12], 9));
    assert!(!reference_cursors_reached(&[9, 8, 12], 9));
    assert!(!reference_cursors_reached(&[], 9));
}

#[test]
fn one_node_reference_delivery_uses_its_only_current_destination() {
    assert_eq!(reference_delivery_durability(1), Durability::Local);
    assert_eq!(reference_delivery_durability(2), Durability::Replicated);
    assert_eq!(reference_delivery_durability(3), Durability::Replicated);
}

#[tokio::test]
async fn stop_interrupts_a_long_worker_delay() {
    let (stop, mut signal) = tokio::sync::watch::channel(false);
    let waiter =
        tokio::spawn(async move { wait_for_stop(&mut signal, Duration::from_secs(60)).await });

    stop.send(true).unwrap();
    assert!(waiter.await.unwrap());
}
