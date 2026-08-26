use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::Barrier;

use super::{key, store};

#[tokio::test]
async fn reversed_path_sets_are_canonical_and_mutually_exclusive() {
    let (_temporary, store) = store().await;
    let first = key("definitions/a");
    let second = key("definitions/b");
    let barrier = Arc::new(Barrier::new(3));
    let inside = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();

    for keys in [
        vec![first.clone(), second.clone(), first.clone()],
        vec![second, first],
    ] {
        let store = store.clone();
        let barrier = barrier.clone();
        let inside = inside.clone();
        let maximum = maximum.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .with_ordinary_object_path_locks(&keys, || async {
                    let active = inside.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(active, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    inside.fetch_sub(1, Ordering::SeqCst);
                })
                .await;
        }));
    }

    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(2), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("canonical path-set acquisition must not deadlock");
    assert_eq!(maximum.load(Ordering::SeqCst), 1);
}
