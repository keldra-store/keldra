use tokio::sync::{Mutex as AsyncMutex, Notify};

use super::*;

fn bounds() -> PublicationCohortBounds {
    PublicationCohortBounds::new(128, 8, 8, Duration::from_millis(5), 1, 1)
}

#[tokio::test]
async fn delayed_admission_opens_one_fresh_bounded_collection_window() {
    let admission = Arc::new(Semaphore::new(0));
    let admission_waiting = Arc::new(Notify::new());
    let admission_granted = Arc::new(Notify::new());
    let batches = Arc::new(AsyncMutex::new(Vec::new()));
    let scheduler = PublicationCohortScheduler::start_with_admission(
        bounds(),
        {
            let admission = admission.clone();
            let admission_waiting = admission_waiting.clone();
            let admission_granted = admission_granted.clone();
            move |_class| {
                let admission = admission.clone();
                let admission_waiting = admission_waiting.clone();
                let admission_granted = admission_granted.clone();
                async move {
                    admission_waiting.notify_one();
                    let permit = admission.acquire_owned().await.map_err(|_| ())?;
                    admission_granted.notify_one();
                    Ok(permit)
                }
            }
        },
        {
            let batches = batches.clone();
            move |_class, _cohort: u8, payloads: Vec<u64>| {
                let batches = batches.clone();
                async move {
                    batches.lock().await.push(payloads.clone());
                    payloads.into_iter().map(Ok::<_, ()>).collect()
                }
            }
        },
    );

    let waiting = admission_waiting.notified();
    let first = tokio::spawn({
        let scheduler = scheduler.clone();
        async move {
            scheduler
                .submit(PublicationCohortClass::Incremental, 1, 7, 1, 1, 1, 1)
                .await
        }
    });
    waiting.await;
    tokio::time::sleep(bounds().max_collection_delay + Duration::from_millis(5)).await;

    let granted = admission_granted.notified();
    admission.add_permits(1);
    granted.await;
    let second = scheduler
        .submit(PublicationCohortClass::Incremental, 2, 7, 2, 1, 1, 1)
        .await;

    assert_eq!(first.await.unwrap(), Ok(1));
    assert_eq!(second, Ok(2));
    assert_eq!(*batches.lock().await, vec![vec![1, 2]]);
}
