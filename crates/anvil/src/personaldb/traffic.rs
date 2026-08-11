//! Public PersonalDB payload accounting at the protocol boundary.

use tokio_stream::StreamExt as _;

use super::service::PersonalDbFrameStream;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct CompletedPayload {
    pub(super) path: String,
    pub(super) bytes: u64,
}

pub(super) fn record_payload_after_success<T, E>(
    result: Result<T, E>,
    payload: CompletedPayload,
    record: impl FnOnce(&CompletedPayload),
) -> Result<T, E> {
    let value = result?;
    record(&payload);
    Ok(value)
}

pub(super) fn record_payloads_when_stream_completes(
    mut stream: PersonalDbFrameStream,
    payloads: Vec<CompletedPayload>,
    mut record: impl FnMut(&CompletedPayload) + Send + 'static,
) -> PersonalDbFrameStream {
    Box::pin(async_stream::try_stream! {
        while let Some(frame) = stream.next().await {
            yield frame?;
        }
        for payload in &payloads {
            record(payload);
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anvil_api::v1::PersonalDbCanonicalFrame;
    use tonic::Status;

    use super::*;

    fn frame(value: &[u8]) -> PersonalDbCanonicalFrame {
        PersonalDbCanonicalFrame {
            value: value.to_vec(),
        }
    }

    #[test]
    fn ingress_is_recorded_only_after_success() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let payload = CompletedPayload {
            path: "_anvil/personaldb/v1/db/group/log/payloads/1.changeset".into(),
            bytes: 37,
        };
        let result = record_payload_after_success(Ok::<_, Status>(7), payload.clone(), {
            let recorded = recorded.clone();
            move |value| recorded.lock().unwrap().push(value.clone())
        });
        assert_eq!(result.unwrap(), 7);
        assert_eq!(*recorded.lock().unwrap(), [payload.clone()]);

        let error = Status::aborted("not committed");
        let result = record_payload_after_success(Err::<(), _>(error), payload, {
            let recorded = recorded.clone();
            move |value| recorded.lock().unwrap().push(value.clone())
        });
        assert_eq!(result.unwrap_err().code(), tonic::Code::Aborted);
        assert_eq!(recorded.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn egress_is_recorded_once_only_after_the_stream_completes() {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let payloads = vec![
            CompletedPayload {
                path: "first".into(),
                bytes: 11,
            },
            CompletedPayload {
                path: "second".into(),
                bytes: 29,
            },
        ];
        let source: PersonalDbFrameStream =
            Box::pin(tokio_stream::iter([Ok(frame(b"one")), Ok(frame(b"two"))]));
        let mut metered = record_payloads_when_stream_completes(source, payloads.clone(), {
            let recorded = recorded.clone();
            move |value| recorded.lock().unwrap().push(value.clone())
        });

        assert!(metered.next().await.unwrap().is_ok());
        assert!(recorded.lock().unwrap().is_empty());
        assert!(metered.next().await.unwrap().is_ok());
        assert!(recorded.lock().unwrap().is_empty());
        assert!(metered.next().await.is_none());
        assert_eq!(*recorded.lock().unwrap(), payloads);
        assert!(metered.next().await.is_none());
        assert_eq!(recorded.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn failed_or_unfinished_streams_record_no_egress() {
        let unfinished = Arc::new(Mutex::new(Vec::new()));
        let source: PersonalDbFrameStream =
            Box::pin(tokio_stream::iter([Ok(frame(b"one")), Ok(frame(b"two"))]));
        let mut metered = record_payloads_when_stream_completes(
            source,
            vec![CompletedPayload {
                path: "unfinished".into(),
                bytes: 41,
            }],
            {
                let unfinished = unfinished.clone();
                move |value| unfinished.lock().unwrap().push(value.clone())
            },
        );
        assert!(metered.next().await.unwrap().is_ok());
        drop(metered);
        assert!(unfinished.lock().unwrap().is_empty());

        let failed = Arc::new(Mutex::new(Vec::new()));
        let source: PersonalDbFrameStream = Box::pin(tokio_stream::iter([
            Ok(frame(b"one")),
            Err(Status::data_loss("broken stream")),
        ]));
        let mut metered = record_payloads_when_stream_completes(
            source,
            vec![CompletedPayload {
                path: "failed".into(),
                bytes: 43,
            }],
            {
                let failed = failed.clone();
                move |value| failed.lock().unwrap().push(value.clone())
            },
        );
        assert!(metered.next().await.unwrap().is_ok());
        assert!(metered.next().await.unwrap().is_err());
        assert!(metered.next().await.is_none());
        assert!(failed.lock().unwrap().is_empty());
    }
}
