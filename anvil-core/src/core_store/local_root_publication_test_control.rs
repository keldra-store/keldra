#[cfg(feature = "root-publication-test-control")]
use super::{CoreStore, validate_logical_id};
#[cfg(feature = "root-publication-test-control")]
use anyhow::{Context, Result, anyhow};
#[cfg(feature = "root-publication-test-control")]
use std::path::{Path, PathBuf};
#[cfg(feature = "root-publication-test-control")]
use std::time::Duration;

#[cfg(feature = "root-publication-test-control")]
const CONTROL_DIR_ENV: &str = "ANVIL_TEST_ROOT_PUBLICATION_CONTROL_DIR";
#[cfg(feature = "root-publication-test-control")]
const ARM_AFTER_Q2_PREFIX: &str = "pause-after-root-register-q2-";
#[cfg(feature = "root-publication-test-control")]
const REACHED_AFTER_Q2_PREFIX: &str = "reached-after-root-register-q2-";
#[cfg(feature = "root-publication-test-control")]
const RELEASE_AFTER_Q2_PREFIX: &str = "release-after-root-register-q2-";

pub(super) async fn pause_after_root_register_commit(transaction_id: &str) {
    #[cfg(test)]
    in_process::pause_after_root_register_commit(transaction_id).await;

    #[cfg(feature = "root-publication-test-control")]
    pause_for_external_control(transaction_id).await;
}

#[cfg(feature = "root-publication-test-control")]
impl CoreStore {
    pub(crate) async fn arm_external_root_publication_pause_after_q2(
        &self,
        transaction_id: &str,
    ) -> Result<()> {
        validate_logical_id(transaction_id, "root-publication test transaction id")?;
        let control_dir = require_external_control_dir()?;
        tokio::fs::create_dir_all(&control_dir)
            .await
            .with_context(|| {
                format!(
                    "create root-publication test control directory {}",
                    control_dir.display()
                )
            })?;
        for prefix in [REACHED_AFTER_Q2_PREFIX, RELEASE_AFTER_Q2_PREFIX] {
            remove_marker_if_present(marker_path(&control_dir, prefix, transaction_id)).await?;
        }
        let armed = marker_path(&control_dir, ARM_AFTER_Q2_PREFIX, transaction_id);
        tokio::fs::write(&armed, transaction_id.as_bytes())
            .await
            .with_context(|| {
                format!(
                    "write root-publication test pause marker {}",
                    armed.display()
                )
            })
    }

    pub(crate) async fn external_root_publication_test_status(
        &self,
        transaction_id: &str,
    ) -> Result<(bool, bool, bool, bool)> {
        validate_logical_id(transaction_id, "root-publication test transaction id")?;
        let control_dir = require_external_control_dir()?;
        let pause_reached = marker_exists(marker_path(
            &control_dir,
            REACHED_AFTER_Q2_PREFIX,
            transaction_id,
        ))
        .await?;
        // The pause transition atomically renames the armed marker to reached.
        // Read reached first and treat it as authoritative so a status request
        // cannot observe armed before the rename and reached after it.
        let armed = if pause_reached {
            false
        } else {
            marker_exists(marker_path(
                &control_dir,
                ARM_AFTER_Q2_PREFIX,
                transaction_id,
            ))
            .await?
        };
        let intent_present = self.read_root_publication_intent(transaction_id)?.is_some();
        Ok((
            armed,
            pause_reached,
            intent_present,
            self.coremeta_recovery_ready(),
        ))
    }
}

#[cfg(feature = "root-publication-test-control")]
async fn pause_for_external_control(transaction_id: &str) {
    let Some(control_dir) = external_control_dir() else {
        return;
    };
    let armed = marker_path(&control_dir, ARM_AFTER_Q2_PREFIX, transaction_id);
    let reached = marker_path(&control_dir, REACHED_AFTER_Q2_PREFIX, transaction_id);
    let release = marker_path(&control_dir, RELEASE_AFTER_Q2_PREFIX, transaction_id);
    let armed_transaction = match tokio::fs::read_to_string(&armed).await {
        Ok(transaction_id) => transaction_id,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(
                path = %armed.display(),
                %error,
                "failed to read root-publication test pause marker"
            );
            return;
        }
    };
    if armed_transaction.trim() != transaction_id {
        tracing::warn!(
            path = %armed.display(),
            "ignored mismatched root-publication test pause marker"
        );
        return;
    }
    if let Err(error) = tokio::fs::rename(&armed, &reached).await {
        tracing::warn!(
            path = %armed.display(),
            %error,
            "failed to enter root-publication test pause"
        );
        return;
    }

    loop {
        match tokio::fs::metadata(&release).await {
            Ok(_) => {
                let _ = tokio::fs::remove_file(&release).await;
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Err(error) => {
                tracing::warn!(
                    path = %release.display(),
                    %error,
                    "failed to poll root-publication test release marker"
                );
                return;
            }
        }
    }
}

#[cfg(feature = "root-publication-test-control")]
async fn marker_exists(path: PathBuf) -> Result<bool> {
    match tokio::fs::metadata(&path).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error)
            .with_context(|| format!("inspect root-publication test marker {}", path.display())),
    }
}

#[cfg(feature = "root-publication-test-control")]
async fn remove_marker_if_present(path: PathBuf) -> Result<()> {
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error)
            .with_context(|| format!("remove root-publication test marker {}", path.display())),
    }
}

#[cfg(feature = "root-publication-test-control")]
fn require_external_control_dir() -> Result<PathBuf> {
    external_control_dir().ok_or_else(|| anyhow!("root-publication test control is disabled"))
}

#[cfg(feature = "root-publication-test-control")]
fn external_control_dir() -> Option<PathBuf> {
    std::env::var_os(CONTROL_DIR_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(feature = "root-publication-test-control")]
fn marker_path(control_dir: &Path, prefix: &str, transaction_id: &str) -> PathBuf {
    control_dir.join(format!(
        "{prefix}{}",
        external_control_token(transaction_id)
    ))
}

#[cfg(feature = "root-publication-test-control")]
fn external_control_token(transaction_id: &str) -> String {
    blake3::hash(transaction_id.as_bytes()).to_hex().to_string()
}

#[cfg(test)]
mod in_process {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::{Arc, LazyLock, Mutex as StdMutex};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct PublicationPauseState {
        transaction_id: String,
        point: PublicationPausePoint,
        reached: Arc<Notify>,
        release: Arc<Notify>,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    enum PublicationPausePoint {
        BeforeCoordinator,
        AfterRootRegisterQuorum,
    }

    type PublicationPauseKey = (String, PublicationPausePoint);

    static PUBLICATION_PAUSES: LazyLock<
        StdMutex<HashMap<PublicationPauseKey, PublicationPauseState>>,
    > = LazyLock::new(|| StdMutex::new(HashMap::new()));
    static PUBLICATION_FAILURES: LazyLock<StdMutex<BTreeSet<String>>> =
        LazyLock::new(|| StdMutex::new(BTreeSet::new()));

    pub(crate) struct PublicationPause {
        transaction_id: String,
        point: PublicationPausePoint,
        reached: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl PublicationPause {
        pub(crate) async fn wait_until_reached(&self) {
            self.reached.notified().await;
        }

        pub(crate) fn release(self) {
            drop(self);
        }
    }

    impl Drop for PublicationPause {
        fn drop(&mut self) {
            self.release.notify_one();
            let mut pauses = PUBLICATION_PAUSES
                .lock()
                .expect("publication pause lock poisoned");
            pauses.remove(&(self.transaction_id.clone(), self.point));
        }
    }

    pub(crate) fn pause_publication(transaction_id: &str) -> PublicationPause {
        pause_publication_at(transaction_id, PublicationPausePoint::BeforeCoordinator)
    }

    pub(crate) fn pause_after_root_register_quorum(transaction_id: &str) -> PublicationPause {
        pause_publication_at(
            transaction_id,
            PublicationPausePoint::AfterRootRegisterQuorum,
        )
    }

    fn pause_publication_at(
        transaction_id: &str,
        point: PublicationPausePoint,
    ) -> PublicationPause {
        let reached = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let state = PublicationPauseState {
            transaction_id: transaction_id.to_string(),
            point,
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
        };
        let mut pauses = PUBLICATION_PAUSES
            .lock()
            .expect("publication pause lock poisoned");
        let key = (transaction_id.to_string(), point);
        assert!(
            pauses.insert(key, state).is_none(),
            "a publication pause is already active for this transaction and point"
        );
        PublicationPause {
            transaction_id: transaction_id.to_string(),
            point,
            reached,
            release,
        }
    }

    pub(crate) async fn pause_before_coordinator(transaction_id: &str) {
        pause_at(transaction_id, PublicationPausePoint::BeforeCoordinator).await;
    }

    pub(super) async fn pause_after_root_register_commit(transaction_id: &str) {
        pause_at(
            transaction_id,
            PublicationPausePoint::AfterRootRegisterQuorum,
        )
        .await;
    }

    async fn pause_at(transaction_id: &str, point: PublicationPausePoint) {
        let state = PUBLICATION_PAUSES
            .lock()
            .expect("publication pause lock poisoned")
            .get(&(transaction_id.to_string(), point))
            .cloned();
        let Some(state) = state else {
            return;
        };
        state.reached.notify_one();
        state.release.notified().await;
    }

    pub(crate) fn fail_publication_once(transaction_id: &str) {
        PUBLICATION_FAILURES
            .lock()
            .expect("publication failure lock poisoned")
            .insert(transaction_id.to_string());
    }

    pub(crate) fn take_publication_failure(transaction_id: &str) -> bool {
        PUBLICATION_FAILURES
            .lock()
            .expect("publication failure lock poisoned")
            .remove(transaction_id)
    }
}

#[cfg(test)]
pub(super) use in_process::{
    PublicationPause, fail_publication_once, pause_after_root_register_quorum,
    pause_before_coordinator, pause_publication, take_publication_failure,
};
