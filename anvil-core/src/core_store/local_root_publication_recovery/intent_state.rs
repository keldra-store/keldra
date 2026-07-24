use super::*;

#[derive(Debug, thiserror::Error)]
#[error("CoreMetaPublicationTerminal: {reason}")]
struct CoreMetaPublicationTerminalError {
    reason: String,
}

pub(in crate::core_store::local) fn publication_terminal_reason(
    error: &anyhow::Error,
) -> Option<&str> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<CoreMetaPublicationTerminalError>()
            .map(|terminal| terminal.reason.as_str())
    })
}

pub(super) fn publication_terminal_error(reason: impl Into<String>) -> anyhow::Error {
    CoreMetaPublicationTerminalError {
        reason: reason.into(),
    }
    .into()
}

impl CoreStore {
    pub(in crate::core_store::local) async fn resume_root_publication_intent_for_recovery(
        &self,
        intent: RootPublicationIntent,
    ) -> Result<()> {
        match self.resume_root_publication_intent(intent).await {
            Ok(_) => Ok(()),
            Err(error) if publication_terminal_reason(&error).is_some() => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl RootPublicationIntent {
    pub(in crate::core_store::local) fn ensure_pending(&self) -> Result<()> {
        if self.state == RootPublicationIntentState::Terminal {
            return Err(publication_terminal_error(
                self.terminal_reason
                    .as_deref()
                    .unwrap_or("publication guard failed"),
            ));
        }
        Ok(())
    }

    pub(in crate::core_store::local) fn coordinator_scope(&self) -> Result<Option<(String, u64)>> {
        let coordinators = self
            .roots
            .iter()
            .filter(|root| root.publication.descriptor.transaction_coordinator)
            .collect::<Vec<_>>();
        match coordinators.as_slice() {
            [] => {
                if self.roots.len() > 1 {
                    bail!(
                        "CoreMeta publication intent spanning multiple roots requires a coordinator"
                    );
                }
                Ok(None)
            }
            [root] => Ok(Some((
                root.publication.descriptor.root_key_hash(),
                root.publication.post_root_generation,
            ))),
            _ => bail!("CoreMeta publication intent has multiple coordinator roots"),
        }
    }

    pub(in crate::core_store::local) fn all_outcomes_recorded(&self) -> bool {
        self.roots
            .iter()
            .all(|root| root.certificate_hash.is_some())
    }

    pub(in crate::core_store::local) fn no_outcomes_recorded(&self) -> bool {
        self.roots
            .iter()
            .all(|root| root.certificate_hash.is_none())
    }
}
