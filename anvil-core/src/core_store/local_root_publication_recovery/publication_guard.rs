use super::*;

const MAX_TERMINAL_REASON_CHARS: usize = 1_024;

pub(super) fn append_publication_guard_plan_hash(
    bytes: &mut Vec<u8>,
    _guard: Option<&CorePublicationGuardSummary>,
) {
    append_hash_part(bytes, b"anvil.core.root_publication_guard.none.v1");
}

impl RootPublicationIntent {
    fn encoded_rows(&self) -> impl Iterator<Item = &CoreMetaEncodedOwnedRow> {
        self.roots
            .iter()
            .flat_map(|root| root.rows.iter())
            .chain(self.local_rows.iter())
    }

    pub(super) fn transaction_deadline_elapsed(&self) -> Result<bool> {
        Ok(false)
    }
}

impl CoreStore {
    pub(in crate::core_store::local) async fn ensure_publication_intent_active(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        intent.ensure_pending()
    }

    pub(super) fn ensure_publication_intent_active_locked(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        intent.ensure_pending()
    }

    pub(super) async fn acquire_publication_intent_locks(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<(Vec<CoreStoreLock>, Option<()>)> {
        let mut keys = BTreeSet::new();
        keys.insert(("transaction".to_string(), intent.transaction_id.clone()));
        for root in &intent.roots {
            keys.insert((
                "root-publication".to_string(),
                root.publication.descriptor.root_key_hash(),
            ));
        }
        for row in intent.encoded_rows() {
            let cf = canonical_coremeta_cf_name(&row.cf)?;
            let table_id = core_meta_record_table_id(&row.core_meta_key)?;
            let tuple_key = core_meta_record_tuple_key(&row.core_meta_key)?;
            Self::insert_coremeta_row_lock(&mut keys, cf, table_id, tuple_key);
            if !row.root_key_hash.is_empty() {
                keys.insert(("root-publication".to_string(), row.root_key_hash.clone()));
            }
        }
        Ok((self.acquire_sorted_lock_keys(&keys).await?, None))
    }

    pub(super) async fn validate_publication_guards_at_linearization(
        &self,
        intent: &RootPublicationIntent,
        _context: Option<&()>,
    ) -> Result<()> {
        intent.ensure_pending()?;
        if intent.guard.is_some() {
            bail!("legacy transaction publication guards are not supported");
        }
        Ok(())
    }

    pub(super) fn terminal_publication_guard_failure<T>(
        &self,
        intent: &RootPublicationIntent,
        reason: &str,
    ) -> Result<T> {
        self.mark_root_publication_intent_terminal(intent, reason)?;
        Err(publication_terminal_error(reason))
    }

    pub(in crate::core_store::local) fn mark_root_publication_intent_terminal(
        &self,
        intent: &RootPublicationIntent,
        reason: &str,
    ) -> Result<()> {
        if intent.state == RootPublicationIntentState::Terminal {
            return intent.ensure_pending();
        }
        if reason.trim().is_empty() {
            bail!("CoreMeta publication terminal reason must not be empty");
        }
        if !self.validate_persisted_root_publication_intent_summary(intent)? {
            bail!("CoreMeta publication intent disappeared before terminalization");
        }
        let mut terminal = intent.clone();
        terminal.state = RootPublicationIntentState::Terminal;
        terminal.terminal_reason = Some(reason.chars().take(MAX_TERMINAL_REASON_CHARS).collect());
        let header = intent_header_proto(&terminal)?;
        let tuple_key = intent_header_key(&terminal.transaction_id)?;
        let payload = encode_deterministic_proto(&header);
        self.meta.write_local_committed_batch(&[CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_ROOT_PUBLICATION_INTENT_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }])
    }
}
