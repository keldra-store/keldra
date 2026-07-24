use super::*;

impl CoreStore {
    /// Finishes the one direct append generation that can immediately follow
    /// the visible stream head. A failed publisher may leave that generation's
    /// durable intent behind, so a later append must recover it before it can
    /// safely allocate another record at the same sequence.
    pub(super) async fn resume_pending_direct_stream_publication_unlocked(
        &self,
        stream_id: &str,
    ) -> Result<bool> {
        let next_sequence = self
            .read_stream_head_from_meta(stream_id)?
            .map_or(Ok(1), |head| {
                head.last_sequence
                    .checked_add(1)
                    .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))
            })?;
        let transaction_id = super::local_roots_layout::direct_stream_publication_transaction_id(
            stream_id,
            next_sequence,
            next_sequence,
        );
        let Some(intent) = self.read_root_publication_intent(&transaction_id)? else {
            return Ok(false);
        };

        self.validate_pending_direct_stream_publication(
            stream_id,
            next_sequence,
            &transaction_id,
            &intent,
        )?;
        self.resume_root_publication_intent(intent).await?;

        let published_sequence = self
            .read_stream_head_from_meta(stream_id)?
            .map(|head| head.last_sequence)
            .ok_or_else(|| anyhow!("CoreStore recovered stream publication has no visible head"))?;
        if published_sequence != next_sequence {
            bail!(
                "CoreStore recovered stream publication advanced {stream_id} to sequence \
                 {published_sequence}, expected {next_sequence}"
            );
        }
        Ok(true)
    }

    fn validate_pending_direct_stream_publication(
        &self,
        stream_id: &str,
        sequence: u64,
        transaction_id: &str,
        intent: &RootPublicationIntent,
    ) -> Result<()> {
        if intent.transaction_id != transaction_id {
            bail!("CoreStore pending stream publication transaction scope mismatch");
        }
        let Some((actual_stream_id, first_sequence, last_sequence)) =
            self.validate_direct_stream_publication_intent(intent)?
        else {
            bail!("CoreStore pending stream publication does not use a direct stream id");
        };
        if actual_stream_id != stream_id || first_sequence != sequence || last_sequence != sequence
        {
            bail!("CoreStore pending stream publication candidate scope mismatch");
        }
        Ok(())
    }

    pub(super) fn validate_direct_stream_publication_intent(
        &self,
        intent: &RootPublicationIntent,
    ) -> Result<Option<(String, u64, u64)>> {
        let Some((stream_id, first_sequence, last_sequence)) =
            super::local_roots_layout::direct_stream_publication_transaction_parts(
                &intent.transaction_id,
            )?
        else {
            return Ok(None);
        };
        if intent.roots.len() != 1 {
            bail!("CoreStore direct stream publication must contain exactly one root");
        }
        let record_count = last_sequence
            .checked_sub(first_sequence)
            .and_then(|difference| difference.checked_add(1))
            .ok_or_else(|| anyhow!("CoreStore direct stream publication range overflow"))?;
        if record_count > super::local_root_publication_recovery::MAX_PUBLICATION_ROWS as u64 {
            bail!("CoreStore direct stream publication range exceeds row bounds");
        }

        let expected_root_anchor =
            super::local_roots_layout::stream_coremeta_root_anchor_key(&stream_id);
        let expected_publication = if stream_id == CORE_TRANSACTION_STREAM_ID {
            CoreMetaRootPublication::new(expected_root_anchor, WriterFamily::CoreControl)
                .coordinator()
        } else {
            CoreMetaRootPublication::new(expected_root_anchor, WriterFamily::Stream)
        };
        let root = &intent.roots[0];
        if root.publication.descriptor != expected_publication {
            bail!("CoreStore pending stream publication root descriptor mismatch");
        }

        for sequence in first_sequence..=last_sequence {
            let expected_record_key = stream_record_key(&stream_id, sequence);
            if !publication_contains_row(
                &root.rows,
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &expected_record_key,
            )? {
                bail!("CoreStore pending stream publication is missing its record row");
            }
        }
        let expected_head_key = stream_head_key(&stream_id);
        if !publication_contains_row(
            &root.rows,
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &expected_head_key,
        )? {
            bail!("CoreStore pending stream publication is missing its head row");
        }
        Ok(Some((stream_id, first_sequence, last_sequence)))
    }
}

fn publication_contains_row(
    rows: &[CoreMetaEncodedOwnedRow],
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> Result<bool> {
    for row in rows {
        if row.cf == cf
            && core_meta_record_table_id(&row.core_meta_key)? == table_id
            && core_meta_record_tuple_key(&row.core_meta_key)? == tuple_key
        {
            return Ok(true);
        }
    }
    Ok(false)
}
