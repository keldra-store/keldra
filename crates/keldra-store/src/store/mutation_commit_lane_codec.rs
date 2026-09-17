//! Native ordered completion marker encoding and validation.

use super::{
    LANE_COMPLETION_BYTES, LANE_COMPLETION_FORMAT, LANE_COMPLETION_PREFIX, LaneCompletion,
    VersionId,
};

pub(super) fn lane_completion_ticket_from_key(key: &[u8]) -> anyhow::Result<u64> {
    let suffix = key
        .strip_prefix(LANE_COMPLETION_PREFIX)
        .ok_or_else(|| anyhow::anyhow!("mutation lane completion key has the wrong prefix"))?;
    Ok(u64::from_be_bytes(suffix.try_into().map_err(|_| {
        anyhow::anyhow!("mutation lane completion key is malformed")
    })?))
}

impl LaneCompletion {
    pub(in crate::store) fn key(self) -> Vec<u8> {
        lane_completion_key(self.ticket)
    }

    pub(in crate::store) fn encode(self) -> [u8; LANE_COMPLETION_BYTES] {
        let mut encoded = [0_u8; LANE_COMPLETION_BYTES];
        encoded[0] = LANE_COMPLETION_FORMAT;
        let values = [
            self.ticket,
            self.first_offset,
            self.last_offset,
            self.journal_entries,
            self.journal_bytes,
            self.receipt_entries,
            self.receipt_bytes,
        ];
        for (index, value) in values.into_iter().enumerate() {
            let start = 1 + index * 8;
            encoded[start..start + 8].copy_from_slice(&value.to_be_bytes());
        }
        let option = 1 + values.len() * 8;
        if let Some(version) = self.high_version {
            encoded[option] = 1;
            encoded[option + 1..option + 9].copy_from_slice(&version.0.to_be_bytes());
        }
        encoded[option + 9] = u8::from(self.reference_cursor_advanced);
        encoded[option + 10] = u8::from(self.inline_reference_safe);
        encoded[option + 11] = u8::from(self.visibility_settled);
        encoded
    }

    pub(in crate::store) fn decode(encoded: &[u8]) -> Result<Self, String> {
        let encoded: &[u8; LANE_COMPLETION_BYTES] = encoded
            .try_into()
            .map_err(|_| "lane completion length is invalid".to_owned())?;
        if encoded[0] != LANE_COMPLETION_FORMAT {
            return Err("lane completion format is unsupported".into());
        }
        let read = |start: usize| {
            u64::from_be_bytes(encoded[start..start + 8].try_into().expect("fixed slice"))
        };
        let option = 1 + 7 * 8;
        let high_version = match encoded[option] {
            0 => None,
            1 => Some(VersionId(read(option + 1))),
            _ => return Err("lane completion version marker is invalid".into()),
        };
        let reference_cursor_advanced = match encoded[option + 9] {
            0 => false,
            1 => true,
            _ => return Err("lane completion reference-settlement marker is invalid".into()),
        };
        let inline_reference_safe = match encoded[option + 10] {
            0 => false,
            1 => true,
            _ => return Err("lane completion inline-reference marker is invalid".into()),
        };
        let visibility_settled = match encoded[option + 11] {
            0 => false,
            1 => true,
            _ => return Err("lane completion visibility marker is invalid".into()),
        };
        let completion = Self {
            ticket: read(1),
            first_offset: read(9),
            last_offset: read(17),
            journal_entries: read(25),
            journal_bytes: read(33),
            receipt_entries: read(41),
            receipt_bytes: read(49),
            high_version,
            reference_cursor_advanced,
            inline_reference_safe,
            visibility_settled,
        };
        completion.validate()?;
        Ok(completion)
    }

    pub(super) fn validate(self) -> Result<(), String> {
        if self.ticket == 0 {
            return Err("lane completion ticket is zero".into());
        }
        let range_entries = if self.first_offset == 0 && self.last_offset == 0 {
            0
        } else if self.first_offset == 0 || self.last_offset < self.first_offset {
            return Err("lane completion source range is invalid".into());
        } else {
            self.last_offset - self.first_offset + 1
        };
        if range_entries != self.journal_entries {
            return Err("lane completion source range disagrees with its entry count".into());
        }
        Ok(())
    }
}

pub(super) fn lane_completion_key(ticket: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(LANE_COMPLETION_PREFIX.len() + 8);
    key.extend_from_slice(LANE_COMPLETION_PREFIX);
    key.extend_from_slice(&ticket.to_be_bytes());
    key
}
