use std::fmt::{Display, Formatter};

use jiff::fmt::strtime;
use jiff::fmt::strtime::BrokenDownTime;
use jiff::tz::TimeZone;
use jiff::{Timestamp, civil::Date, civil::DateTime};
use keldra_index::typed_json::DateFormat;

const MAX_DATE_PATTERN_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DateError {
    InvalidFormat,
    InvalidValue,
    PrecisionLoss,
    OutOfRange,
}

impl Display for DateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidFormat => "date format is invalid or unsupported",
            Self::InvalidValue => "date value does not match its field format",
            Self::PrecisionLoss => "date value has non-zero precision finer than milliseconds",
            Self::OutOfRange => "date value is outside the signed Unix-millisecond range",
        })
    }
}

pub(crate) fn validate_format(format: &DateFormat) -> Result<(), DateError> {
    let DateFormat::Strftime(pattern) = format else {
        return Ok(());
    };
    if pattern.is_empty()
        || pattern.len() > MAX_DATE_PATTERN_BYTES
        || pattern.contains('\0')
        || uses_unsupported_directive(pattern)
    {
        return Err(DateError::InvalidFormat);
    }

    // A fixed complete instant proves both formatting syntax and that parsing
    // the emitted form supplies a complete calendar date. It also rejects
    // timestamp-only, time-only, locale-dependent and named-zone patterns.
    let sample =
        Timestamp::from_millisecond(1_721_016_123_456).map_err(|_| DateError::InvalidFormat)?;
    let encoded = strtime::format(pattern, sample).map_err(|_| DateError::InvalidFormat)?;
    let parsed = BrokenDownTime::parse(pattern, encoded).map_err(|_| DateError::InvalidFormat)?;
    parsed.to_date().map_err(|_| DateError::InvalidFormat)?;
    timestamp_from_broken_down(&parsed).map_err(|_| DateError::InvalidFormat)?;
    Ok(())
}

pub(crate) fn parse_millis(value: &str, format: &DateFormat) -> Result<i64, DateError> {
    let timestamp = match format {
        DateFormat::Iso8601 => parse_iso8601(value)?,
        DateFormat::Strftime(pattern) => {
            let parsed =
                BrokenDownTime::parse(pattern, value).map_err(|_| DateError::InvalidValue)?;
            timestamp_from_broken_down(&parsed)?
        }
    };
    if timestamp.subsec_nanosecond() % 1_000_000 != 0 {
        return Err(DateError::PrecisionLoss);
    }
    Ok(timestamp.as_millisecond())
}

pub(crate) fn format_millis(value: i64, format: &DateFormat) -> Result<String, DateError> {
    let timestamp = Timestamp::from_millisecond(value).map_err(|_| DateError::OutOfRange)?;
    match format {
        DateFormat::Iso8601 => Ok(timestamp.to_string()),
        DateFormat::Strftime(pattern) => {
            strtime::format(pattern, timestamp).map_err(|_| DateError::InvalidFormat)
        }
    }
}

fn parse_iso8601(value: &str) -> Result<Timestamp, DateError> {
    if let Ok(timestamp) = value.parse::<Timestamp>() {
        return Ok(timestamp);
    }
    if let Ok(datetime) = value.parse::<DateTime>() {
        return TimeZone::UTC
            .to_timestamp(datetime)
            .map_err(|_| DateError::OutOfRange);
    }
    let date = value.parse::<Date>().map_err(|_| DateError::InvalidValue)?;
    TimeZone::UTC
        .to_timestamp(DateTime::from(date))
        .map_err(|_| DateError::OutOfRange)
}

fn timestamp_from_broken_down(value: &BrokenDownTime) -> Result<Timestamp, DateError> {
    let datetime = value.to_datetime().map_err(|_| DateError::InvalidValue)?;
    match value.offset() {
        Some(offset) => offset
            .to_timestamp(datetime)
            .map_err(|_| DateError::OutOfRange),
        None => TimeZone::UTC
            .to_timestamp(datetime)
            .map_err(|_| DateError::OutOfRange),
    }
}

fn uses_unsupported_directive(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'%' {
            index += 1;
            continue;
        }
        index += 1;
        if index == bytes.len() {
            return true;
        }
        if bytes[index] == b'%' {
            index += 1;
            continue;
        }
        while index < bytes.len() && !bytes[index].is_ascii_alphabetic() && bytes[index] != b'+' {
            index += 1;
        }
        if index == bytes.len() {
            return true;
        }
        // Locale words/meridiem and named time zones are deliberately outside
        // the stable schema contract. E/O modifiers are locale alternatives.
        if matches!(
            bytes[index],
            b'a' | b'A'
                | b'b'
                | b'B'
                | b'h'
                | b'c'
                | b'x'
                | b'X'
                | b'p'
                | b'P'
                | b's'
                | b'Z'
                | b'Q'
                | b'E'
                | b'O'
        ) {
            return true;
        }
        index += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso8601_normalizes_offsets_and_date_only_values_to_utc_millis() {
        let format = DateFormat::Iso8601;
        let expected = parse_millis("2024-07-15T20:24:59.123Z", &format).unwrap();
        assert_eq!(
            parse_millis("2024-07-15T16:24:59.123-04:00", &format).unwrap(),
            expected
        );
        assert_eq!(
            format_millis(expected, &format).unwrap(),
            "2024-07-15T20:24:59.123Z"
        );
        assert_eq!(
            format_millis(parse_millis("1970-01-02", &format).unwrap(), &format).unwrap(),
            "1970-01-02T00:00:00Z"
        );
    }

    #[test]
    fn custom_format_defaults_missing_offset_to_utc_and_round_trips() {
        let format = DateFormat::Strftime("%d/%m/%Y %H:%M:%S%.3f".into());
        validate_format(&format).unwrap();
        let millis = parse_millis("15/07/2024 20:24:59.123", &format).unwrap();
        assert_eq!(
            format_millis(millis, &format).unwrap(),
            "15/07/2024 20:24:59.123"
        );
    }

    #[test]
    fn invalid_patterns_and_sub_millisecond_values_are_rejected() {
        for pattern in ["", "%H:%M", "%Y-%B-%d", "%Y-%m-%d %Q", "%"] {
            assert_eq!(
                validate_format(&DateFormat::Strftime(pattern.into())),
                Err(DateError::InvalidFormat),
                "{pattern:?}"
            );
        }
        assert_eq!(
            parse_millis("2024-07-15T20:24:59.123000001Z", &DateFormat::Iso8601),
            Err(DateError::PrecisionLoss)
        );
    }
}
