//! Portable succinct component codecs.
//!
//! `sux` intentionally is not serialized by copying Rust/native layout. These
//! codecs persist fixed little-endian words and validated support metadata,
//! then construct safe `sux` query structures from the decoded logical data.

use sux::dict::{EfSeq, EliasFanoBuilder, RearCodedListBuilder, RearCodedListStr};
#[cfg(test)]
use sux::traits::IndexedDict;
use sux::traits::IndexedSeq;

use crate::IndexError;
use crate::codec::{DecodeBudget, Decoder, Encoder};

const ELIAS_FANO_REVISION: u8 = 1;
const PREFIX_REVISION: u8 = 1;
const SELECT_SAMPLE_RATE: usize = 256;
const PREFIX_RESTART_INTERVAL: usize = 16;

#[derive(Debug)]
pub(crate) struct EliasFanoSequence {
    sequence: Option<EfSeq<u64>>,
    len: usize,
}

impl EliasFanoSequence {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get(&self, index: usize) -> Result<u64, IndexError> {
        if index >= self.len {
            return Err(IndexError::InvalidFormat("Elias-Fano index"));
        }
        Ok(self.sequence.as_ref().unwrap().get(index))
    }

    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        (0..self.len).map(|index| self.sequence.as_ref().unwrap().get(index))
    }
}

pub(crate) fn encode_elias_fano(values: &[u64]) -> Result<Vec<u8>, IndexError> {
    if values.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(IndexError::InvalidDefinition(
            "Elias-Fano values must be monotone".into(),
        ));
    }
    let count = values.len();
    let upper = values.last().copied().unwrap_or(0);
    let low_width = low_width(count, upper);
    let low_bit_count = count
        .checked_mul(low_width as usize)
        .ok_or(IndexError::OffsetOverflow)?;
    let mut low_words = vec![0u64; low_bit_count.div_ceil(64)];
    let high_length = if count == 0 {
        0
    } else {
        count
            .checked_add(1)
            .and_then(|value| value.checked_add((upper >> low_width) as usize))
            .ok_or(IndexError::OffsetOverflow)?
    };
    let mut high_words = vec![0u64; high_length.div_ceil(64)];
    let mut samples = Vec::with_capacity(count.div_ceil(SELECT_SAMPLE_RATE));
    for (index, value) in values.iter().copied().enumerate() {
        write_low(&mut low_words, index, low_width, value);
        let high = usize::try_from(value >> low_width).map_err(|_| IndexError::OffsetOverflow)?;
        let position = high.checked_add(index).ok_or(IndexError::OffsetOverflow)?;
        high_words[position / 64] |= 1u64 << (position % 64);
        if index % SELECT_SAMPLE_RATE == 0 {
            samples.push(u64::try_from(position).map_err(|_| IndexError::OffsetOverflow)?);
        }
    }

    let mut output = Encoder::default();
    output.u8(ELIAS_FANO_REVISION);
    output.u64(u64::try_from(count).map_err(|_| IndexError::OffsetOverflow)?);
    output.u64(upper);
    output.u8(low_width);
    output.u64(u64::try_from(high_length).map_err(|_| IndexError::OffsetOverflow)?);
    output.u64(u64::try_from(low_words.len()).map_err(|_| IndexError::OffsetOverflow)?);
    for word in low_words {
        output.u64(word);
    }
    output.u64(u64::try_from(high_words.len()).map_err(|_| IndexError::OffsetOverflow)?);
    for word in high_words {
        output.u64(word);
    }
    output.raw_u32(SELECT_SAMPLE_RATE as u32);
    output.u64(u64::try_from(samples.len()).map_err(|_| IndexError::OffsetOverflow)?);
    for position in samples {
        output.u64(position);
    }
    Ok(output.finish())
}

#[cfg(test)]
pub(crate) fn decode_elias_fano(bytes: &[u8]) -> Result<EliasFanoSequence, IndexError> {
    decode_elias_fano_with_budget(bytes, DecodeBudget::new())
}

pub(crate) fn decode_elias_fano_with_budget(
    bytes: &[u8],
    budget: DecodeBudget,
) -> Result<EliasFanoSequence, IndexError> {
    let checkpoint = budget.checkpoint();
    let mut decoder = Decoder::with_budget(bytes, budget.clone());
    if decoder.u8()? != ELIAS_FANO_REVISION {
        return Err(IndexError::InvalidFormat("Elias-Fano codec revision"));
    }
    let count = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    decoder.guard_count::<u64>(count, 0)?;
    let upper = decoder.u64()?;
    let encoded_low_width = decoder.u8()?;
    if encoded_low_width > 63 || encoded_low_width != low_width(count, upper) {
        return Err(IndexError::InvalidFormat("Elias-Fano low width"));
    }
    let low_width = encoded_low_width;
    let high_length = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    let expected_high_length = if count == 0 {
        0
    } else {
        count
            .checked_add(1)
            .and_then(|value| value.checked_add((upper >> low_width) as usize))
            .ok_or(IndexError::OffsetOverflow)?
    };
    if high_length != expected_high_length {
        return Err(IndexError::InvalidFormat("Elias-Fano high length"));
    }
    let low_count = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    let expected_low_count = count
        .checked_mul(low_width as usize)
        .ok_or(IndexError::OffsetOverflow)?
        .div_ceil(64);
    if low_count != expected_low_count {
        return Err(IndexError::InvalidFormat("Elias-Fano low words"));
    }
    decoder.guard_count::<u64>(low_count, 8)?;
    let mut low_words = Vec::with_capacity(low_count);
    for _ in 0..low_count {
        low_words.push(decoder.u64()?);
    }
    let high_count = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    if high_count != high_length.div_ceil(64) {
        return Err(IndexError::InvalidFormat("Elias-Fano high words"));
    }
    decoder.guard_count::<u64>(high_count, 8)?;
    let mut high_words = Vec::with_capacity(high_count);
    for _ in 0..high_count {
        high_words.push(decoder.u64()?);
    }
    if decoder.u32()? as usize != SELECT_SAMPLE_RATE {
        return Err(IndexError::InvalidFormat("Elias-Fano select sample rate"));
    }
    let sample_count = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    if sample_count != count.div_ceil(SELECT_SAMPLE_RATE) {
        return Err(IndexError::InvalidFormat("Elias-Fano select sample count"));
    }
    decoder.guard_count::<u64>(sample_count, 8)?;
    let mut samples = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        samples.push(decoder.u64()?);
    }
    decoder.finish()?;
    validate_unused_bits(&low_words, count.saturating_mul(low_width as usize))?;
    validate_unused_bits(&high_words, high_length)?;

    let mut values = Vec::with_capacity(count);
    let mut one_index = 0usize;
    for position in 0..high_length {
        if high_words[position / 64] & (1u64 << (position % 64)) == 0 {
            continue;
        }
        if one_index >= count {
            return Err(IndexError::InvalidFormat("Elias-Fano high population"));
        }
        if one_index % SELECT_SAMPLE_RATE == 0
            && samples[one_index / SELECT_SAMPLE_RATE]
                != u64::try_from(position).map_err(|_| IndexError::OffsetOverflow)?
        {
            return Err(IndexError::InvalidFormat("Elias-Fano select support"));
        }
        let high = position
            .checked_sub(one_index)
            .ok_or(IndexError::InvalidFormat("Elias-Fano high value"))?;
        let low = read_low(&low_words, one_index, low_width);
        let value =
            (u64::try_from(high).map_err(|_| IndexError::OffsetOverflow)? << low_width) | low;
        if value > upper || values.last().is_some_and(|previous| *previous > value) {
            return Err(IndexError::InvalidFormat("Elias-Fano monotonicity"));
        }
        values.push(value);
        one_index += 1;
    }
    if one_index != count || (count > 0 && values.last().copied() != Some(upper)) {
        return Err(IndexError::InvalidFormat("Elias-Fano population"));
    }
    let retained = count
        .checked_mul(std::mem::size_of::<u64>() * 2)
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(IndexError::OffsetOverflow)?;
    decoder.charge(retained)?;
    let sequence = build_sux(values);
    budget.rewind(checkpoint);
    budget.charge(retained)?;
    Ok(sequence)
}

fn build_sux(values: Vec<u64>) -> EliasFanoSequence {
    let len = values.len();
    let sequence = values.last().copied().map(|upper| {
        let mut builder = EliasFanoBuilder::<u64>::new(len, upper);
        for value in values {
            builder.push(value);
        }
        builder.build_with_seq()
    });
    EliasFanoSequence { sequence, len }
}

fn low_width(count: usize, upper: u64) -> u8 {
    if count > 0 && u128::from(upper) >= count as u128 {
        ((u128::from(upper) / count as u128).ilog2()).min(63) as u8
    } else {
        0
    }
}

fn write_low(words: &mut [u64], index: usize, width: u8, value: u64) {
    if width == 0 {
        return;
    }
    let width = width as usize;
    let bit = index * width;
    let word = bit / 64;
    let shift = bit % 64;
    let mask = (1u64 << width) - 1;
    words[word] |= (value & mask) << shift;
    if shift + width > 64 {
        words[word + 1] |= (value & mask) >> (64 - shift);
    }
}

fn read_low(words: &[u64], index: usize, width: u8) -> u64 {
    if width == 0 {
        return 0;
    }
    let width = width as usize;
    let bit = index * width;
    let word = bit / 64;
    let shift = bit % 64;
    let mask = (1u64 << width) - 1;
    let mut value = words[word] >> shift;
    if shift + width > 64 {
        value |= words[word + 1] << (64 - shift);
    }
    value & mask
}

fn validate_unused_bits(words: &[u64], used_bits: usize) -> Result<(), IndexError> {
    let remainder = used_bits % 64;
    if remainder > 0 && words.last().is_some_and(|word| *word >> remainder != 0) {
        return Err(IndexError::InvalidFormat("nonzero succinct padding bits"));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct PrefixDictionary {
    strings: RearCodedListStr<true>,
}

impl PrefixDictionary {
    pub(crate) fn len(&self) -> usize {
        self.strings.len()
    }

    pub(crate) fn get(&self, index: usize) -> Result<String, IndexError> {
        if index >= self.len() {
            return Err(IndexError::InvalidFormat("prefix dictionary index"));
        }
        Ok(self.strings.get(index))
    }

    #[cfg(test)]
    pub(crate) fn index_of(&self, value: &str) -> Option<usize> {
        self.strings.index_of(value)
    }
}

pub(crate) fn encode_prefix_dictionary(values: &[String]) -> Result<Vec<u8>, IndexError> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(IndexError::InvalidDefinition(
            "prefix dictionary strings must be unique and sorted".into(),
        ));
    }
    let mut payload = Encoder::default();
    let mut offsets = Vec::with_capacity(values.len().saturating_add(1));
    offsets.push(0u64);
    let mut previous = "";
    for (index, value) in values.iter().enumerate() {
        let prefix = if index % PREFIX_RESTART_INTERVAL == 0 {
            0
        } else {
            common_utf8_prefix(previous, value)
        };
        payload.u32(prefix)?;
        payload.bytes(&value.as_bytes()[prefix..])?;
        let length = u64::try_from(payload.len()).map_err(|_| IndexError::OffsetOverflow)?;
        offsets.push(length);
        previous = value;
    }
    let offsets = encode_elias_fano(&offsets)?;
    let payload = payload.finish();
    let mut output = Encoder::default();
    output.u8(PREFIX_REVISION);
    output.raw_u32(PREFIX_RESTART_INTERVAL as u32);
    output.u64(u64::try_from(values.len()).map_err(|_| IndexError::OffsetOverflow)?);
    output.bytes(&offsets)?;
    output.bytes(&payload)?;
    Ok(output.finish())
}

#[cfg(test)]
pub(crate) fn decode_prefix_dictionary(bytes: &[u8]) -> Result<PrefixDictionary, IndexError> {
    decode_prefix_dictionary_with_budget(bytes, DecodeBudget::new())
}

pub(crate) fn decode_prefix_dictionary_with_budget(
    bytes: &[u8],
    budget: DecodeBudget,
) -> Result<PrefixDictionary, IndexError> {
    let checkpoint = budget.checkpoint();
    let mut decoder = Decoder::with_budget(bytes, budget.clone());
    if decoder.u8()? != PREFIX_REVISION || decoder.u32()? as usize != PREFIX_RESTART_INTERVAL {
        return Err(IndexError::InvalidFormat("prefix dictionary revision"));
    }
    let count = usize::try_from(decoder.u64()?).map_err(|_| IndexError::OffsetOverflow)?;
    decoder.guard_count::<String>(count, 0)?;
    let nested_budget = decoder.budget();
    let offsets = decode_elias_fano_with_budget(decoder.bytes()?, nested_budget)?;
    let payload = decoder.bytes()?;
    decoder.finish()?;
    if offsets.len() != count.saturating_add(1)
        || offsets.get(0)? != 0
        || offsets.get(count)? != payload.len() as u64
    {
        return Err(IndexError::InvalidFormat("prefix dictionary offsets"));
    }
    let mut strings = Vec::with_capacity(count);
    let mut previous = String::new();
    let mut retained_string_bytes = 0usize;
    let mut largest_string_bytes = 0usize;
    for index in 0..count {
        let start = usize::try_from(offsets.get(index)?).map_err(|_| IndexError::OffsetOverflow)?;
        let end =
            usize::try_from(offsets.get(index + 1)?).map_err(|_| IndexError::OffsetOverflow)?;
        let record = payload
            .get(start..end)
            .ok_or(IndexError::InvalidFormat("prefix dictionary record"))?;
        let mut record = Decoder::with_budget(record, decoder.budget());
        let prefix = record.u32()? as usize;
        let suffix = record.bytes()?;
        record.finish()?;
        if (index % PREFIX_RESTART_INTERVAL == 0 && prefix != 0)
            || prefix > previous.len()
            || !previous.is_char_boundary(prefix)
        {
            return Err(IndexError::InvalidFormat("prefix dictionary prefix length"));
        }
        let suffix = std::str::from_utf8(suffix)
            .map_err(|_| IndexError::InvalidFormat("prefix dictionary UTF-8"))?;
        let mut value = previous[..prefix].to_owned();
        value.push_str(suffix);
        if strings.last().is_some_and(|previous| previous >= &value) {
            return Err(IndexError::InvalidFormat("prefix dictionary order"));
        }
        decoder.charge(value.len())?;
        retained_string_bytes = retained_string_bytes
            .checked_add(value.len())
            .ok_or(IndexError::OffsetOverflow)?;
        largest_string_bytes = largest_string_bytes.max(value.len());
        previous = value.clone();
        strings.push(value);
    }
    decoder.charge(largest_string_bytes)?;
    let retained = count
        .checked_mul(std::mem::size_of::<String>())
        .and_then(|bytes| bytes.checked_add(retained_string_bytes))
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or(IndexError::OffsetOverflow)?;
    decoder.charge(retained)?;
    let mut builder = RearCodedListBuilder::<str, true>::new(PREFIX_RESTART_INTERVAL);
    for value in &strings {
        builder.push(value.as_str());
    }
    let dictionary = PrefixDictionary {
        strings: builder.build(),
    };
    budget.rewind(checkpoint);
    budget.charge(retained)?;
    Ok(dictionary)
}

fn common_utf8_prefix(left: &str, right: &str) -> usize {
    let mut length = left
        .as_bytes()
        .iter()
        .zip(right.as_bytes())
        .take_while(|(left, right)| left == right)
        .count();
    while !left.is_char_boundary(length) || !right.is_char_boundary(length) {
        length -= 1;
    }
    length
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_elias_fano_round_trips_and_beats_fixed_width() {
        let values = (0..20_000u64).map(|value| value * 3).collect::<Vec<_>>();
        let encoded = encode_elias_fano(&values).unwrap();
        assert!(encoded.len() < values.len() * 8 / 2);
        let decoded = decode_elias_fano(&encoded).unwrap();
        assert_eq!(decoded.iter().collect::<Vec<_>>(), values);
    }

    #[test]
    fn prefix_dictionary_round_trips_and_compresses_repeated_paths() {
        let values = (0..10_000)
            .map(|value| format!("/tenant/00000001/advisories/{value:08}"))
            .collect::<Vec<_>>();
        let plain = values.iter().map(|value| 4 + value.len()).sum::<usize>();
        let encoded = encode_prefix_dictionary(&values).unwrap();
        assert!(encoded.len() < plain / 2);
        let decoded = decode_prefix_dictionary(&encoded).unwrap();
        assert_eq!(decoded.len(), values.len());
        assert_eq!(decoded.get(731).unwrap(), values[731]);
        assert_eq!(decoded.index_of(&values[9000]), Some(9000));
    }

    #[test]
    fn corrupt_select_support_is_rejected() {
        let values = (0..1024u64).map(|value| value * 5).collect::<Vec<_>>();
        let mut encoded = encode_elias_fano(&values).unwrap();
        *encoded.last_mut().unwrap() ^= 1;
        assert!(decode_elias_fano(&encoded).is_err());
    }
}
