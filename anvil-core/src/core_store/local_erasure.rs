use super::*;

pub(super) fn local_block_id_for_stored_block(profile_id: &str, stored_hash: &str) -> String {
    let mut hasher = Sha256::new();
    for part in ["anvil.block.id.v2", profile_id, stored_hash] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("blk_{}", hex::encode(hasher.finalize()))
}

pub(super) fn local_block_id_for_logical_block(
    logical_file_id: &str,
    writer_generation: u64,
    block_ordinal: u64,
    plaintext_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        "anvil.block.logical_id.v1",
        logical_file_id,
        &writer_generation.to_string(),
        &block_ordinal.to_string(),
        plaintext_hash,
    ] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("blk_{}", hex::encode(hasher.finalize()))
}

pub(super) fn encode_erasure_shards(
    bytes: &[u8],
    profile: LocalErasureProfile,
) -> Result<Vec<Vec<u8>>> {
    let shard_len = bytes.len().div_ceil(profile.data_shards).max(1);
    let total_shards = profile.total_shards();
    let mut shards = vec![vec![0u8; shard_len]; total_shards];
    for (index, shard) in shards.iter_mut().take(profile.data_shards).enumerate() {
        let start = index.saturating_mul(shard_len);
        if start >= bytes.len() {
            break;
        }
        let end = usize::min(start + shard_len, bytes.len());
        shard[..end - start].copy_from_slice(&bytes[start..end]);
    }
    for parity_row in 0..profile.parity_shards {
        let parity_index = profile.data_shards + parity_row;
        for byte_index in 0..shard_len {
            let mut acc = 0u8;
            for data_index in 0..profile.data_shards {
                let coefficient = gf_pow((data_index + 1) as u8, parity_row as u32);
                acc ^= gf_mul(coefficient, shards[data_index][byte_index]);
            }
            shards[parity_index][byte_index] = acc;
        }
    }
    Ok(shards)
}

pub(super) fn reconstruct_data_shards(
    shards: &mut [Option<Vec<u8>>],
    profile: LocalErasureProfile,
) -> Result<()> {
    let total_shards = profile.total_shards();
    if shards.len() != total_shards {
        bail!(
            "CoreStore erasure reconstruction expected {} shards for {}, got {}",
            total_shards,
            profile.id,
            shards.len()
        );
    }
    let shard_len = shards
        .iter()
        .find_map(|shard| shard.as_ref().map(Vec::len))
        .ok_or_else(|| anyhow!("CoreStore erasure reconstruction has no shards"))?;
    for shard in shards.iter().flatten() {
        if shard.len() != shard_len {
            bail!("CoreStore erasure reconstruction shard lengths differ");
        }
    }
    if shards.iter().filter(|shard| shard.is_some()).count() < profile.minimum_read_shards {
        bail!(
            "CoreStore erasure reconstruction has fewer than {} readable shards for {}",
            profile.minimum_read_shards,
            profile.id
        );
    }
    if shards.iter().take(profile.data_shards).all(Option::is_some) {
        return Ok(());
    }

    let selected = shards
        .iter()
        .enumerate()
        .filter_map(|(index, shard)| shard.as_ref().map(|payload| (index, payload.clone())))
        .take(profile.data_shards)
        .collect::<Vec<_>>();
    if selected.len() < profile.data_shards {
        bail!("CoreStore erasure reconstruction cannot select enough shards");
    }

    let matrix = selected
        .iter()
        .map(|(shard_index, _)| erasure_coding_row(*shard_index, profile.data_shards))
        .collect::<Vec<_>>();
    let inverse = invert_gf256_matrix(&matrix)?;
    for data_index in 0..profile.data_shards {
        if shards[data_index].is_some() {
            continue;
        }
        let mut reconstructed = vec![0u8; shard_len];
        for (source_row, (_, source_payload)) in selected.iter().enumerate() {
            let coefficient = inverse[data_index][source_row];
            if coefficient == 0 {
                continue;
            }
            for byte_index in 0..shard_len {
                reconstructed[byte_index] ^= gf_mul(coefficient, source_payload[byte_index]);
            }
        }
        shards[data_index] = Some(reconstructed);
    }

    Ok(())
}

pub(super) fn reconstruct_all_shards(
    shards: &mut [Option<Vec<u8>>],
    profile: LocalErasureProfile,
) -> Result<()> {
    reconstruct_data_shards(shards, profile)?;
    let shard_len = shards
        .iter()
        .find_map(|shard| shard.as_ref().map(Vec::len))
        .ok_or_else(|| anyhow!("CoreStore erasure reconstruction has no shards"))?;
    for parity_row in 0..profile.parity_shards {
        let parity_index = profile.data_shards + parity_row;
        if shards[parity_index].is_some() {
            continue;
        }
        let mut reconstructed = vec![0u8; shard_len];
        for data_index in 0..profile.data_shards {
            let data = shards[data_index]
                .as_ref()
                .ok_or_else(|| anyhow!("CoreStore erasure reconstruction left missing data"))?;
            let coefficient = gf_pow((data_index + 1) as u8, parity_row as u32);
            for byte_index in 0..shard_len {
                reconstructed[byte_index] ^= gf_mul(coefficient, data[byte_index]);
            }
        }
        shards[parity_index] = Some(reconstructed);
    }
    Ok(())
}

pub(super) fn erasure_coding_row(shard_index: usize, data_shards: usize) -> Vec<u8> {
    if shard_index < data_shards {
        let mut row = vec![0u8; data_shards];
        row[shard_index] = 1;
        return row;
    }
    let parity_row = shard_index - data_shards;
    (0..data_shards)
        .map(|data_index| gf_pow((data_index + 1) as u8, parity_row as u32))
        .collect()
}

pub(super) fn invert_gf256_matrix(matrix: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
    let n = matrix.len();
    if n == 0 {
        bail!("CoreStore cannot invert an empty erasure matrix");
    }
    if matrix.iter().any(|row| row.len() != n) {
        bail!("CoreStore erasure matrix must be square");
    }

    let mut augmented = vec![vec![0u8; n * 2]; n];
    for row in 0..n {
        augmented[row][..n].copy_from_slice(&matrix[row]);
        augmented[row][n + row] = 1;
    }

    for col in 0..n {
        let pivot = (col..n)
            .find(|row| augmented[*row][col] != 0)
            .ok_or_else(|| anyhow!("CoreStore erasure matrix is singular"))?;
        if pivot != col {
            augmented.swap(pivot, col);
        }
        let inv_pivot = gf_inv(augmented[col][col])?;
        for value in &mut augmented[col] {
            *value = gf_mul(*value, inv_pivot);
        }
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = augmented[row][col];
            if factor == 0 {
                continue;
            }
            for idx in 0..(n * 2) {
                augmented[row][idx] ^= gf_mul(factor, augmented[col][idx]);
            }
        }
    }

    Ok(augmented.into_iter().map(|row| row[n..].to_vec()).collect())
}

pub(super) fn gf_pow(value: u8, exponent: u32) -> u8 {
    let mut acc = 1u8;
    for _ in 0..exponent {
        acc = gf_mul(acc, value);
    }
    acc
}

pub(super) fn gf_inv(value: u8) -> Result<u8> {
    if value == 0 {
        bail!("CoreStore cannot invert zero in GF(2^8)");
    }
    Ok(gf_pow(value, 254))
}

pub(super) fn gf_mul(mut lhs: u8, mut rhs: u8) -> u8 {
    let mut acc = 0u8;
    for _ in 0..8 {
        if rhs & 1 != 0 {
            acc ^= lhs;
        }
        let carry = lhs & 0x80 != 0;
        lhs <<= 1;
        if carry {
            lhs ^= 0x1d;
        }
        rhs >>= 1;
    }
    acc
}

pub(super) fn required_data_shard_indices_for_range(
    logical_size: u64,
    data_shards: usize,
    range: &CoreByteRange,
) -> Result<BTreeSet<u16>> {
    if data_shards == 0 {
        bail!("CoreStore range read requires at least one data shard");
    }
    if range.start > range.end_exclusive {
        bail!("CoreStore range start must be <= end_exclusive");
    }
    if range.end_exclusive > logical_size {
        bail!("CoreStore range end_exclusive exceeds logical object size");
    }

    let shard_len = logical_size.div_ceil(data_shards as u64).max(1);
    let mut indices = BTreeSet::new();
    for shard_index in 0..data_shards {
        let shard_start = shard_index as u64 * shard_len;
        let shard_end = (shard_start + shard_len).min(logical_size);
        if range.start.max(shard_start) < range.end_exclusive.min(shard_end) {
            indices.insert(shard_index as u16);
        }
    }
    Ok(indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconstructs_missing_data_and_parity_shards() {
        let source = b"distributed erasure recovery must restore every placement";
        let encoded = encode_erasure_shards(source, LOCAL_EC_4_2_PROFILE).unwrap();
        let mut degraded = encoded.iter().cloned().map(Some).collect::<Vec<_>>();
        degraded[1] = None;
        degraded[5] = None;

        reconstruct_all_shards(&mut degraded, LOCAL_EC_4_2_PROFILE).unwrap();

        assert_eq!(
            degraded.into_iter().map(Option::unwrap).collect::<Vec<_>>(),
            encoded
        );
    }

    #[test]
    fn refuses_repair_below_read_quorum() {
        let encoded = encode_erasure_shards(b"too few shards", LOCAL_EC_4_2_PROFILE).unwrap();
        let mut degraded = encoded.into_iter().map(Some).collect::<Vec<_>>();
        degraded[0] = None;
        degraded[1] = None;
        degraded[2] = None;

        let error = reconstruct_all_shards(&mut degraded, LOCAL_EC_4_2_PROFILE).unwrap_err();
        assert!(error.to_string().contains("fewer than 4 readable shards"));
    }
}
