use super::*;

pub(super) fn validate_and_pin(args: Args) -> Result<(RuntimeConfig, String, String)> {
    ensure!(
        args.confirm_clean_target,
        "--confirm-clean-target is required because this command writes the supplied tenant/bucket"
    );
    validate_canonical("tenant", &args.tenant)?;
    validate_canonical("bucket", &args.bucket)?;
    ensure!(
        args.bucket == OSV_QUALIFICATION_BUCKET,
        "the OSV qualification bucket must be {OSV_QUALIFICATION_BUCKET}"
    );
    validate_snapshot_day(&args.snapshot_day)?;
    validate_git_commit("--anvil-commit", &args.anvil_commit)?;
    validate_canonical("client ID", &args.client_id)?;
    ensure!(
        args.source_url.starts_with("https://")
            && args.source_url.trim() == args.source_url
            && !args.source_url.chars().any(char::is_whitespace),
        "--source-url must be a canonical HTTPS URL"
    );
    ensure!(
        (1..=168).contains(&args.source_cadence_hours),
        "--source-cadence-hours must be between 1 and 168"
    );
    ensure!(
        matches!(args.durability, DurabilityArgument::Local),
        "the Anvil 0.8 OSV qualification requires --durability local"
    );
    ensure!(
        (1..=SERVER_MAX_BULK_ITEMS).contains(&args.batch_size),
        "--batch-size must be between 1 and {SERVER_MAX_BULK_ITEMS}"
    );
    ensure!(
        (1..=SERVER_MAX_BULK_ENCODED_BYTES).contains(&args.maximum_batch_payload_bytes),
        "--maximum-batch-payload-bytes must be between 1 and {SERVER_MAX_BULK_ENCODED_BYTES}"
    );
    ensure!(
        (MIN_SHARD_UNCOMPRESSED_BYTES..=MAX_SHARD_UNCOMPRESSED_BYTES)
            .contains(&args.shard_uncompressed_bytes),
        "--shard-uncompressed-bytes must be between {MIN_SHARD_UNCOMPRESSED_BYTES} and {MAX_SHARD_UNCOMPRESSED_BYTES}"
    );
    ensure!(
        (1..=16).contains(&args.concurrency),
        "--concurrency must be between 1 and 16"
    );
    ensure!(
        args.verification_concurrency > 0,
        "--verification-concurrency must be non-zero"
    );
    validate_sha256("--corpus-sha256", &args.corpus_sha256)?;
    let corpus = args
        .corpus
        .canonicalize()
        .with_context(|| format!("canonicalise corpus path {}", args.corpus.display()))?;
    let metadata = corpus
        .metadata()
        .with_context(|| format!("stat corpus path {}", corpus.display()))?;
    ensure!(metadata.is_file(), "corpus path must name a regular file");
    let observed = sha256_file(&corpus)?;
    ensure!(
        observed == args.corpus_sha256,
        "corpus SHA-256 mismatch: expected {}, observed {observed}",
        args.corpus_sha256
    );
    let client_secret = read_client_secret(&args.client_secret_file)?;
    let snapshot_id = format!("osv-{}-{}", args.snapshot_day, &observed[..24]);
    let (write_endpoints, write_node_count) =
        resolve_write_endpoints(&args.endpoint, args.write_endpoints, args.concurrency)?;
    Ok((
        RuntimeConfig {
            endpoint: args.endpoint,
            write_endpoints,
            write_node_count,
            tenant: args.tenant,
            bucket: args.bucket,
            corpus_path_display: corpus.display().to_string(),
            corpus,
            corpus_sha256: observed,
            corpus_bytes: metadata.len(),
            snapshot_day: args.snapshot_day,
            snapshot_id,
            anvil_commit: args.anvil_commit,
            source_url: args.source_url,
            source_cadence_hours: args.source_cadence_hours,
            durability: args.durability.api_value(),
            durability_name: args.durability.name(),
            batch_size: args.batch_size,
            maximum_batch_payload_bytes: args.maximum_batch_payload_bytes,
            shard_uncompressed_bytes: args.shard_uncompressed_bytes,
            concurrency: args.concurrency,
            verification_concurrency: args.verification_concurrency,
            output: args.output,
            auth: None,
        },
        args.client_id,
        client_secret,
    ))
}

pub(super) fn resolve_write_endpoints(
    primary_endpoint: &str,
    write_endpoints: Vec<String>,
    default_concurrency: usize,
) -> Result<(Vec<String>, u8)> {
    if write_endpoints.is_empty() {
        ensure!(default_concurrency > 0, "default write concurrency is zero");
        return Ok((vec![primary_endpoint.to_owned(); default_concurrency], 1));
    }
    let distinct = write_endpoints.iter().collect::<BTreeSet<_>>();
    ensure!(
        distinct.len() == write_endpoints.len(),
        "--write-endpoint values must be distinct"
    );
    let node_count = u8::try_from(write_endpoints.len())
        .context("more than 255 --write-endpoint values were supplied")?;
    Ok((write_endpoints, node_count))
}

fn validate_canonical(name: &str, value: &str) -> Result<()> {
    ensure!(
        !value.is_empty() && value.trim() == value,
        "{name} must be non-empty and have no surrounding whitespace"
    );
    ensure!(!value.contains('\0'), "{name} must not contain NUL");
    Ok(())
}

pub(super) fn validate_snapshot_day(value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    ensure!(
        bytes.len() == 10
            && bytes[4] == b'-'
            && bytes[7] == b'-'
            && bytes
                .iter()
                .enumerate()
                .all(|(index, byte)| matches!(index, 4 | 7) || byte.is_ascii_digit()),
        "--snapshot-day must use YYYY-MM-DD"
    );
    Ok(())
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{name} must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}

pub(super) fn validate_git_commit(name: &str, value: &str) -> Result<()> {
    ensure!(
        value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{name} must be an exact 40-character hexadecimal commit ID"
    );
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .with_context(|| format!("read {}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn read_client_secret(path: &Path) -> Result<String> {
    let metadata = std::fs::symlink_metadata(path)
        .with_context(|| format!("stat client secret file {}", path.display()))?;
    ensure!(
        metadata.file_type().is_file(),
        "client secret path must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        ensure!(
            metadata.permissions().mode() & 0o7777 == 0o600,
            "client secret file must have mode 0600"
        );
    }
    ensure!(
        metadata.len() <= 4 * 1024,
        "client secret file exceeds 4096 bytes"
    );
    let secret = std::fs::read_to_string(path)
        .with_context(|| format!("read client secret file {}", path.display()))?;
    let secret = secret.trim();
    validate_client_secret_value(secret)?;
    Ok(secret.to_owned())
}

pub(super) fn validate_client_secret_value(secret: &str) -> Result<()> {
    ensure!(
        (32..=4 * 1024).contains(&secret.len()),
        "client secret must contain between 32 and 4096 UTF-8 bytes"
    );
    Ok(())
}
