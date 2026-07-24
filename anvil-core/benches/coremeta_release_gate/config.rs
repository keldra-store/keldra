use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MANIFEST_SCHEMA: &str = "anvil.perf.coremeta_gate_manifest.v1";
pub const DEFAULT_MANIFEST: &str = "ops/perf/coremeta-release-gate.json";

#[derive(Debug)]
pub struct Cli {
    pub profile: String,
    pub manifest_path: PathBuf,
    pub output_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GateManifest {
    pub schema: String,
    pub dataset_id: String,
    pub seed: String,
    pub profiles: BTreeMap<String, ProfileSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProfileSpec {
    pub small_rows: u64,
    pub large_rows: u64,
    pub unrelated_rows: u64,
    pub batch_rows: usize,
    pub payload_bytes: usize,
    pub page_size: usize,
    pub scaling_page_size: usize,
    pub warmup_operations: usize,
    pub point_samples: usize,
    pub page_samples: usize,
    pub mutation_samples: usize,
    pub root_publication_warmup_operations: usize,
    pub root_publication_samples: usize,
    pub history_probe_warmup_operations: usize,
    pub history_probe_samples: usize,
    pub history_page_size: usize,
    pub history_max_page_bytes: u64,
    pub multi_page_generation_warmup_operations: usize,
    pub multi_page_generation_samples: usize,
    pub multi_page_generation_rows: usize,
    pub root_publication_history_rows_per_logical_mutation: usize,
    pub root_publication_history_fixed_rows: usize,
    pub thresholds: Thresholds,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Thresholds {
    pub point_get_p95_ms: f64,
    pub page_p95_floor_ms: f64,
    pub page_p95_point_multiplier: f64,
    pub durable_single_p95_ms: f64,
    pub transactional_head_p95_floor_ms: f64,
    pub transactional_head_single_multiplier: f64,
    pub point_work_growth_ratio: f64,
    pub deep_page_work_growth_ratio: f64,
    pub deep_to_early_work_ratio: f64,
    pub page_size_work_ratio_multiplier: f64,
    pub page_work_per_item: f64,
    pub page_fixed_work: f64,
    pub root_publication_work_growth_ratio: f64,
    pub root_publication_row_work_ratio_multiplier: f64,
    pub root_publication_work_per_mutation: f64,
    pub root_publication_fixed_work: f64,
    pub history_work_growth_ratio: f64,
    pub history_page_work_ratio_multiplier: f64,
    pub history_work_per_item: f64,
    pub history_fixed_work: f64,
}

#[derive(Debug)]
pub struct LoadedConfig {
    pub manifest: GateManifest,
    pub manifest_bytes: Vec<u8>,
    pub profile_name: String,
    pub profile: ProfileSpec,
    pub manifest_hash: String,
    pub manifest_sha256: String,
}

impl Cli {
    pub fn parse() -> Result<Self> {
        let mut profile = "quick".to_string();
        let mut manifest_path = PathBuf::from(DEFAULT_MANIFEST);
        let mut output_path = PathBuf::from("target/anvil/perf/coremeta/quick/report.json");
        let mut output_was_set = false;
        let mut args = std::env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--profile" => {
                    profile = required_value(&mut args, "--profile")?;
                }
                "--manifest" => {
                    manifest_path = PathBuf::from(required_value(&mut args, "--manifest")?);
                }
                "--output" => {
                    output_path = PathBuf::from(required_value(&mut args, "--output")?);
                    output_was_set = true;
                }
                // Cargo appends this marker when it launches a harness-free bench target.
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => bail!("unknown argument {arg:?}; use --help for usage"),
            }
        }

        if !matches!(profile.as_str(), "quick" | "release") {
            bail!("profile must be quick or release");
        }
        if !output_was_set {
            output_path =
                PathBuf::from(format!("target/anvil/perf/coremeta/{profile}/report.json"));
        }

        Ok(Self {
            profile,
            manifest_path,
            output_path,
        })
    }
}

impl LoadedConfig {
    pub fn load(cli: &Cli) -> Result<Self> {
        let bytes = std::fs::read(&cli.manifest_path)
            .with_context(|| format!("read {}", cli.manifest_path.display()))?;
        let manifest: GateManifest = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse {}", cli.manifest_path.display()))?;
        validate_manifest(&manifest)?;
        let profile = manifest
            .profiles
            .get(&cli.profile)
            .cloned()
            .with_context(|| format!("manifest has no {} profile", cli.profile))?;
        validate_profile(&cli.profile, &profile)?;
        let manifest_hash = format!("blake3:{}", blake3::hash(&bytes).to_hex());
        let manifest_sha256 = format!("sha256:{}", hex::encode(Sha256::digest(&bytes)));

        Ok(Self {
            manifest,
            manifest_bytes: bytes,
            profile_name: cli.profile.clone(),
            profile,
            manifest_hash,
            manifest_sha256,
        })
    }
}

fn required_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn validate_manifest(manifest: &GateManifest) -> Result<()> {
    if manifest.schema != MANIFEST_SCHEMA {
        bail!(
            "CoreMeta gate manifest schema must be {MANIFEST_SCHEMA}, got {}",
            manifest.schema
        );
    }
    if manifest.dataset_id.trim().is_empty() || manifest.seed.trim().is_empty() {
        bail!("CoreMeta gate manifest requires non-empty dataset_id and seed");
    }
    for required in ["quick", "release"] {
        if !manifest.profiles.contains_key(required) {
            bail!("CoreMeta gate manifest is missing the {required} profile");
        }
    }
    Ok(())
}

fn validate_profile(name: &str, profile: &ProfileSpec) -> Result<()> {
    if profile.small_rows == 0 || profile.large_rows <= profile.small_rows {
        bail!("{name}: large_rows must exceed a non-zero small_rows");
    }
    if profile.large_rows / profile.small_rows < 8 {
        bail!("{name}: large_rows must be at least 8x small_rows");
    }
    if profile.unrelated_rows == 0 {
        bail!("{name}: unrelated_rows must be non-zero");
    }
    if profile.batch_rows == 0 || profile.batch_rows > 65_536 {
        bail!("{name}: batch_rows must be between 1 and 65,536");
    }
    if profile.payload_bytes == 0 || profile.payload_bytes > 32 * 1024 {
        bail!("{name}: payload_bytes must be between 1 and 32 KiB");
    }
    if profile.page_size == 0
        || profile.scaling_page_size <= profile.page_size
        || profile.scaling_page_size > 4096
        || profile.scaling_page_size as u64 >= profile.small_rows
    {
        bail!("{name}: page sizes must be ordered, bounded, and smaller than the dataset");
    }
    if profile.point_samples < 100 || profile.page_samples < 100 || profile.mutation_samples < 20 {
        bail!("{name}: sample counts are too small for stable p95 evidence");
    }
    if profile.warmup_operations == 0 {
        bail!("{name}: warmup_operations must be non-zero");
    }
    if profile.root_publication_warmup_operations == 0 || profile.root_publication_samples < 20 {
        bail!("{name}: root publication needs a warmup and at least 20 samples");
    }
    let publication_generations = profile
        .root_publication_warmup_operations
        .checked_add(profile.root_publication_samples)
        .context("root publication operation count overflow")?;
    if profile.history_probe_warmup_operations == 0
        || profile.history_probe_samples < 20
        || profile.history_page_size == 0
        || profile.history_page_size > 4096
        || profile.history_page_size >= publication_generations
        || !(128 * 1024..=16 * 1024 * 1024).contains(&profile.history_max_page_bytes)
    {
        bail!(
            "{name}: history probes need a warmup, at least 20 samples, a 1 to 4096 item page smaller than the publication history, and a 128 KiB to 16 MiB byte limit"
        );
    }
    if profile.multi_page_generation_warmup_operations == 0
        || profile.multi_page_generation_samples < 3
        || profile.multi_page_generation_rows <= profile.page_size
        || profile.multi_page_generation_rows <= profile.history_page_size
        || profile.multi_page_generation_rows > 4096
    {
        bail!(
            "{name}: multi-page generation settings need a warmup, at least 3 samples, more rows than both page bounds, and at most 4096 rows"
        );
    }
    if !(1..=16).contains(&profile.root_publication_history_rows_per_logical_mutation)
        || !(1..=64).contains(&profile.root_publication_history_fixed_rows)
    {
        bail!(
            "{name}: root publication history amplification must use 1 to 16 rows per logical mutation and 1 to 64 fixed rows"
        );
    }

    let thresholds = &profile.thresholds;
    let values = [
        thresholds.point_get_p95_ms,
        thresholds.page_p95_floor_ms,
        thresholds.page_p95_point_multiplier,
        thresholds.durable_single_p95_ms,
        thresholds.transactional_head_p95_floor_ms,
        thresholds.transactional_head_single_multiplier,
        thresholds.point_work_growth_ratio,
        thresholds.deep_page_work_growth_ratio,
        thresholds.deep_to_early_work_ratio,
        thresholds.page_size_work_ratio_multiplier,
        thresholds.page_work_per_item,
        thresholds.page_fixed_work,
        thresholds.root_publication_work_growth_ratio,
        thresholds.root_publication_row_work_ratio_multiplier,
        thresholds.root_publication_work_per_mutation,
        thresholds.root_publication_fixed_work,
        thresholds.history_work_growth_ratio,
        thresholds.history_page_work_ratio_multiplier,
        thresholds.history_work_per_item,
        thresholds.history_fixed_work,
    ];
    if values
        .iter()
        .any(|value| !value.is_finite() || *value <= 0.0)
    {
        bail!("{name}: every threshold must be finite and positive");
    }
    Ok(())
}

fn print_help() {
    println!(
        "CoreMeta release gate\n\n\
         Usage: coremeta_release_gate [OPTIONS]\n\n\
         Options:\n\
           --profile <quick|release>  Dataset and sample profile [default: quick]\n\
           --manifest <path>          Gate manifest [default: {DEFAULT_MANIFEST}]\n\
           --output <path>            Machine-readable report path\n\
           -h, --help                 Print this help"
    );
}
