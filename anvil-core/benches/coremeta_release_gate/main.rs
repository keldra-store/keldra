mod config;
mod dataset;
mod report;
mod runner;

use std::time::{Instant, SystemTime};

use anyhow::{Result, bail};

use crate::config::{Cli, LoadedConfig};
use crate::report::GateReport;

fn main() {
    if let Err(error) = execute() {
        eprintln!("[coremeta-perf-gate] failed: {error:#}");
        std::process::exit(1);
    }
}

fn execute() -> Result<()> {
    let cli = Cli::parse()?;
    let config = LoadedConfig::load(&cli)?;
    let started_at = SystemTime::now();
    let timer = Instant::now();

    eprintln!(
        "[coremeta-perf-gate] profile={} small_rows={} large_rows={} page_size={}",
        config.profile_name,
        config.profile.small_rows,
        config.profile.large_rows,
        config.profile.page_size
    );
    let result = runner::run(&config.manifest, &config.profile)?;
    let report = GateReport::new(
        &config.profile_name,
        &config.manifest,
        &config.manifest_hash,
        &config.manifest_sha256,
        config.profile.clone(),
        started_at,
        timer.elapsed(),
        result.scenarios,
        result.gates,
    );
    report.write(&cli.output_path, &config.manifest_bytes)?;

    for gate in &report.gates {
        eprintln!(
            "[coremeta-perf-gate] gate={} pass={} observed={:?} threshold={} {}",
            gate.name, gate.pass, gate.observed, gate.effective_threshold, gate.unit
        );
    }
    eprintln!(
        "[coremeta-perf-gate] report={} pass={}",
        cli.output_path.display(),
        report.pass
    );
    if !report.pass {
        let failed = report
            .gates
            .iter()
            .filter(|gate| !gate.pass)
            .map(|gate| gate.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("performance release gates failed: {failed}");
    }
    Ok(())
}
