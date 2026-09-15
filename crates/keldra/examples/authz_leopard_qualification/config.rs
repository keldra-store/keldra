use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Phase {
    Full,
    PrepareRebuild,
    VerifyRebuild,
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct Config {
    pub endpoints: Vec<String>,
    #[serde(skip_serializing)]
    pub client_id: String,
    #[serde(skip_serializing)]
    pub client_secret: String,
    pub storage_tenant: String,
    pub realm: String,
    pub schema_id: String,
    pub roots: usize,
    pub depth: usize,
    pub fanout: usize,
    pub checks_per_batch: usize,
    pub benchmark_batches: usize,
    pub max_in_flight_batches: usize,
    pub phase: Phase,
    pub state_path: PathBuf,
    pub resource_evidence_path: Option<PathBuf>,
    pub require_internal_telemetry: bool,
}

impl Config {
    pub(super) fn from_env() -> Result<Self> {
        let phase = match optional("KELDRA_AUTHZ_LEOPARD_PHASE").as_deref() {
            None | Some("full") => Phase::Full,
            Some("prepare-rebuild") => Phase::PrepareRebuild,
            Some("verify-rebuild") => Phase::VerifyRebuild,
            Some(value) => bail!("unknown Leopard qualification phase {value:?}"),
        };
        let config = Self {
            endpoints: required("KELDRA_AUTHZ_LEOPARD_ENDPOINTS")?
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect(),
            client_id: required("KELDRA_AUTHZ_LEOPARD_CLIENT_ID")?,
            client_secret: required("KELDRA_AUTHZ_LEOPARD_CLIENT_SECRET")?,
            storage_tenant: required("KELDRA_AUTHZ_LEOPARD_TENANT")?,
            realm: optional("KELDRA_AUTHZ_LEOPARD_REALM")
                .unwrap_or_else(|| "leopard-qualification".into()),
            schema_id: optional("KELDRA_AUTHZ_LEOPARD_SCHEMA_ID")
                .unwrap_or_else(|| "leopard-qualification-v1".into()),
            roots: number("KELDRA_AUTHZ_LEOPARD_ROOTS", 16)?,
            depth: number("KELDRA_AUTHZ_LEOPARD_DEPTH", 5)?,
            fanout: number("KELDRA_AUTHZ_LEOPARD_FANOUT", 4)?,
            checks_per_batch: number("KELDRA_AUTHZ_LEOPARD_CHECKS_PER_BATCH", 1_000)?,
            benchmark_batches: number("KELDRA_AUTHZ_LEOPARD_BENCHMARK_BATCHES", 10_000)?,
            max_in_flight_batches: number("KELDRA_AUTHZ_LEOPARD_MAX_IN_FLIGHT", 32)?,
            phase,
            state_path: PathBuf::from(
                optional("KELDRA_AUTHZ_LEOPARD_STATE_PATH")
                    .unwrap_or_else(|| "authz-leopard-qualification-state.json".into()),
            ),
            resource_evidence_path: optional("KELDRA_AUTHZ_LEOPARD_RESOURCE_EVIDENCE")
                .map(PathBuf::from),
            require_internal_telemetry: boolean(
                "KELDRA_AUTHZ_LEOPARD_REQUIRE_INTERNAL_TELEMETRY",
                true,
            )?,
        };
        ensure!(
            matches!(config.endpoints.len(), 1 | 3),
            "Leopard qualification requires either one or three endpoints"
        );
        ensure!(config.roots > 0, "roots must be nonzero");
        ensure!((1..=8).contains(&config.depth), "depth must be in 1..=8");
        ensure!(
            (2..=16).contains(&config.fanout),
            "fanout must be in 2..=16"
        );
        ensure!(
            (1..=1_000).contains(&config.checks_per_batch),
            "checks per batch must be in 1..=1000"
        );
        ensure!(
            config.benchmark_batches > 0,
            "benchmark batches must be nonzero"
        );
        ensure!(
            config.max_in_flight_batches > 0,
            "max in-flight batches must be nonzero"
        );
        Ok(config)
    }
}

fn required(name: &str) -> Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}

fn optional(name: &str) -> Option<String> {
    env::var(name).ok().filter(|value| !value.trim().is_empty())
}

fn number(name: &str, default: usize) -> Result<usize> {
    optional(name)
        .map(|value| {
            value
                .parse()
                .with_context(|| format!("{name} is not an integer"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn boolean(name: &str, default: bool) -> Result<bool> {
    optional(name)
        .map(|value| match value.as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => bail!("{name} must be true, false, 1, or 0"),
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_names_are_explicit() {
        assert_eq!(Phase::Full, Phase::Full);
        assert_eq!(Phase::PrepareRebuild, Phase::PrepareRebuild);
        assert_eq!(Phase::VerifyRebuild, Phase::VerifyRebuild);
    }
}
