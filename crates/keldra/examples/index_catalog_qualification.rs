//! Public-API qualification for a large logical index catalogue.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use keldra_storage::v1::{GetIndexRequest, ListIndicesRequest};
use keldra_storage::{
    KeywordField, TypedJsonIndexBuilder, connect_channel, exchange_client_credentials, index_client,
};
use serde::Serialize;
use tokio::task::JoinSet;

const CONTENT_TYPE: &str = "application/vnd.keldra.catalog-scale+json";
const TOKEN_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Phase {
    Create,
    Verify,
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(long)]
    endpoint: String,
    #[arg(long, env = "KELDRA_CLIENT_ID")]
    client_id: String,
    #[arg(long, env = "KELDRA_CLIENT_SECRET", hide_env_values = true)]
    client_secret: String,
    #[arg(long)]
    bucket: String,
    #[arg(long, default_value_t = 250_000)]
    definitions: usize,
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    #[arg(long, value_enum)]
    phase: Phase,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    phase: &'static str,
    completed_unix_milliseconds: u128,
    definitions: usize,
    concurrency: usize,
    elapsed_seconds: f64,
    definitions_per_second: f64,
    listed: usize,
    sampled_gets: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arc::new(Args::parse());
    ensure!(args.definitions > 0, "definitions must be positive");
    ensure!(args.concurrency > 0, "concurrency must be positive");
    let started = Instant::now();
    let (listed, sampled_gets) = match args.phase {
        Phase::Create => {
            create_definitions(args.clone()).await?;
            (0, 0)
        }
        Phase::Verify => verify_definitions(&args).await?,
    };
    let elapsed_seconds = started.elapsed().as_secs_f64();
    let report = Report {
        schema: "keldra.index-catalog-qualification.v1",
        phase: match args.phase {
            Phase::Create => "create",
            Phase::Verify => "verify",
        },
        completed_unix_milliseconds: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
        definitions: args.definitions,
        concurrency: args.concurrency,
        elapsed_seconds,
        definitions_per_second: args.definitions as f64 / elapsed_seconds,
        listed,
        sampled_gets,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    if let Some(output) = &args.output {
        std::fs::write(output, &encoded).with_context(|| format!("write {}", output.display()))?;
    }
    println!("{}", String::from_utf8(encoded)?);
    Ok(())
}

async fn create_definitions(args: Arc<Args>) -> Result<()> {
    let next = Arc::new(AtomicUsize::new(0));
    let workers = args.concurrency.min(args.definitions);
    let mut tasks = JoinSet::new();
    for _ in 0..workers {
        let args = args.clone();
        let next = next.clone();
        tasks.spawn(async move {
            let channel = connect_channel(&args.endpoint)
                .await
                .map_err(|error| anyhow::anyhow!("connect to {}: {error}", args.endpoint))?;
            let mut refreshed_at = Instant::now()
                .checked_sub(TOKEN_REFRESH_INTERVAL)
                .unwrap_or_else(Instant::now);
            let mut client = None;
            loop {
                let ordinal = next.fetch_add(1, Ordering::Relaxed);
                if ordinal >= args.definitions {
                    return Ok::<(), anyhow::Error>(());
                }
                if client.is_none() || refreshed_at.elapsed() >= TOKEN_REFRESH_INTERVAL {
                    let token = exchange_client_credentials(
                        channel.clone(),
                        args.client_id.clone(),
                        args.client_secret.clone(),
                    )
                    .await?
                    .access_token;
                    client = Some(index_client(channel.clone(), &token)?);
                    refreshed_at = Instant::now();
                }
                let name = index_name(ordinal);
                let request = TypedJsonIndexBuilder::new(&args.bucket, &name)
                    .path_prefix("catalog-scale/")
                    .content_type(CONTENT_TYPE)
                    .field(KeywordField::single("category", "/category").exact())
                    .finish(format!("catalog-scale-create-{ordinal}"))?;
                let definition = client
                    .as_mut()
                    .expect("client installed")
                    .create_index(request)
                    .await
                    .with_context(|| format!("create {name}"))?
                    .into_inner();
                ensure!(
                    definition.name == name,
                    "created definition identity changed"
                );
                ensure!(definition.index_id != 0, "created definition has zero ID");
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.context("catalog creation worker panicked")??;
    }
    Ok(())
}

async fn verify_definitions(args: &Args) -> Result<(usize, usize)> {
    let channel = connect_channel(&args.endpoint)
        .await
        .map_err(|error| anyhow::anyhow!("connect to {}: {error}", args.endpoint))?;
    let token = exchange_client_credentials(
        channel.clone(),
        args.client_id.clone(),
        args.client_secret.clone(),
    )
    .await?
    .access_token;
    let mut client = index_client(channel, &token)?;
    let mut after = None;
    let mut listed = 0usize;
    loop {
        let response = client
            .list_indices(ListIndicesRequest {
                bucket: args.bucket.clone(),
                start_after_name: after.clone(),
                limit: 1_000,
            })
            .await?
            .into_inner();
        ensure!(!response.indices.is_empty() || !response.has_more);
        for definition in &response.indices {
            ensure!(definition.name == index_name(listed));
            ensure!(definition.index_id != 0);
            listed += 1;
        }
        if !response.has_more {
            break;
        }
        after = response.indices.last().map(|value| value.name.clone());
        ensure!(after.is_some(), "catalog page did not advance");
    }
    ensure!(listed == args.definitions, "catalog cardinality changed");

    let samples = [0, args.definitions / 2, args.definitions - 1];
    for ordinal in samples {
        let name = index_name(ordinal);
        let definition = client
            .get_index(GetIndexRequest {
                bucket: args.bucket.clone(),
                name: name.clone(),
            })
            .await?
            .into_inner();
        ensure!(definition.name == name);
        ensure!(definition.index_id != 0);
    }
    Ok((listed, samples.len()))
}

fn index_name(ordinal: usize) -> String {
    format!("catalog-{ordinal:06}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_sort_in_creation_order_at_the_scale_gate() {
        assert_eq!(index_name(0), "catalog-000000");
        assert_eq!(index_name(249_999), "catalog-249999");
        assert!(index_name(99_999) < index_name(100_000));
    }
}
