//! Public-API writer for bounded three-node membership-cutover qualification.

use std::env;
use std::error::Error;

use keldra_storage::v1::bulk_operation::Operation as BulkOperationValue;
use keldra_storage::v1::bulk_outcome::Outcome as BulkOutcomeValue;
use keldra_storage::v1::{
    BulkOperation, BulkPutRequest, BulkWriteRequest, Durability, ObjectAddress,
};
use keldra_storage::{connect_channel, exchange_client_credentials, object_client};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;
const MAX_BULK_ITEMS: usize = 1_000;

#[tokio::main(flavor = "current_thread")]
async fn main() -> TestResult<()> {
    let endpoint = required("KELDRA_CUTOVER_QUALIFICATION_ENDPOINT")?;
    let tenant = required("KELDRA_CUTOVER_QUALIFICATION_TENANT")?;
    let bucket = required("KELDRA_CUTOVER_QUALIFICATION_BUCKET")?;
    let phase = required("KELDRA_CUTOVER_QUALIFICATION_PHASE")?;
    let first = decimal("KELDRA_CUTOVER_QUALIFICATION_FIRST")?;
    let count = positive_decimal("KELDRA_CUTOVER_QUALIFICATION_COUNT")?;
    let channel = connect_channel(&endpoint).await?;
    let token = exchange_client_credentials(
        channel.clone(),
        required("KELDRA_CUTOVER_QUALIFICATION_CLIENT_ID")?,
        required("KELDRA_CUTOVER_QUALIFICATION_CLIENT_SECRET")?,
    )
    .await?
    .access_token;
    let mut client = object_client(channel, &token)?;

    let end = first
        .checked_add(count)
        .ok_or("cutover qualification range overflowed")?;
    for batch_first in (first..end).step_by(MAX_BULK_ITEMS) {
        let batch_end = end.min(batch_first.saturating_add(MAX_BULK_ITEMS as u64));
        let operations = (batch_first..batch_end)
            .map(|position| {
                let command_id = format!("membership-cutover-{phase}-{position}");
                BulkOperation {
                    operation: Some(BulkOperationValue::Put(BulkPutRequest {
                        address: Some(ObjectAddress {
                            tenant: tenant.clone(),
                            bucket: bucket.clone(),
                            path: format!("membership-cutover/{phase}-{position}.bin"),
                        }),
                        bytes: b"x".to_vec(),
                        content_type: "application/octet-stream".into(),
                        command_id,
                        durability: Durability::Local as i32,
                    })),
                }
            })
            .collect::<Vec<_>>();
        let expected = operations.len();
        let response = client
            .bulk_write(BulkWriteRequest { operations })
            .await?
            .into_inner();
        if response.outcomes.len() != expected {
            return Err(format!(
                "BulkWrite returned {} outcomes for {expected} cutover operations",
                response.outcomes.len()
            )
            .into());
        }
        for (expected_index, outcome) in response.outcomes.into_iter().enumerate() {
            let position = batch_first
                .checked_add(expected_index as u64)
                .ok_or("cutover qualification outcome position overflowed")?;
            let expected_command_id = format!("membership-cutover-{phase}-{position}");
            if outcome.index as usize != expected_index {
                return Err(format!(
                    "BulkWrite outcome {} was returned at position {expected_index}",
                    outcome.index
                )
                .into());
            }
            match outcome.outcome {
                Some(BulkOutcomeValue::Receipt(receipt))
                    if receipt.command_id == expected_command_id
                        && receipt.version != 0
                        && !receipt.deleted => {}
                Some(BulkOutcomeValue::Failure(failure)) => {
                    return Err(format!(
                        "cutover BulkWrite item {position} failed with code {}: {}",
                        failure.code, failure.message
                    )
                    .into());
                }
                Some(BulkOutcomeValue::Receipt(receipt)) => {
                    return Err(format!(
                        "cutover BulkWrite item {position} returned invalid receipt command={} version={} deleted={}",
                        receipt.command_id, receipt.version, receipt.deleted
                    )
                    .into());
                }
                None => {
                    return Err(
                        format!("cutover BulkWrite item {position} returned no outcome").into(),
                    );
                }
            }
        }
    }
    println!("committed {count} cutover writes from position {first}");
    Ok(())
}

fn required(name: &str) -> TestResult<String> {
    let value = env::var(name).map_err(|_| format!("{name} is required"))?;
    if value.is_empty() {
        return Err(format!("{name} must not be empty").into());
    }
    Ok(value)
}

fn decimal(name: &str) -> TestResult<u64> {
    let value = required(name)?;
    value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an unsigned decimal integer").into())
}

fn positive_decimal(name: &str) -> TestResult<u64> {
    let value = decimal(name)?;
    if value == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(value)
}
