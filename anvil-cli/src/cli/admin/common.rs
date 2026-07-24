use anvil::anvil_api as api;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use clap::Args;
use serde::Serialize;
use serde_json::{Value, json};
use std::future::Future;
use std::io::Write;

pub(super) type AdminClient = AdminServiceClient<tonic::transport::Channel>;

#[derive(Args, Debug, Clone)]
pub struct MutationOptions {
    /// AdminRequestContext.request_id. Defaults to a generated UUID.
    #[clap(long)]
    pub(super) request_id: Option<String>,
    /// AdminRequestContext.idempotency_key. Defaults to a generated UUID.
    #[clap(long)]
    pub(super) idempotency_key: Option<String>,
    /// AdminRequestContext.audit_reason. Required for all mutations.
    #[clap(long)]
    pub(super) audit_reason: String,
    /// AdminRequestContext.expected_generation. Required for update/delete commands; create/register commands default to 0.
    #[clap(long)]
    pub(super) expected_generation: Option<u64>,
}
impl MutationOptions {
    pub(super) fn to_create_context(&self) -> anyhow::Result<api::AdminRequestContext> {
        let expected_generation = self.expected_generation.unwrap_or(0);
        if expected_generation != 0 {
            anyhow::bail!(
                "create/register commands must use --expected-generation 0 when supplied"
            );
        }
        Ok(self.context_with_generation(expected_generation))
    }

    pub(super) fn to_update_context(&self) -> anyhow::Result<api::AdminRequestContext> {
        let Some(expected_generation) = self.expected_generation else {
            anyhow::bail!("--expected-generation is required for update/delete lifecycle commands");
        };
        if expected_generation == 0 {
            anyhow::bail!("update/delete commands must use a non-zero --expected-generation");
        }
        Ok(self.context_with_generation(expected_generation))
    }

    pub(super) fn to_action_context(&self) -> api::AdminRequestContext {
        self.context_with_generation(self.expected_generation.unwrap_or(0))
    }

    fn context_with_generation(&self, expected_generation: u64) -> api::AdminRequestContext {
        api::AdminRequestContext {
            request_id: self
                .request_id
                .clone()
                .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4())),
            idempotency_key: self
                .idempotency_key
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            audit_reason: self.audit_reason.clone(),
            expected_generation,
        }
    }
}
#[derive(Args, Debug, Clone, Default)]
pub struct PageOptions {
    #[clap(long)]
    pub(super) page_token: Option<String>,
    #[clap(long)]
    pub(super) page_size: Option<u32>,
}
impl PageOptions {
    pub(super) fn to_page_request(&self) -> Option<api::PageRequest> {
        if self.page_token.is_none() && self.page_size.is_none() {
            return None;
        }

        Some(api::PageRequest {
            page_token: self.page_token.clone().unwrap_or_default(),
            page_size: self.page_size.unwrap_or_default(),
        })
    }
}
pub(super) fn with_auth<T>(message: T, token: &str) -> anyhow::Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().map_err(|err| {
            anyhow::anyhow!("failed to build authorization metadata header: {err}")
        })?,
    );
    Ok(request)
}
pub(super) fn request_id_or_cli(request_id: &Option<String>) -> String {
    request_id
        .clone()
        .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4()))
}
pub(super) async fn print_rpc_response<T, F>(
    resource_type: &'static str,
    context: Option<&api::AdminRequestContext>,
    request_id: Option<&str>,
    rpc: F,
) -> anyhow::Result<()>
where
    T: Serialize,
    F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match rpc.await {
        Ok(response) => print_admin_success(resource_type, &response.into_inner(), context),
        Err(status) => {
            print_admin_error(resource_type, context, request_id, &status)?;
            Err(status.into())
        }
    }
}
#[derive(Serialize)]
struct AdminCliJsonOutput {
    schema: &'static str,
    request_id: String,
    ok: bool,
    resource_type: String,
    resource: Option<Value>,
    generation: Option<u64>,
    audit_event_id: String,
    idempotency_key: Option<String>,
    error: Option<AdminCliJsonError>,
}
#[derive(Serialize)]
struct AdminCliJsonError {
    request_id: String,
    code: String,
    message: String,
    resource_id: String,
    current_generation: u64,
}
fn print_admin_success<T: Serialize>(
    resource_type: &'static str,
    value: &T,
    context: Option<&api::AdminRequestContext>,
) -> anyhow::Result<()> {
    let value = serde_json::to_value(value)?;
    let resource = admin_cli_resource(&value);
    let request_id = value
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| context.map(|context| context.request_id.as_str()))
        .unwrap_or_default()
        .to_string();
    let audit_event_id = value
        .get("audit_event_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let generation = value.get("generation").and_then(Value::as_u64).or_else(|| {
        resource
            .as_ref()
            .and_then(|resource| resource.get("generation"))
            .and_then(Value::as_u64)
    });
    let output = AdminCliJsonOutput {
        schema: "anvil.admin_cli.output.v1",
        request_id,
        ok: true,
        resource_type: resource_type.to_string(),
        resource,
        generation,
        audit_event_id,
        idempotency_key: context.map(|context| context.idempotency_key.clone()),
        error: None,
    };
    print_json(&output)
}
fn print_admin_error(
    resource_type: &'static str,
    context: Option<&api::AdminRequestContext>,
    request_id: Option<&str>,
    status: &tonic::Status,
) -> anyhow::Result<()> {
    let request_id = request_id
        .filter(|value| !value.is_empty())
        .or_else(|| context.map(|context| context.request_id.as_str()))
        .unwrap_or_default()
        .to_string();
    let output = AdminCliJsonOutput {
        schema: "anvil.admin_cli.output.v1",
        request_id: request_id.clone(),
        ok: false,
        resource_type: resource_type.to_string(),
        resource: None,
        generation: None,
        audit_event_id: String::new(),
        idempotency_key: context.map(|context| context.idempotency_key.clone()),
        error: Some(AdminCliJsonError {
            request_id,
            code: format!("{:?}", status.code()),
            message: status.message().to_string(),
            resource_id: String::new(),
            current_generation: 0,
        }),
    };
    print_json(&output)
}
fn admin_cli_resource(value: &Value) -> Option<Value> {
    for field in [
        "tenant",
        "bucket",
        "link",
        "host_alias",
        "region",
        "cell",
        "node",
    ] {
        if let Some(resource) = value.get(field) {
            return Some(resource.clone());
        }
    }
    if let Some(resource_id) = value.get("resource_id") {
        return Some(json!({
            "resource_id": resource_id,
            "generation": value.get("generation").cloned().unwrap_or(Value::Null),
            "idempotent_replay": value
                .get("idempotent_replay")
                .cloned()
                .unwrap_or(Value::Bool(false)),
        }));
    }
    Some(value.clone())
}
fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value)?;
    writeln!(stdout)?;
    Ok(())
}
pub(super) fn required_part<'a>(part: Option<&'a str>, name: &str) -> Result<&'a str, String> {
    part.filter(|value| !value.is_empty())
        .ok_or_else(|| format!("bucket override is missing {name}"))
}
pub(super) fn normalize_enum_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(char::to_lowercase)
        .collect()
}
