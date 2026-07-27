use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use clap::Subcommand;
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum AuthzCommands {
    Schema {
        #[clap(subcommand)]
        command: SchemaCommands,
    },
    Tuple {
        #[clap(subcommand)]
        command: TupleCommands,
    },
    Check {
        namespace: String,
        object_id: String,
        relation: String,
        subject_kind: String,
        subject_id: String,
    },
    ListObjects {
        namespace: String,
        relation: String,
        subject_kind: String,
        subject_id: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    ListSubjects {
        namespace: String,
        object_id: String,
        relation: String,
        subject_kind: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    Watch {
        namespace: String,
        #[clap(long, default_value_t = 0)]
        after_revision: u64,
    },
}

#[derive(Subcommand)]
pub enum SchemaCommands {
    Put {
        schema_id: String,
        namespace: String,
        schema_json: String,
        #[clap(long, default_value = "tenant schema update")]
        reason: String,
    },
    Bind {
        schema_id: String,
        schema_revision: u64,
        schema_digest: String,
        realm_id: String,
        #[clap(long)]
        expected_generation: Option<u64>,
        #[clap(long, default_value = "bind schema")]
        reason: String,
    },
    Get {
        schema_id: String,
        #[clap(long, default_value = "")]
        schema_revision: String,
    },
    Binding {
        realm_id: String,
    },
}

#[derive(Subcommand)]
pub enum TupleCommands {
    Write {
        namespace: String,
        object_id: String,
        relation: String,
        subject_kind: String,
        subject_id: String,
        operation: String,
        #[clap(long, default_value = "")]
        caveat_hash: String,
        #[clap(long, default_value = "tenant tuple update")]
        reason: String,
    },
    Read {
        namespace: String,
        #[clap(long, default_value = "")]
        object_id: String,
        #[clap(long, default_value = "")]
        relation: String,
        #[clap(long, default_value = "")]
        subject_kind: String,
        #[clap(long, default_value = "")]
        subject_id: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

pub async fn handle_authz_command(command: &AuthzCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = AuthServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    let tenant_id = crate::cli::object::decode_native_token_claims(&token)?
        .tenant_id
        .to_string();
    match command {
        AuthzCommands::Schema {
            command:
                SchemaCommands::Put {
                    schema_id,
                    namespace,
                    schema_json,
                    reason,
                },
        } => {
            let mut request = tonic::Request::new(api::PutAuthzSchemaRequest {
                context: None,
                anvil_storage_tenant_id: tenant_id,
                schema_id: schema_id.clone(),
                namespaces: vec![api::AuthzNamespaceSchema {
                    namespace: namespace.clone(),
                    relations: Vec::new(),
                    schema_json: schema_json.clone(),
                    schema_hash: String::new(),
                    schema_version: 0,
                    authz_revision: 0,
                    applied_at: String::new(),
                }],
                reason: reason.clone(),
            });
            add_auth(&mut request, &token);
            let response = client.put_authz_schema(request).await?.into_inner();
            let schema_ref = response.schema_ref.unwrap_or_default();
            println!(
                "{}\t{}\t{}",
                schema_ref.schema_id, schema_ref.schema_revision, schema_ref.schema_digest
            );
        }
        AuthzCommands::Schema {
            command:
                SchemaCommands::Bind {
                    schema_id,
                    schema_revision,
                    schema_digest,
                    realm_id,
                    expected_generation,
                    reason,
                },
        } => {
            let mut request = tonic::Request::new(api::BindAuthzSchemaRequest {
                context: None,
                scope: Some(scope(&tenant_id, realm_id)),
                schema_ref: Some(api::AuthzSchemaRef {
                    schema_id: schema_id.clone(),
                    schema_revision: *schema_revision,
                    schema_digest: schema_digest.clone(),
                }),
                expected_binding_generation: *expected_generation,
                reason: reason.clone(),
            });
            add_auth(&mut request, &token);
            let response = client.bind_authz_schema(request).await?.into_inner();
            println!(
                "binding_generation={} zookie={}",
                response.binding_generation, response.zookie
            );
        }
        AuthzCommands::Schema {
            command:
                SchemaCommands::Get {
                    schema_id,
                    schema_revision,
                },
        } => {
            let revision = if schema_revision.is_empty() {
                None
            } else {
                Some(schema_revision.parse()?)
            };
            let mut request = tonic::Request::new(api::GetAuthzSchemaRequest {
                namespace: String::new(),
                anvil_storage_tenant_id: tenant_id,
                schema_id: schema_id.clone(),
                schema_revision: revision,
            });
            add_auth(&mut request, &token);
            let response = client.get_authz_schema(request).await?.into_inner();
            println!(
                "namespaces={} version={}",
                response.namespaces.len(),
                response.schema_version
            );
        }
        AuthzCommands::Schema {
            command: SchemaCommands::Binding { realm_id },
        } => {
            let mut request = tonic::Request::new(api::GetAuthzSchemaBindingRequest {
                scope: Some(scope(&tenant_id, realm_id)),
            });
            add_auth(&mut request, &token);
            let response = client.get_authz_schema_binding(request).await?.into_inner();
            let schema_ref = response.schema_ref.unwrap_or_default();
            println!(
                "{}\t{}\t{}",
                schema_ref.schema_id, schema_ref.schema_revision, response.binding_generation
            );
        }
        AuthzCommands::Tuple {
            command:
                TupleCommands::Write {
                    namespace,
                    object_id,
                    relation,
                    subject_kind,
                    subject_id,
                    operation,
                    caveat_hash,
                    reason,
                },
        } => {
            let mut request = tonic::Request::new(api::WriteAuthzTupleRequest {
                context: None,
                namespace: namespace.clone(),
                object_id: object_id.clone(),
                relation: relation.clone(),
                subject_kind: subject_kind.clone(),
                subject_id: subject_id.clone(),
                caveat_hash: caveat_hash.clone(),
                operation: operation.clone(),
                reason: reason.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let response = client.write_authz_tuple(request).await?.into_inner();
            println!("revision={} zookie={}", response.revision, response.zookie);
        }
        AuthzCommands::Tuple {
            command:
                TupleCommands::Read {
                    namespace,
                    object_id,
                    relation,
                    subject_kind,
                    subject_id,
                    page_size,
                    page_token,
                },
        } => {
            let mut request = tonic::Request::new(api::ReadAuthzTuplesRequest {
                namespace: namespace.clone(),
                object_id: object_id.clone(),
                relation: relation.clone(),
                subject_kind: subject_kind.clone(),
                subject_id: subject_id.clone(),
                caveat_hash: String::new(),
                consistency: "latest".to_string(),
                zookie: String::new(),
                page_size: *page_size,
                page_token: page_token.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let response = client.read_authz_tuples(request).await?.into_inner();
            for tuple in response.tuples {
                println!(
                    "{}:{}#{} <- {}:{}",
                    tuple.namespace,
                    tuple.object_id,
                    tuple.relation,
                    tuple.subject_kind,
                    tuple.subject_id
                );
            }
            if !response.next_page_token.is_empty() {
                println!("next_page_token={}", response.next_page_token);
            }
        }
        AuthzCommands::Check {
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
        } => {
            let mut request = tonic::Request::new(api::CheckPermissionRequest {
                namespace: namespace.clone(),
                object_id: object_id.clone(),
                relation: relation.clone(),
                subject_kind: subject_kind.clone(),
                subject_id: subject_id.clone(),
                caveat_hash: String::new(),
                consistency: "latest".to_string(),
                zookie: String::new(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let response = client.check_permission(request).await?.into_inner();
            println!(
                "allowed={} revision={} zookie={}",
                response.allowed, response.revision, response.zookie
            );
        }
        AuthzCommands::ListObjects {
            namespace,
            relation,
            subject_kind,
            subject_id,
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListAuthzObjectsRequest {
                namespace: namespace.clone(),
                relation: relation.clone(),
                subject_kind: subject_kind.clone(),
                subject_id: subject_id.clone(),
                caveat_hash: String::new(),
                consistency: "latest".to_string(),
                zookie: String::new(),
                page_size: *page_size,
                page_token: page_token.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let response = client.list_authz_objects(request).await?.into_inner();
            for object_id in response.object_ids {
                println!("{object_id}");
            }
        }
        AuthzCommands::ListSubjects {
            namespace,
            object_id,
            relation,
            subject_kind,
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListAuthzSubjectsRequest {
                namespace: namespace.clone(),
                object_id: object_id.clone(),
                relation: relation.clone(),
                subject_kind: subject_kind.clone(),
                consistency: "latest".to_string(),
                zookie: String::new(),
                page_size: *page_size,
                page_token: page_token.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let response = client.list_authz_subjects(request).await?.into_inner();
            for subject in response.subjects {
                println!("{}:{}", subject.subject_kind, subject.subject_id);
            }
        }
        AuthzCommands::Watch {
            namespace,
            after_revision,
        } => {
            let mut request = tonic::Request::new(api::WatchAuthzTupleLogRequest {
                after_revision: *after_revision,
                namespace: namespace.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_authz_tuple_log(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!(
                    "{}\t{}\t{}:{}#{}",
                    item.revision, item.operation, item.namespace, item.object_id, item.relation
                );
            }
        }
    }
    Ok(())
}

fn scope(tenant_id: &str, realm_id: &str) -> api::AuthzScope {
    api::AuthzScope {
        anvil_storage_tenant_id: tenant_id.to_string(),
        authz_realm_id: realm_id.to_string(),
    }
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
