use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::Subcommand;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Subcommand)]
pub enum ObjectCommands {
    /// Upload a file to an object
    Put {
        src: String,
        dest: String,
        #[clap(long)]
        content_type: Option<String>,
        #[clap(long, default_value = "{}")]
        user_metadata_json: String,
        #[clap(long)]
        transaction_id: Option<String>,
        #[clap(long)]
        storage_class: Option<String>,
    },
    /// Download an object to a file or stdout
    Get { src: String, dest: Option<String> },
    /// Remove an object
    Rm {
        path: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// List objects in a bucket
    Ls { path: String },
    /// Show object metadata
    Head { path: String },
    /// Manage bucket boundary schemas used by CoreStore placement and query planning.
    Boundary {
        #[clap(subcommand)]
        command: ObjectBoundaryCommands,
    },
    /// Manage object links.
    Link {
        #[clap(subcommand)]
        command: ObjectLinkCommands,
    },
}

#[derive(Subcommand)]
pub enum ObjectBoundaryCommands {
    /// Create or update a bucket boundary schema from a JSON file.
    Put {
        bucket: String,
        schema_json: String,
        #[clap(long)]
        expected_generation: Option<u64>,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Read the current bucket boundary schema.
    Get { bucket: String },
    /// Start a boundary migration between schema generations.
    StartMigration {
        bucket: String,
        #[clap(long)]
        from_generation: u64,
        #[clap(long)]
        to_generation: u64,
        #[clap(long, default_value = "reindex-only")]
        mode: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Read boundary migration status.
    GetMigration {
        bucket: String,
        migration_id: String,
    },
}

#[derive(Debug, Deserialize)]
struct BoundarySchemaInput {
    dimensions: Vec<BoundaryDimensionInput>,
}

#[derive(Debug, Deserialize)]
struct BoundaryDimensionInput {
    name: String,
    source: BoundarySourceInput,
    value_type: String,
    categories: Vec<String>,
    required: bool,
    cardinality: String,
    max_values_per_block: u32,
    placement_affinity: String,
    compaction_scope: String,
    #[serde(default)]
    shared_ranges_allowed: bool,
    #[serde(default)]
    shared_record_kinds: Vec<String>,
    #[serde(default)]
    deprecated: bool,
}

#[derive(Debug, Deserialize)]
struct BoundarySourceInput {
    kind: String,
    value: String,
    #[serde(default)]
    max_body_bytes: u64,
}

#[derive(Subcommand)]
pub enum ObjectLinkCommands {
    /// Create a link inside a bucket.
    Create {
        link: String,
        target: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
        #[clap(long, default_value = "follow")]
        resolution: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Update an existing link.
    Update {
        link: String,
        target: String,
        #[clap(long)]
        expected_generation: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
        #[clap(long, default_value = "follow")]
        resolution: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Delete a link.
    Delete {
        link: String,
        #[clap(long)]
        expected_generation: u64,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Read link metadata.
    Read { link: String },
    /// List links in a bucket.
    List {
        path: String,
        #[clap(long, default_value_t = 100)]
        limit: u32,
    },
}

fn parse_bucket_path(path: &str) -> anyhow::Result<String> {
    let path = path
        .strip_prefix("s3://")
        .unwrap_or(path)
        .trim_end_matches('/');
    if path.is_empty() || path.contains('/') {
        return Err(anyhow::anyhow!(
            "expected a bucket path such as s3://bucket-name"
        ));
    }
    Ok(path.to_string())
}

fn parse_s3_path(path: &str) -> anyhow::Result<(String, String)> {
    let path = path.strip_prefix("s3://").unwrap_or(path);
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid S3 path"));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct NativeTokenClaims {
    pub(crate) sub: String,
    pub(crate) tenant_id: i64,
}

pub(crate) fn decode_native_token_claims(token: &str) -> anyhow::Result<NativeTokenClaims> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("access token is not a JWT"))?;
    let bytes = URL_SAFE_NO_PAD.decode(payload)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn native_mutation_context(
    _ctx: &Context,
    token: &str,
    _bucket_name: &str,
    tag: &str,
    transaction_id: Option<String>,
) -> anyhow::Result<api::NativeMutationContext> {
    let claims = decode_native_token_claims(token)?;

    Ok(api::NativeMutationContext {
        tenant_id: claims.tenant_id,
        // The server resolves and validates the tenant-scoped bucket by name.
        // Keeping this unset avoids requiring bucket:list just to mutate one object.
        bucket_id: 0,
        principal: claims.sub,
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        transaction_id,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    })
}

pub async fn handle_object_command(command: &ObjectCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        ObjectCommands::Put {
            src,
            dest,
            content_type,
            user_metadata_json,
            transaction_id,
            storage_class,
        } => {
            let (bucket, key) = parse_s3_path(dest)?;
            serde_json::from_str::<serde_json::Value>(user_metadata_json)
                .map_err(|error| anyhow::anyhow!("invalid --user-metadata-json: {error}"))?;
            let mutation_context =
                native_mutation_context(ctx, &token, &bucket, "put", transaction_id.clone())
                    .await?;
            let metadata = api::ObjectMetadata {
                bucket_name: bucket,
                object_key: key,
                mutation_context: Some(mutation_context),
                content_type: content_type.clone(),
                user_metadata_json: user_metadata_json.clone(),
                storage_class: storage_class.clone(),
            };
            let mut file = tokio::fs::File::open(src).await?;
            let (tx, rx) = mpsc::channel(4);
            let metadata_tx = tx.clone();
            metadata_tx
                .send(api::PutObjectRequest {
                    data: Some(api::put_object_request::Data::Metadata(metadata)),
                })
                .await?;
            drop(metadata_tx);
            let upload_task = tokio::spawn(async move {
                let mut buffer = vec![0_u8; 256 * 1024];
                loop {
                    let read = match file.read(&mut buffer).await {
                        Ok(0) => break,
                        Ok(read) => read,
                        Err(error) => return Err(error),
                    };
                    if tx
                        .send(api::PutObjectRequest {
                            data: Some(api::put_object_request::Data::Chunk(
                                buffer[..read].to_vec(),
                            )),
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok::<(), std::io::Error>(())
            });
            let mut request = tonic::Request::new(ReceiverStream::new(rx));
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.put_object(request).await?;
            upload_task.await??;
            println!("Uploaded {} to {}", src, dest);
        }
        ObjectCommands::Get { src, dest } => {
            let (bucket, key) = parse_s3_path(src)?;
            let mut request = tonic::Request::new(api::GetObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
                range: None,

                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let mut stream = client.get_object(request).await?.into_inner();

            if let Some(dest_path) = dest {
                let mut file = tokio::fs::File::create(dest_path).await?;
                let mut expected_len = None;
                let mut bytes_written = 0_u64;
                while let Some(chunk) = stream.message().await? {
                    match chunk.data {
                        Some(api::get_object_response::Data::Metadata(info)) => {
                            expected_len = Some(u64::try_from(info.content_length)?);
                        }
                        Some(api::get_object_response::Data::Chunk(bytes)) => {
                            file.write_all(&bytes).await?;
                            bytes_written = bytes_written.saturating_add(bytes.len() as u64);
                        }
                        None => {}
                    }
                }
                file.flush().await?;
                if let Some(expected_len) = expected_len {
                    anyhow::ensure!(
                        bytes_written == expected_len,
                        "downloaded {bytes_written} bytes from {src}, expected {expected_len}"
                    );
                }
                println!("Downloaded {} to {}", src, dest_path);
            } else {
                let mut expected_len = None;
                let mut bytes_written = 0_u64;
                while let Some(chunk) = stream.message().await? {
                    match chunk.data {
                        Some(api::get_object_response::Data::Metadata(info)) => {
                            expected_len = Some(u64::try_from(info.content_length)?);
                        }
                        Some(api::get_object_response::Data::Chunk(bytes)) => {
                            bytes_written = bytes_written.saturating_add(bytes.len() as u64);
                            print!("{}", String::from_utf8_lossy(&bytes));
                        }
                        None => {}
                    }
                }
                if let Some(expected_len) = expected_len {
                    anyhow::ensure!(
                        bytes_written == expected_len,
                        "downloaded {bytes_written} bytes from {src}, expected {expected_len}"
                    );
                }
            }
        }
        ObjectCommands::Rm {
            path,
            transaction_id,
        } => {
            let (bucket, key) = parse_s3_path(path)?;
            let mutation_context =
                native_mutation_context(ctx, &token, &bucket, "rm", transaction_id.clone()).await?;
            let mut request = tonic::Request::new(api::DeleteObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
                mutation_context: Some(mutation_context),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.delete_object(request).await?;
            println!("Removed {}", path);
        }
        ObjectCommands::Ls { path } => {
            let (bucket, prefix) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::ListObjectsRequest {
                bucket_name: bucket,
                prefix,
                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let resp = client.list_objects(request).await?;
            for obj in resp.into_inner().objects {
                println!("{}\t{}\t{}", obj.last_modified, obj.size, obj.key);
            }
        }
        ObjectCommands::Head { path } => {
            let (bucket, key) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::HeadObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,

                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let resp = client.head_object(request).await?;
            let obj = resp.into_inner();
            println!(
                "ETag: {}\nSize: {}\nLast Modified: {}",
                obj.etag, obj.size, obj.last_modified
            );
        }
        ObjectCommands::Boundary { command } => {
            handle_object_boundary_command(command, &mut client, &token).await?;
        }
        ObjectCommands::Link { command } => {
            handle_object_link_command(command, ctx, &mut client, &token).await?;
        }
    }

    Ok(())
}

async fn handle_object_boundary_command(
    command: &ObjectBoundaryCommands,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        ObjectBoundaryCommands::Put {
            bucket,
            schema_json,
            expected_generation,
            transaction_id,
        } => {
            let bucket_name = parse_bucket_path(bucket)?;
            let raw = tokio::fs::read_to_string(schema_json).await?;
            let input: BoundarySchemaInput = serde_json::from_str(&raw)?;
            let mut request = tonic::Request::new(api::PutBoundarySchemaRequest {
                bucket_name,
                expected_generation: *expected_generation,
                dimensions: input
                    .dimensions
                    .into_iter()
                    .map(boundary_dimension_input_to_proto)
                    .collect(),
                mutation_id: format!("boundary-schema-{}", uuid::Uuid::new_v4()),
                transaction_id: transaction_id.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.put_boundary_schema(request).await?.into_inner();
            print_boundary_schema(response.schema)?;
        }
        ObjectBoundaryCommands::Get { bucket } => {
            let bucket_name = parse_bucket_path(bucket)?;
            let mut request = tonic::Request::new(api::GetBoundarySchemaRequest {
                bucket_name,
                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.get_boundary_schema(request).await?.into_inner();
            print_boundary_schema(response.schema)?;
        }
        ObjectBoundaryCommands::StartMigration {
            bucket,
            from_generation,
            to_generation,
            mode,
            transaction_id,
        } => {
            let bucket_name = parse_bucket_path(bucket)?;
            let mut request = tonic::Request::new(api::StartBoundaryMigrationRequest {
                bucket_name,
                from_generation: *from_generation,
                to_generation: *to_generation,
                mode: boundary_migration_mode(mode)?,
                mutation_context: Some(api::NativeMutationContext {
                    tenant_id: 0,
                    bucket_id: 0,
                    principal: String::new(),
                    request_id: String::new(),
                    precondition: String::new(),
                    authz_zookie_optional: String::new(),
                    idempotency_key: format!("boundary-migration-{}", uuid::Uuid::new_v4()),
                    transaction_id: transaction_id.clone(),
                    saga_operation: None,
                    saga_compensation_operation: None,
                    write_visibility: None,
                }),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.start_boundary_migration(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        ObjectBoundaryCommands::GetMigration {
            bucket,
            migration_id,
        } => {
            let bucket_name = parse_bucket_path(bucket)?;
            let mut request = tonic::Request::new(api::GetBoundaryMigrationRequest {
                bucket_name,
                migration_id: migration_id.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.get_boundary_migration(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
    }
    Ok(())
}

fn boundary_migration_mode(value: &str) -> anyhow::Result<i32> {
    match value {
        "reindex-only" | "reindex_only" => Ok(1),
        "rewrite-on-compaction" | "rewrite_on_compaction" => Ok(2),
        "force-rewrite-now" | "force_rewrite_now" => Ok(3),
        other => anyhow::bail!("unsupported boundary migration mode {other}"),
    }
}

fn boundary_dimension_input_to_proto(value: BoundaryDimensionInput) -> api::BoundaryDimension {
    api::BoundaryDimension {
        name: value.name,
        source: Some(api::BoundarySource {
            kind: value.source.kind,
            value: value.source.value,
            max_body_bytes: value.source.max_body_bytes,
        }),
        value_type: value.value_type,
        categories: value.categories,
        required: value.required,
        cardinality: value.cardinality,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity,
        compaction_scope: value.compaction_scope,
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds,
        deprecated: value.deprecated,
    }
}

fn print_boundary_schema(schema: Option<api::BoundarySchemaRecord>) -> anyhow::Result<()> {
    let schema = schema.ok_or_else(|| anyhow::anyhow!("server returned no boundary schema"))?;
    println!("Bucket: {}", schema.bucket_name);
    println!("Generation: {}", schema.generation);
    println!("Hash: {}", schema.schema_hash);
    for dimension in schema.dimensions {
        let source = dimension.source.unwrap_or_default();
        println!(
            "- {} {} required={} source={}({}) categories={}",
            dimension.name,
            dimension.value_type,
            dimension.required,
            source.kind,
            source.value,
            dimension.categories.join(",")
        );
    }
    Ok(())
}

async fn handle_object_link_command(
    command: &ObjectLinkCommands,
    ctx: &Context,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        ObjectLinkCommands::Create {
            link,
            target,
            allow_dangling,
            resolution,
            transaction_id,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let (target_bucket, target_key) = parse_s3_path(target)?;
            anyhow::ensure!(
                bucket == target_bucket,
                "cross-bucket object links are not supported by the public CLI"
            );
            let mut request = tonic::Request::new(api::CreateObjectLinkRequest {
                context: Some(public_link_context(
                    "link-create",
                    0,
                    transaction_id.clone(),
                )),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
                target_key,
                target_version: String::new(),
                resolution: parse_link_resolution(resolution)?,
                allow_dangling: *allow_dangling,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.create_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::Update {
            link,
            target,
            expected_generation,
            allow_dangling,
            resolution,
            transaction_id,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let (target_bucket, target_key) = parse_s3_path(target)?;
            anyhow::ensure!(
                bucket == target_bucket,
                "cross-bucket object links are not supported by the public CLI"
            );
            let mut request = tonic::Request::new(api::UpdateObjectLinkRequest {
                context: Some(public_link_context(
                    "link-update",
                    *expected_generation,
                    transaction_id.clone(),
                )),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
                target_key,
                target_version: String::new(),
                resolution: parse_link_resolution(resolution)?,
                allow_dangling: *allow_dangling,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.update_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::Delete {
            link,
            expected_generation,
            transaction_id,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let mut request = tonic::Request::new(api::DeleteObjectLinkRequest {
                context: Some(public_link_context(
                    "link-delete",
                    *expected_generation,
                    transaction_id.clone(),
                )),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.delete_object_link(request).await?.into_inner();
            println!(
                "Deleted {} at generation {}",
                response.resource_id, response.generation
            );
        }
        ObjectLinkCommands::Read { link } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let mut request = tonic::Request::new(api::ReadObjectLinkRequest {
                request_id: format!("link-read-{}", uuid::Uuid::new_v4()),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,

                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.read_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::List { path, limit } => {
            let (bucket, prefix) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::ListObjectLinksRequest {
                tenant_id: String::new(),
                bucket_name: bucket,
                prefix,
                page: Some(api::PageRequest {
                    page_token: String::new(),
                    page_size: *limit,
                }),

                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.list_object_links(request).await?.into_inner();
            for link in response.links {
                println!(
                    "{} -> {} (generation {})",
                    link.link_key, link.target_key, link.generation
                );
            }
        }
    }

    let _ = ctx;
    Ok(())
}

fn public_link_context(
    tag: &str,
    expected_generation: u64,
    transaction_id: Option<String>,
) -> api::PublicMutationContext {
    api::PublicMutationContext {
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        expected_generation,
        transaction_id,
        saga_operation: None,
        saga_compensation_operation: None,
    }
}

fn parse_link_resolution(value: &str) -> anyhow::Result<i32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "follow" => Ok(api::ObjectLinkResolution::Follow as i32),
        "redirect" => Ok(api::ObjectLinkResolution::Redirect as i32),
        other => Err(anyhow::anyhow!(
            "invalid link resolution '{other}', expected follow or redirect"
        )),
    }
}

fn print_link(link: Option<api::ObjectLinkDescriptor>) {
    if let Some(link) = link {
        println!(
            "{} -> {}{} (generation {})",
            link.link_key,
            link.target_key,
            if link.target_version.is_empty() {
                String::new()
            } else {
                format!("@{}", link.target_version)
            },
            link.generation
        );
    }
}
