use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum IndexCommands {
    Create {
        bucket: String,
        name: String,
        kind: String,
        #[clap(long)]
        transaction_id: Option<String>,
        #[clap(long, default_value = "{}")]
        selector_json: String,
        #[clap(long, default_value = "{}")]
        extractor_json: String,
        #[clap(long, default_value = "inherit_object")]
        authorization_mode: String,
        #[clap(long, default_value = "{}")]
        build_policy_json: String,
    },
    Update {
        bucket: String,
        name: String,
        #[clap(long)]
        transaction_id: Option<String>,
        #[clap(long, default_value = "{}")]
        selector_json: String,
        #[clap(long, default_value = "{}")]
        extractor_json: String,
        #[clap(long, default_value = "inherit_object")]
        authorization_mode: String,
        #[clap(long, default_value = "{}")]
        build_policy_json: String,
    },
    Disable {
        bucket: String,
        name: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    Drop {
        bucket: String,
        name: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    List {
        bucket: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        include_disabled: bool,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
    Query {
        bucket: String,
        index: String,
        #[clap(long, default_value = "")]
        text: String,
        #[clap(long, value_delimiter = ',')]
        vector: Vec<f32>,
        #[clap(long, default_value_t = 20)]
        limit: u32,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        phrase: bool,
        #[clap(long, default_value = "")]
        path_prefix: String,
        #[clap(long, default_value = "{}")]
        metadata_filters_json: String,
        #[clap(long, default_value = "[]")]
        boundary_predicates_json: String,
        #[clap(long, default_value = "{}")]
        typed_predicates_json: String,
        #[clap(long, default_value = "[]")]
        typed_order_json: String,
        #[clap(long, default_value = "")]
        page_token: String,
        #[clap(long, default_value = "")]
        require_caught_up_to_watch_cursor: String,
        #[clap(long, default_value_t = 0)]
        lag_timeout_ms: u64,
    },
    Diagnostics {
        bucket: String,
        index: String,
        #[clap(long, default_value_t = 100)]
        page_size: u32,
        #[clap(long, default_value = "")]
        page_token: String,
        #[clap(long, default_value = "")]
        severity: String,
    },
}

pub async fn handle_index_command(command: &IndexCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = IndexServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        IndexCommands::Create {
            bucket,
            name,
            kind,
            transaction_id,
            selector_json,
            extractor_json,
            authorization_mode,
            build_policy_json,
        } => {
            let mut request = tonic::Request::new(api::CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: name.clone(),
                kind: parse_index_kind(kind)?,
                selector_json: selector_json.clone(),
                extractor_json: extractor_json.clone(),
                authorization_mode: authorization_mode.clone(),
                build_policy_json: build_policy_json.clone(),

                options: write_options(transaction_id),
            });
            add_auth(&mut request, &token);
            print_index(client.create_index(request).await?.into_inner().index);
        }
        IndexCommands::Update {
            bucket,
            name,
            transaction_id,
            selector_json,
            extractor_json,
            authorization_mode,
            build_policy_json,
        } => {
            let mut request = tonic::Request::new(api::UpdateIndexRequest {
                bucket_name: bucket.clone(),
                name: name.clone(),
                selector_json: selector_json.clone(),
                extractor_json: extractor_json.clone(),
                authorization_mode: authorization_mode.clone(),
                build_policy_json: build_policy_json.clone(),

                options: write_options(transaction_id),
            });
            add_auth(&mut request, &token);
            print_index(client.update_index(request).await?.into_inner().index);
        }
        IndexCommands::Disable {
            bucket,
            name,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::DisableIndexRequest {
                bucket_name: bucket.clone(),
                name: name.clone(),

                options: write_options(transaction_id),
            });
            add_auth(&mut request, &token);
            print_index(client.disable_index(request).await?.into_inner().index);
        }
        IndexCommands::Drop {
            bucket,
            name,
            transaction_id,
        } => {
            let mut request = tonic::Request::new(api::DropIndexRequest {
                bucket_name: bucket.clone(),
                name: name.clone(),

                options: write_options(transaction_id),
            });
            add_auth(&mut request, &token);
            client.drop_index(request).await?;
            println!("dropped {bucket}/{name}");
        }
        IndexCommands::List {
            bucket,
            include_disabled,
            page_size,
            page_token,
        } => {
            let mut request = tonic::Request::new(api::ListIndexesRequest {
                bucket_name: bucket.clone(),
                include_disabled: *include_disabled,
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            add_auth(&mut request, &token);
            let response = client.list_indexes(request).await?.into_inner();
            for index in response.indexes {
                print_index(Some(index));
            }
            if let Some(page) = response
                .page
                .filter(|page| !page.next_page_token.is_empty())
            {
                println!("next_page_token={}", page.next_page_token);
            }
        }
        IndexCommands::Query {
            bucket,
            index,
            text,
            vector,
            limit,
            phrase,
            path_prefix,
            metadata_filters_json,
            boundary_predicates_json,
            typed_predicates_json,
            typed_order_json,
            page_token,
            require_caught_up_to_watch_cursor,
            lag_timeout_ms,
        } => {
            let mut request = tonic::Request::new(api::QueryIndexRequest {
                bucket_name: bucket.clone(),
                index_name: index.clone(),
                query_text: text.clone(),
                query_vector: vector.clone(),
                limit: *limit,
                phrase: *phrase,
                path_prefix: path_prefix.clone(),
                metadata_filters_json: metadata_filters_json.clone(),
                boundary_predicates_json: boundary_predicates_json.clone(),
                typed_predicates_json: typed_predicates_json.clone(),
                typed_order_json: typed_order_json.clone(),
                page_token: page_token.clone(),
                require_caught_up_to_watch_cursor: require_caught_up_to_watch_cursor.clone(),
                lag_timeout_ms: *lag_timeout_ms,
            });
            add_auth(&mut request, &token);
            let response = client.query_index(request).await?.into_inner();
            for hit in response.hits {
                println!("{}\t{}\t{}", hit.score, hit.object_key, hit.metadata_json);
            }
            if !response.next_page_token.is_empty() {
                println!("next_page_token={}", response.next_page_token);
            }
        }
        IndexCommands::Diagnostics {
            bucket,
            index,
            page_size,
            page_token,
            severity,
        } => {
            let mut request = tonic::Request::new(api::ListIndexDiagnosticsRequest {
                bucket_name: bucket.clone(),
                index_name: index.clone(),
                severity: severity.clone(),
                page: Some(api::PageRequest {
                    page_size: *page_size,
                    page_token: page_token.clone(),
                }),
            });
            add_auth(&mut request, &token);
            let response = client.list_index_diagnostics(request).await?.into_inner();
            for diagnostic in response.diagnostics {
                println!(
                    "{}\t{}\t{}\t{}",
                    diagnostic.cursor, diagnostic.severity, diagnostic.code, diagnostic.message
                );
            }
            if let Some(page) = response
                .page
                .filter(|page| !page.next_page_token.is_empty())
            {
                println!("next_page_token={}", page.next_page_token);
            }
        }
    }
    Ok(())
}

fn parse_index_kind(value: &str) -> anyhow::Result<i32> {
    let kind = match value.trim().to_ascii_lowercase().as_str() {
        "path" => api::IndexKind::Path,
        "metadata" | "metadata-filter" | "metadata_filter" => api::IndexKind::MetadataFilter,
        "full-text" | "full_text" | "fulltext" => api::IndexKind::FullText,
        "vector" => api::IndexKind::Vector,
        "hybrid" => api::IndexKind::Hybrid,
        "personaldb-row-metadata" | "personaldb_row_metadata" => {
            api::IndexKind::PersonaldbRowMetadata
        }
        "git-source" | "git_source" => api::IndexKind::GitSource,
        "typed-json" | "typed_json" => api::IndexKind::TypedJson,
        other => anyhow::bail!("unknown index kind '{other}'"),
    };
    Ok(kind as i32)
}

fn print_index(index: Option<api::IndexDefinitionRecord>) {
    if let Some(index) = index {
        println!(
            "{}\t{}\t{:?}\t{}\tv{}",
            index.bucket_name,
            index.name,
            api::IndexKind::try_from(index.kind).unwrap_or(api::IndexKind::Unspecified),
            index.enabled,
            index.version
        );
    }
}

fn write_options(transaction_id: &Option<String>) -> Option<api::WriteOptions> {
    let transaction_id = transaction_id.as_ref()?.trim();
    if transaction_id.is_empty() {
        return None;
    }
    Some(api::WriteOptions {
        idempotency_key: format!("index-cli-{}", uuid::Uuid::new_v4()),
        consistency: api::ConsistencyMode::Committed as i32,
        wait_for_finalization: false,
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        execution: Some(api::write_options::Execution::TransactionId(
            transaction_id.to_string(),
        )),
    })
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
