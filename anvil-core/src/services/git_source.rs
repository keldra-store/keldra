use crate::anvil_api::git_source_service_server::GitSourceService;
use crate::anvil_api::*;
use crate::object_manager::ObjectWriteOptions;
use crate::{
    AppState, access_control, auth, authz_journal, git_pack, git_source_index, git_source_manifest,
    git_source_query, git_source_watch,
    permissions::AnvilAction,
    services::watch_envelope::{self, WatchEnvelopeParts},
};
use futures_util::StreamExt;
use serde_json::json;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl GitSourceService for AppState {
    type WatchGitSourceStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchGitSourceResponse, Status>> + Send>,
    >;

    async fn put_git_pack(
        &self,
        request: Request<tonic::Streaming<PutGitPackRequest>>,
    ) -> Result<Response<PutGitPackResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let (metadata, pack_bytes) = collect_git_pack_stream(request.into_inner()).await?;
        validate_component("repository_id", &metadata.repository_id)?;
        if metadata.bucket_name.is_empty() {
            return Err(Status::invalid_argument("bucket_name must not be empty"));
        }
        let resource = git_source_resource(&metadata.repository_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::GitSourceWrite,
            &resource,
        )
        .await?;

        let source_hash = blake3::hash(&pack_bytes);
        let source_hash_hex = source_hash.to_hex().to_string();
        git_pack::build_git_source_index_from_pack(&metadata.repository_id, &pack_bytes, [0; 16])
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let object_key = format!(
            "git-source/{}/packs/{}.pack",
            metadata.repository_id, source_hash_hex
        );
        let pack_object = self
            .object_manager
            .put_object(
                &claims,
                &metadata.bucket_name,
                &object_key,
                tokio_stream::iter(vec![Ok(pack_bytes.clone())]),
                ObjectWriteOptions {
                    content_type: Some("application/x-git-packed-objects".to_string()),
                    user_metadata: Some(json!({
                        "object_kind": "git_pack",
                        "repository_id": metadata.repository_id.clone(),
                    })),
                    transaction_id: None,
                    transaction_principal: None,
                    storage_class_id: None,
                    ..Default::default()
                },
            )
            .await?;

        let parsed = git_pack::build_git_source_index_from_pack(
            &metadata.repository_id,
            &pack_bytes,
            *pack_object.version_id.as_bytes(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let generation = self
            .next_git_source_generation(claims.tenant_id, &metadata.repository_id)
            .await?;
        let index_ref = git_source_index::write_git_source_index(
            &self.storage,
            git_source_index::GitSourceIndexWrite {
                tenant_id: claims.tenant_id,
                repository_id: &metadata.repository_id,
                generation,
                source_hash: parsed.pack_hash,
                hash_algorithm: parsed.hash_algorithm,
                records: &parsed.records,
            },
        )
        .await
        .map_err(|err| Status::internal(format!("{err:#}")))?;
        let updated_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        git_source_manifest::write_git_source_repository_manifest(
            &self.storage,
            &git_source_manifest::GitSourceRepositoryManifest {
                format_version: 1,
                tenant_id: claims.tenant_id,
                repository_id: metadata.repository_id.clone(),
                bucket_name: metadata.bucket_name.clone(),
                object_key: object_key.clone(),
                pack_object_version_id: pack_object.version_id.to_string(),
                source_hash: source_hash_hex.clone(),
                generation,
                record_count: parsed.records.len() as u64,
                index_path: index_ref.clone(),
                updated_at: updated_at.clone(),
            },
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        let authz_revision = authz_journal::latest_authz_revision(&self.storage, claims.tenant_id)
            .await
            .map_err(|err| Status::internal(err.to_string()))?;
        let authz_revision = u64::try_from(authz_revision)
            .map_err(|_| Status::internal("Invalid authorization revision"))?;
        let payload = git_source_watch::GitSourceWatchPayload {
            repository_id: metadata.repository_id.clone(),
            event_type: "index_published".to_string(),
            generation,
            source_hash: source_hash_hex.clone(),
            index_path: index_ref.clone(),
            pack_object_version_id: Some(pack_object.version_id.to_string()),
            emitted_at: updated_at,
        };
        let watch_cursor = git_source_watch::append_git_source_watch_record(
            &self.storage,
            claims.tenant_id,
            &metadata.repository_id,
            *pack_object.mutation_id.as_bytes(),
            authz_revision,
            payload,
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;

        let (watch_cursor_low, watch_cursor_high) = split_u128(watch_cursor);
        Ok(Response::new(PutGitPackResponse {
            repository_id: metadata.repository_id,
            bucket_name: metadata.bucket_name,
            object_key,
            version_id: pack_object.version_id.to_string(),
            payload_hash: pack_object.content_hash,
            generation,
            source_hash: source_hash_hex,
            index_path: index_ref,
            record_count: parsed.records.len() as u64,
            watch_cursor_low,
            watch_cursor_high,
        }))
    }

    async fn get_git_object(
        &self,
        request: Request<GetGitObjectRequest>,
    ) -> Result<Response<GetGitObjectResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(self, &claims, &req.repository_id).await?;
        let object_id = parse_git_hex_id("object_id", &req.object_id)?;
        let index = self
            .latest_git_source_index(&claims, &req.repository_id)
            .await?;
        let generation = index.header.generation;
        let locations = git_source_query::get_git_object(&index, &object_id)
            .map_err(|err| Status::invalid_argument(err.to_string()))?
            .into_iter()
            .map(git_blob_location)
            .collect::<Result<Vec<_>, _>>()?;
        let locations = locations
            .into_iter()
            .map(|location| {
                (
                    format!(
                        "{}\0{}\0{}",
                        location.commit_id, location.tree_path, location.blob_start
                    ),
                    location,
                )
            })
            .collect::<Vec<_>>();
        let filters = [
            ("repository_id", req.repository_id.as_str()),
            ("object_id", req.object_id.as_str()),
        ];
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let (locations, page) = crate::services::collection_cursor::paginate(
            locations,
            req.page.as_ref(),
            "anvil.GitSourceService/GetGitObject",
            &filters,
            &principal_scope,
            "commit_id.tree_path.blob_start.asc",
            self.config.jwt_secret.as_bytes(),
            |location| location.0.as_str(),
            |_| generation,
        )?;

        Ok(Response::new(GetGitObjectResponse {
            locations: locations
                .into_iter()
                .map(|(_, location)| location)
                .collect(),
            page: Some(page),
        }))
    }

    async fn get_git_blob_by_path(
        &self,
        request: Request<GetGitBlobByPathRequest>,
    ) -> Result<Response<GetGitBlobByPathResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(self, &claims, &req.repository_id).await?;
        let commit_id = parse_git_hex_id("commit_id", &req.commit_id)?;
        let index = self
            .latest_git_source_index(&claims, &req.repository_id)
            .await?;
        let location = git_source_query::get_git_blob_by_path(&index, &commit_id, &req.tree_path)
            .map_err(|err| Status::invalid_argument(err.to_string()))?
            .ok_or_else(|| Status::not_found("Git blob path not found"))?;

        Ok(Response::new(GetGitBlobByPathResponse {
            location: Some(git_blob_location(location)?),
        }))
    }

    async fn list_git_tree(
        &self,
        request: Request<ListGitTreeRequest>,
    ) -> Result<Response<ListGitTreeResponse>, Status> {
        let claims = authorize_git_source_read(&request)?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        ensure_git_source_read(self, &claims, &req.repository_id).await?;
        let commit_id = parse_git_hex_id("commit_id", &req.commit_id)?;
        let index = self
            .latest_git_source_index(&claims, &req.repository_id)
            .await?;
        let generation = index.header.generation;
        let entries = git_source_query::list_git_tree(&index, &commit_id, &req.prefix)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let entries = entries
            .into_iter()
            .map(git_tree_entry_record)
            .collect::<Result<Vec<_>, _>>()?;
        let filters = [
            ("repository_id", req.repository_id.as_str()),
            ("commit_id", req.commit_id.as_str()),
            ("prefix", req.prefix.as_str()),
        ];
        let principal_scope = format!("tenant:{}/subject:{}", claims.tenant_id, claims.sub);
        let (entries, page) = crate::services::collection_cursor::paginate(
            entries,
            req.page.as_ref(),
            "anvil.GitSourceService/ListGitTree",
            &filters,
            &principal_scope,
            "tree_path.asc",
            self.config.jwt_secret.as_bytes(),
            |entry| entry.tree_path.as_str(),
            |_| generation,
        )?;

        Ok(Response::new(ListGitTreeResponse {
            entries,
            page: Some(page),
        }))
    }

    async fn watch_git_source(
        &self,
        request: Request<WatchGitSourceRequest>,
    ) -> Result<Response<Self::WatchGitSourceStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_component("repository_id", &req.repository_id)?;
        let resource = git_source_resource(&req.repository_id);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::GitSourceWatch,
            &resource,
        )
        .await?;

        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let stream_id =
            git_source_watch::git_source_watch_stream_id(claims.tenant_id, &req.repository_id);
        let mut live = self.storage.subscribe_stream(&stream_id);
        let storage = self.storage.clone();
        let repository_id = req.repository_id;
        let tenant_id = claims.tenant_id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            loop {
                loop {
                    let page = match git_source_watch::list_git_source_watch_event_page(
                        &storage,
                        tenant_id,
                        &repository_id,
                        last_cursor,
                        256,
                    )
                    .await
                    {
                        Ok(page) => page,
                        Err(error) => {
                            let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                            return;
                        }
                    };
                    let previous_cursor = last_cursor;
                    for event in page.events {
                        if tx.send(Ok(watch_git_source_response(event))).await.is_err() {
                            return;
                        }
                    }
                    last_cursor = page.next_cursor;
                    if !page.has_more || last_cursor == previous_cursor {
                        break;
                    }
                }
                match live.recv().await {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchGitSourceStream
        ))
    }
}

impl AppState {
    async fn latest_git_source_index(
        &self,
        claims: &auth::Claims,
        repository_id: &str,
    ) -> Result<git_source_index::DecodedGitSourceIndex, Status> {
        match git_source_query::read_latest_git_source_index(
            &self.storage,
            claims.tenant_id,
            repository_id,
        )
        .await
        {
            Ok(Some(index)) => Ok(index),
            Ok(None) => {
                self.rebuild_latest_git_source_index_from_manifest(claims, repository_id)
                    .await
            }
            Err(_) => {
                self.rebuild_latest_git_source_index_from_manifest(claims, repository_id)
                    .await
            }
        }
    }

    async fn rebuild_latest_git_source_index_from_manifest(
        &self,
        claims: &auth::Claims,
        repository_id: &str,
    ) -> Result<git_source_index::DecodedGitSourceIndex, Status> {
        let manifest = git_source_manifest::read_git_source_repository_manifest(
            &self.storage,
            claims.tenant_id,
            repository_id,
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Git source index not found"))?;
        let version_id = uuid::Uuid::parse_str(&manifest.pack_object_version_id)
            .map_err(|_| Status::internal("Git source manifest stores invalid pack version id"))?;
        let (_object, stream, _range_start) = self
            .object_manager
            .get_object(
                Some(claims.clone()),
                manifest.bucket_name.clone(),
                manifest.object_key.clone(),
                Some(version_id),
                None,
            )
            .await?;
        let pack_bytes = collect_object_stream(stream).await?;
        if blake3::hash(&pack_bytes).to_hex().to_string() != manifest.source_hash {
            return Err(Status::data_loss(
                "Git source pack hash differs from repository manifest",
            ));
        }
        let parsed = git_pack::build_git_source_index_from_pack(
            repository_id,
            &pack_bytes,
            *version_id.as_bytes(),
        )
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let index_ref = git_source_index::write_git_source_index(
            &self.storage,
            git_source_index::GitSourceIndexWrite {
                tenant_id: claims.tenant_id,
                repository_id,
                generation: manifest.generation,
                source_hash: parsed.pack_hash,
                hash_algorithm: parsed.hash_algorithm,
                records: &parsed.records,
            },
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?;
        if index_ref != manifest.index_path {
            let mut updated = manifest;
            updated.index_path = index_ref;
            updated.record_count = parsed.records.len() as u64;
            updated.updated_at =
                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
            git_source_manifest::write_git_source_repository_manifest(&self.storage, &updated)
                .await
                .map_err(|err| Status::internal(err.to_string()))?;
        }
        git_source_query::read_latest_git_source_index(
            &self.storage,
            claims.tenant_id,
            repository_id,
        )
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Git source index not found"))
    }

    async fn next_git_source_generation(
        &self,
        tenant_id: i64,
        repository_id: &str,
    ) -> Result<u64, Status> {
        let Some(index) =
            git_source_query::read_latest_git_source_index(&self.storage, tenant_id, repository_id)
                .await
                .map_err(|err| Status::internal(err.to_string()))?
        else {
            return Ok(1);
        };
        index
            .header
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::internal("Git source generation overflow"))
    }
}

async fn collect_git_pack_stream(
    mut stream: tonic::Streaming<PutGitPackRequest>,
) -> Result<(GitPackMetadata, Vec<u8>), Status> {
    let metadata = match stream.next().await {
        Some(Ok(chunk)) => match chunk.data {
            Some(put_git_pack_request::Data::Metadata(metadata)) => metadata,
            _ => return Err(Status::invalid_argument("First chunk must be metadata")),
        },
        Some(Err(err)) => return Err(err),
        None => return Err(Status::invalid_argument("Empty Git pack stream")),
    };
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk?.data {
            Some(put_git_pack_request::Data::Chunk(data)) => bytes.extend_from_slice(&data),
            Some(put_git_pack_request::Data::Metadata(_)) => {
                return Err(Status::invalid_argument(
                    "Git pack metadata must only appear once",
                ));
            }
            None => {}
        }
    }
    if bytes.is_empty() {
        return Err(Status::invalid_argument("Git pack bytes must not be empty"));
    }
    Ok((metadata, bytes))
}

async fn collect_object_stream(
    mut stream: std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>,
    >,
) -> Result<Vec<u8>, Status> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

fn authorize_git_source_read<T>(request: &Request<T>) -> Result<auth::Claims, Status> {
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    Ok(claims)
}

async fn ensure_git_source_read(
    state: &AppState,
    claims: &auth::Claims,
    repository_id: &str,
) -> Result<(), Status> {
    let resource = git_source_resource(repository_id);
    access_control::require_action(
        &state.storage,
        &state.persistence,
        claims,
        AnvilAction::GitSourceRead,
        &resource,
    )
    .await
}

fn git_source_resource(repository_id: &str) -> String {
    format!("repository:{repository_id}")
}

fn validate_component(name: &str, value: &str) -> Result<(), Status> {
    if value.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{name} must not be empty"
        )));
    }
    if value == "." || value == ".." || value.contains('/') || value.chars().any(char::is_control) {
        return Err(Status::invalid_argument(format!(
            "{name} must be a safe path component"
        )));
    }
    Ok(())
}

fn parse_git_hex_id(name: &str, value: &str) -> Result<Vec<u8>, Status> {
    if value.len() != 40 && value.len() != 64 {
        return Err(Status::invalid_argument(format!(
            "{name} must be a SHA-1 or SHA-256 hex object id"
        )));
    }
    hex::decode(value).map_err(|_| Status::invalid_argument(format!("{name} must be hex")))
}

fn git_blob_location(
    location: git_source_query::GitObjectLookup,
) -> Result<GitBlobLocation, Status> {
    Ok(GitBlobLocation {
        repository_id: location.repository_id,
        commit_id: hex::encode(location.commit_id),
        object_id: hex::encode(location.object_id),
        tree_path: location.tree_path,
        blob_start: location.blob_start,
        blob_len: location.blob_len,
        pack_object_version_id: uuid::Uuid::from_bytes(location.pack_object_version_id).to_string(),
    })
}

fn git_tree_entry_record(
    entry: git_source_query::GitTreeEntry,
) -> Result<GitTreeEntryRecord, Status> {
    Ok(GitTreeEntryRecord {
        tree_path: entry.tree_path,
        object_id: hex::encode(entry.object_id),
        blob_start: entry.blob_start,
        blob_len: entry.blob_len,
        pack_object_version_id: uuid::Uuid::from_bytes(entry.pack_object_version_id).to_string(),
    })
}

fn watch_git_source_response(
    event: git_source_watch::GitSourceWatchEvent,
) -> WatchGitSourceResponse {
    let (cursor_low, cursor_high) = split_u128(event.cursor);
    let payload = event.payload;
    let emitted_at = payload.emitted_at.clone();
    let repository_id = payload.repository_id.clone();
    let generation = payload.generation;
    let payload_hash = watch_envelope::payload_hash(&payload);
    WatchGitSourceResponse {
        cursor_low,
        cursor_high,
        repository_id: repository_id.clone(),
        event_type: payload.event_type,
        generation,
        source_hash: payload.source_hash,
        index_path: payload.index_path,
        pack_object_version_id: payload.pack_object_version_id.unwrap_or_default(),
        authz_revision: event.authz_revision,
        emitted_at: emitted_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "git_source",
            partition_family: "git_source",
            partition_id: repository_id.clone(),
            cursor: event.cursor,
            mutation_id: watch_envelope::uuid_from_bytes(event.mutation_id),
            record_kind: "git_source".to_string(),
            object_ref: repository_id,
            authz_revision: event.authz_revision,
            index_generation: generation,
            personaldb_log_index: 0,
            payload_hash,
            emitted_at,
        })),
    }
}

fn split_u128(value: u128) -> (u64, u64) {
    (value as u64, (value >> 64) as u64)
}

fn join_u128(low: u64, high: u64) -> u128 {
    u128::from(low) | (u128::from(high) << 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_source_watch_cursor_split_round_trips() {
        let cursor = (u128::from(77_u64) << 64) | u128::from(33_u64);
        let (low, high) = split_u128(cursor);
        assert_eq!(join_u128(low, high), cursor);
    }
}
