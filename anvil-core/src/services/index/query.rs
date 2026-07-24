use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQuerySpec {
    pub(super) schema: String,
    pub(super) scope: AnvilQueryScope,
    #[serde(default = "default_query_source_kind")]
    pub(super) source_kind: String,
    #[serde(rename = "where", default)]
    pub(super) predicates: AnvilQueryWhere,
    #[serde(default)]
    pub(super) order_by: Vec<AnvilQueryOrder>,
    #[serde(default)]
    pub(super) limit: Option<u32>,
    #[serde(default)]
    pub(super) consistency: AnvilQueryConsistency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQueryScope {
    #[serde(default)]
    pub(super) mesh_id: Option<String>,
    #[serde(default)]
    pub(super) anvil_storage_tenant_id: Option<String>,
    #[serde(default)]
    pub(super) authz_scope: Option<JsonValue>,
    pub(super) bucket_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct AnvilQueryWhere {
    #[serde(default)]
    pub(super) all: Vec<JsonValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct AnvilQueryOrder {
    pub(super) field: String,
    #[serde(default = "default_ascending")]
    pub(super) direction: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(super) struct AnvilQueryConsistency {
    #[serde(default)]
    pub(super) min_source_cursor: Option<JsonValue>,
    #[serde(default)]
    pub(super) min_authz_revision: Option<JsonValue>,
    #[serde(default)]
    pub(super) allow_stale_index: Option<bool>,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecShape {
    pub(super) source_kind: String,
    pub(super) authz_scope: Option<JsonValue>,
    pub(super) boundary_predicates: Vec<JsonValue>,
    pub(super) path_prefix: Option<String>,
    pub(super) typed_predicates: Vec<JsonValue>,
    pub(super) typed_order: Vec<TypedOrder>,
    pub(super) query_text: Option<String>,
    pub(super) query_vector: Option<Vec<f32>>,
    pub(super) phrase: bool,
    pub(super) can_relation: Option<String>,
    pub(super) min_source_cursor: Option<u64>,
    pub(super) min_authz_revision: Option<u64>,
    pub(super) limit: u32,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecPlan {
    pub(super) index: crate::persistence::IndexDefinition,
    pub(super) typed_filter_index: Option<crate::persistence::IndexDefinition>,
    pub(super) authz_scope: QueryAuthzScope,
    pub(super) canonical_query_hash: String,
    pub(super) plan_json: String,
    pub(super) query_text: String,
    pub(super) query_vector: Vec<f32>,
    pub(super) phrase: bool,
    pub(super) path_prefix: String,
    pub(super) boundary_predicates: Vec<JsonValue>,
    pub(super) typed_predicates: Vec<JsonValue>,
    pub(super) typed_order: Vec<TypedOrder>,
    pub(super) limit: u32,
    pub(super) require_caught_up_to_watch_cursor: String,
}

#[derive(Debug, Clone)]
pub(super) struct QuerySpecIndexSelection {
    pub(super) primary: crate::persistence::IndexDefinition,
    pub(super) typed_filter: Option<crate::persistence::IndexDefinition>,
}

impl QuerySpecIndexSelection {
    pub(super) fn requires_object_authorization(&self) -> bool {
        self.primary.authorization_mode == "inherit_object"
            || self
                .typed_filter
                .as_ref()
                .is_some_and(|index| index.authorization_mode == "inherit_object")
    }

    pub(super) fn effective_authorization_mode(&self) -> &str {
        if self.primary.authorization_mode == "inherit_object" {
            return &self.primary.authorization_mode;
        }
        let Some(filter) = self.typed_filter.as_ref() else {
            return &self.primary.authorization_mode;
        };
        if filter.authorization_mode == "inherit_object"
            || (self.primary.authorization_mode == "public"
                && filter.authorization_mode == "index_only")
        {
            &filter.authorization_mode
        } else {
            &self.primary.authorization_mode
        }
    }
}

impl QuerySpecPlan {
    pub(super) fn single_query_request(
        &self,
        bucket_name: &str,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexRequest, Status> {
        Ok(QueryIndexRequest {
            bucket_name: bucket_name.to_string(),
            index_name: self.index.name.clone(),
            query_text: self.query_text.clone(),
            query_vector: self.query_vector.clone(),
            limit: self.limit,
            phrase: self.phrase,
            path_prefix: self.path_prefix.clone(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: serde_json::to_string(&self.boundary_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_predicates_json: serde_json::to_string(&self.typed_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_order_json: serde_json::to_string(&self.typed_order)
                .map_err(|e| Status::internal(e.to_string()))?,
            page_token: page_token.to_string(),
            require_caught_up_to_watch_cursor: self.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        })
    }
}

pub(super) fn default_query_source_kind() -> String {
    "object_current".to_string()
}

impl AnvilQuerySpec {
    pub(super) fn parse(raw: &str) -> Result<Self, Status> {
        let spec: Self = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid QuerySpec JSON: {e}")))?;
        if spec.schema != "anvil.query.spec.v1" {
            return Err(Status::invalid_argument("Invalid QuerySpec schema"));
        }
        if let Some(mesh_id) = spec.scope.mesh_id.as_deref()
            && mesh_id.trim().is_empty()
        {
            return Err(Status::invalid_argument("QuerySpec scope.mesh_id is empty"));
        }
        if let Some(authz_scope) = spec.scope.authz_scope.as_ref()
            && !authz_scope.is_object()
        {
            return Err(Status::invalid_argument(
                "QuerySpec scope.authz_scope must be an object",
            ));
        }
        Ok(spec)
    }

    pub(super) fn canonical_json(&self) -> Result<String, Status> {
        serde_json::to_string(
            &serde_json::to_value(self)
                .map_err(|e| Status::internal(format!("Serialize QuerySpec: {e}")))?,
        )
        .map_err(|e| Status::internal(format!("Canonicalize QuerySpec: {e}")))
    }

    pub(super) fn shape(&self) -> Result<QuerySpecShape, Status> {
        let mut path_prefix = None;
        let mut boundary_predicates = Vec::new();
        let mut typed_predicates = Vec::new();
        let mut query_text = None;
        let mut query_vector = None;
        let mut phrase = false;
        let mut can_relation = None;

        for predicate in &self.predicates.all {
            if let Some(value) = predicate.get("path_prefix").and_then(JsonValue::as_str) {
                path_prefix = Some(value.to_string());
                continue;
            }
            if let Some(boundary) = predicate.get("boundary") {
                if !boundary.is_object() {
                    return Err(Status::invalid_argument(
                        "QuerySpec boundary predicate must be an object",
                    ));
                }
                boundary_predicates.push(page_token_canonical_json(boundary));
                continue;
            }
            if let Some(field) = predicate.get("field").and_then(JsonValue::as_str) {
                let op = predicate
                    .get("op")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec field predicate requires op")
                    })?;
                let value = predicate
                    .get("value")
                    .or_else(|| predicate.get("values"))
                    .cloned()
                    .unwrap_or(JsonValue::Null);
                let values = match value {
                    JsonValue::Array(values) if op.eq_ignore_ascii_case("in") => values,
                    other => vec![other],
                };
                typed_predicates.push(serde_json::json!({
                    "field": field,
                    "op": op,
                    "values": values,
                }));
                continue;
            }
            if let Some(full_text) = predicate.get("full_text") {
                let query = full_text
                    .get("query")
                    .and_then(JsonValue::as_str)
                    .ok_or_else(|| {
                        Status::invalid_argument("QuerySpec full_text predicate requires query")
                    })?;
                query_text = Some(query.to_string());
                phrase = full_text
                    .get("phrase")
                    .and_then(JsonValue::as_bool)
                    .unwrap_or(false);
                continue;
            }
            if let Some(vector) = predicate.get("vector") {
                let near = vector
                    .get("near")
                    .and_then(JsonValue::as_array)
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "QuerySpec vector.near must be an inline numeric vector",
                        )
                    })?;
                let mut values = Vec::with_capacity(near.len());
                for value in near {
                    let Some(value) = value.as_f64() else {
                        return Err(Status::invalid_argument(
                            "QuerySpec vector.near values must be numeric",
                        ));
                    };
                    values.push(value as f32);
                }
                query_vector = Some(values);
                continue;
            }
            if let Some(can) = predicate.get("can") {
                let relation =
                    can.get("relation")
                        .and_then(JsonValue::as_str)
                        .ok_or_else(|| {
                            Status::invalid_argument("QuerySpec can predicate requires relation")
                        })?;
                can_relation = Some(relation.to_string());
                continue;
            }
            return Err(Status::invalid_argument("Unsupported QuerySpec predicate"));
        }

        let typed_order = self
            .order_by
            .iter()
            .map(|order| {
                let direction = order.direction.to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "QuerySpec order_by direction must be asc or desc",
                    ));
                }
                Ok(TypedOrder {
                    field: order.field.clone(),
                    direction,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QuerySpecShape {
            source_kind: self.source_kind.clone(),
            authz_scope: self.scope.authz_scope.clone(),
            boundary_predicates,
            path_prefix,
            typed_predicates,
            typed_order,
            query_text,
            query_vector,
            phrase,
            can_relation,
            min_source_cursor: parse_optional_u64_json(
                self.consistency.min_source_cursor.as_ref(),
                "min_source_cursor",
            )?,
            min_authz_revision: parse_optional_u64_json(
                self.consistency.min_authz_revision.as_ref(),
                "min_authz_revision",
            )?,
            limit: self.limit.unwrap_or(100),
        })
    }
}

pub(super) fn parse_optional_u64_json(
    value: Option<&JsonValue>,
    label: &str,
) -> Result<Option<u64>, Status> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => number
            .as_u64()
            .ok_or_else(|| Status::invalid_argument(format!("QuerySpec {label} must be u64")))
            .map(Some),
        Some(JsonValue::String(value)) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| Status::invalid_argument(format!("QuerySpec {label} must be u64"))),
        _ => Err(Status::invalid_argument(format!(
            "QuerySpec {label} must be a string or integer"
        ))),
    }
}

pub(super) fn select_query_spec_indexes(
    indexes: &[crate::persistence::IndexDefinition],
    shape: &QuerySpecShape,
    accept_degraded: bool,
) -> Result<QuerySpecIndexSelection, Status> {
    let needs_typed = !shape.typed_predicates.is_empty()
        || !shape.boundary_predicates.is_empty()
        || !shape.typed_order.is_empty();
    let needs_text = shape.query_text.is_some();
    let needs_vector = shape.query_vector.is_some();
    let needs_path_only =
        shape.path_prefix.is_some() && !needs_typed && !needs_text && !needs_vector;

    let typed_filter = if needs_typed {
        Some(
            indexes
                .iter()
                .filter(|index| index.enabled && index.kind == "typed_json")
                .find(|index| typed_json_index_covers(index, shape).unwrap_or(false))
                .cloned()
                .ok_or_else(|| {
                    Status::failed_precondition(
                        "QuerySpec has no typed_json index covering predicates",
                    )
                })?,
        )
    } else {
        None
    };

    if needs_text && needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "hybrid")
            .cloned()
            .ok_or_else(|| {
                Status::failed_precondition("QuerySpec text+vector plan requires a hybrid index")
            })?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if let Some(typed_filter) = typed_filter.clone()
        && !needs_text
        && !needs_vector
    {
        return Ok(QuerySpecIndexSelection {
            primary: typed_filter,
            typed_filter: None,
        });
    }
    if needs_text {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "full_text")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no full_text index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_vector {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "vector")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no vector index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter,
        });
    }
    if needs_path_only {
        let primary = indexes
            .iter()
            .find(|index| index.enabled && index.kind == "path")
            .cloned()
            .ok_or_else(|| Status::failed_precondition("QuerySpec has no path index"))?;
        return Ok(QuerySpecIndexSelection {
            primary,
            typed_filter: None,
        });
    }
    if accept_degraded {
        return Err(Status::failed_precondition(
            "QuerySpec degraded full-scan fallback is forbidden",
        ));
    }
    Err(Status::failed_precondition(
        "QuerySpec requires at least one bounded primitive predicate",
    ))
}

pub(super) fn typed_json_index_covers(
    index: &crate::persistence::IndexDefinition,
    shape: &QuerySpecShape,
) -> Result<bool, Status> {
    let definition = TypedJsonIndexDefinition::from_index(index)?;
    if definition.source_kind != shape.source_kind {
        return Ok(false);
    }
    let fields = definition
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for predicate in &shape.typed_predicates {
        let Some(field) = predicate.get("field").and_then(JsonValue::as_str) else {
            return Ok(false);
        };
        if !fields.contains(field) {
            return Ok(false);
        }
    }
    for predicate in &shape.boundary_predicates {
        let Some(field) = boundary_predicate_field(predicate) else {
            return Ok(false);
        };
        if !fields.contains(field) {
            return Ok(false);
        }
    }
    for order in &shape.typed_order {
        if !fields.contains(order.field.as_str()) {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(super) fn boundary_predicate_field(predicate: &JsonValue) -> Option<&str> {
    let direct = predicate
        .get("dimension")
        .and_then(JsonValue::as_str)
        .or_else(|| predicate.get("name").and_then(JsonValue::as_str))
        .or_else(|| predicate.get("field").and_then(JsonValue::as_str));
    if direct.is_some() {
        return direct;
    }
    for key in ["eq", "in", "range", "prefix", "exists"] {
        if let Some(node) = predicate.get(key) {
            if let Some(field) = node.as_str() {
                return Some(field);
            }
            if let Some(field) = boundary_predicate_field(node) {
                return Some(field);
            }
        }
    }
    None
}

#[derive(Debug, Clone, Default)]
pub(super) struct QueryObjectRef {
    pub(super) object_version_id: String,
    pub(super) object_key: String,
    pub(super) user_meta: Option<JsonValue>,
    pub(super) created_at_nanos: i64,
    pub(super) authz_revision: i64,
}

impl QueryObjectRef {
    pub(super) fn from_typed_field_row(
        row: &typed_field_segment::TypedFieldSegmentRow,
    ) -> Result<Self, Status> {
        let user_meta = JsonValue::Object(
            row.values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        );
        Ok(Self {
            object_version_id: row.object_version_id.clone(),
            object_key: row.object_key.clone(),
            user_meta: Some(user_meta),
            created_at_nanos: 0,
            authz_revision: i64::try_from(row.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryAuthzScope {
    pub(super) realm_id: String,
    pub(super) object_namespace: String,
    pub(super) relation: String,
    pub(super) authorization_mode: String,
    pub(super) principal_hash: String,
    pub(super) scope_hash: String,
    pub(super) revision: u64,
    pub(super) system_revision: u64,
}

impl QueryAuthzScope {
    pub(super) fn for_bucket(
        config: &Config,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        authorization_mode: &str,
        requested_relation: &str,
        explicit_scope: Option<&JsonValue>,
        revision: u64,
        system_revision: u64,
    ) -> Self {
        let relation = if requested_relation == "read" {
            "reader"
        } else {
            requested_relation
        };
        let object_namespace = encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object");
        let principal_hash = stable_string_hash(&claims.sub);
        let scope_shape = serde_json::json!({
            "schema": "anvil.query.authz_scope.v1",
            "mesh_id": config.mesh_id,
            "realm_id": DEFAULT_AUTHZ_REALM_ID,
            "object_namespace": object_namespace,
            "relation": relation,
            "authorization_mode": authorization_mode,
            "tenant_id": claims.tenant_id,
            "bucket_id": bucket.id,
            "bucket_name": bucket.name,
            "principal_hash": principal_hash,
            "explicit_scope": explicit_scope.cloned().unwrap_or(JsonValue::Null),
            "revision": revision,
            "system_revision": system_revision,
        });
        Self {
            realm_id: DEFAULT_AUTHZ_REALM_ID.to_string(),
            object_namespace,
            relation: relation.to_string(),
            authorization_mode: authorization_mode.to_string(),
            principal_hash,
            scope_hash: stable_prefixed_json_hash(&scope_shape),
            revision,
            system_revision,
        }
    }

    pub(super) fn trace_json(&self) -> JsonValue {
        serde_json::json!({
            "realm_id": self.realm_id.clone(),
            "object_namespace": self.object_namespace.clone(),
            "relation": self.relation.clone(),
            "authorization_mode": self.authorization_mode.clone(),
            "principal_hash": self.principal_hash.clone(),
            "scope_hash": self.scope_hash.clone(),
            "revision": self.revision,
            "system_revision": self.system_revision,
        })
    }

    pub(super) fn revision_fence(&self) -> u64 {
        self.revision.max(self.system_revision)
    }
}

pub(super) fn authz_label_filter_for_index_candidate_set(
    _authorization_mode: &str,
    _indexed_authz_revision: u64,
    _query_authz_revision: u64,
) -> Result<Option<&'static BTreeSet<[u8; 32]>>, Status> {
    // Label filters are an optimisation hint only and must not replace Zanzibar
    // visibility checks. Returning None keeps the candidate set broad; callers
    // must run query_hit_visible() before returning a hit.
    Ok(None)
}

#[derive(Debug, Clone, Default)]
pub(super) struct QueryFilters {
    pub(super) path_prefix: Option<String>,
    pub(super) metadata: Vec<MetadataFilter>,
}

#[derive(Debug, Clone)]
pub(super) struct MetadataFilter {
    pub(super) field: String,
    pub(super) expected: JsonValue,
}

impl QueryFilters {
    pub(super) fn from_request(req: &QueryIndexRequest) -> Result<Self, Status> {
        let path_prefix = if req.path_prefix.trim().is_empty() {
            None
        } else {
            Some(req.path_prefix.clone())
        };
        let metadata = parse_metadata_filters(&req.metadata_filters_json)?;
        Ok(Self {
            path_prefix,
            metadata,
        })
    }

    pub(super) fn matches(&self, object_ref: &QueryObjectRef) -> Result<bool, Status> {
        if let Some(path_prefix) = &self.path_prefix
            && !object_ref.object_key.starts_with(path_prefix)
        {
            return Ok(false);
        }
        if self.metadata.is_empty() {
            return Ok(true);
        }
        let Some(metadata) = object_ref.user_meta.as_ref() else {
            return Ok(false);
        };
        for filter in &self.metadata {
            let Some(actual) = metadata_filter_value(metadata, &filter.field) else {
                return Ok(false);
            };
            if actual != &filter.expected {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedJsonIndexDefinition {
    pub(super) source_kind: String,
    pub(super) fields: Vec<TypedFieldDefinition>,
    pub(super) default_order: Vec<TypedOrder>,
}

#[derive(Debug, Clone)]
pub(super) struct TypedFieldDefinition {
    pub(super) name: String,
}

impl TypedJsonIndexDefinition {
    pub(super) fn from_index(index: &crate::persistence::IndexDefinition) -> Result<Self, Status> {
        let source_kind = json_optional_string_field(&index.build_policy, "source_kind")
            .or_else(|| json_optional_string_field(&index.build_policy, "source"))
            .unwrap_or_else(|| "object_current".to_string());
        let fields_json = index
            .build_policy
            .get("fields")
            .or_else(|| index.extractor.get("fields"))
            .ok_or_else(|| Status::invalid_argument("typed_json index requires fields"))?;
        let JsonValue::Array(field_values) = fields_json else {
            return Err(Status::invalid_argument(
                "typed_json fields must be an array",
            ));
        };
        let mut fields = Vec::with_capacity(field_values.len());
        for value in field_values {
            let name = json_optional_string_field(value, "name")
                .ok_or_else(|| Status::invalid_argument("typed_json field requires name"))?;
            let extractor = json_optional_string_field(value, "extractor")
                .or_else(|| json_optional_string_field(value, "json_pointer"))
                .ok_or_else(|| Status::invalid_argument("typed_json field requires extractor"))?;
            validate_typed_extractor(&source_kind, &extractor)?;
            fields.push(TypedFieldDefinition { name });
        }
        let default_order = index
            .build_policy
            .get("default_order")
            .map(TypedOrder::parse_json_array)
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            source_kind,
            fields,
            default_order,
        })
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedIndexRow {
    pub(super) object_key: String,
    pub(super) object_version_id: String,
    pub(super) source_identity: String,
    pub(super) values: BTreeMap<String, JsonValue>,
}

impl TypedIndexRow {
    pub(super) fn from_segment_row(row: typed_field_segment::TypedFieldSegmentRow) -> Self {
        Self {
            object_key: row.object_key,
            object_version_id: row.object_version_id,
            source_identity: row.source_identity,
            values: row.values,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct TypedPredicate {
    pub(super) field: String,
    pub(super) op: String,
    pub(super) values: Vec<JsonValue>,
}

impl TypedPredicate {
    pub(super) fn parse_list(raw: &str) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(Vec::new());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_predicates_json: {e}")))?;
        let JsonValue::Array(items) = parsed else {
            return Err(Status::invalid_argument(
                "typed_predicates_json must be an array",
            ));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires field"))?;
                let op = json_optional_string_field(item, "op")
                    .or_else(|| json_optional_string_field(item, "operator"))
                    .ok_or_else(|| Status::invalid_argument("typed predicate requires op"))?
                    .to_ascii_lowercase();
                let values = if let Some(values) = item.get("values").and_then(JsonValue::as_array)
                {
                    values.clone()
                } else if let Some(value) = item.get("value") {
                    vec![value.clone()]
                } else {
                    Vec::new()
                };
                Ok(Self { field, op, values })
            })
            .collect()
    }

    pub(super) fn matches(&self, row: &TypedIndexRow) -> bool {
        let actual = row.values.get(&self.field).unwrap_or(&JsonValue::Null);
        match self.op.as_str() {
            "eq" | "=" | "==" => self
                .values
                .first()
                .is_some_and(|expected| actual == expected),
            "in" => self.values.iter().any(|expected| actual == expected),
            "lt" | "<" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_lt()),
            "lte" | "<=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_gt()),
            "gt" | ">" => self
                .values
                .first()
                .is_some_and(|expected| compare_json_values(actual, expected).is_gt()),
            "gte" | ">=" => self
                .values
                .first()
                .is_some_and(|expected| !compare_json_values(actual, expected).is_lt()),
            "prefix" => self.values.first().is_some_and(|expected| {
                actual
                    .as_str()
                    .zip(expected.as_str())
                    .is_some_and(|(actual, prefix)| actual.starts_with(prefix))
            }),
            "exists" => !actual.is_null(),
            "is_null" => actual.is_null(),
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct TypedOrder {
    pub(super) field: String,
    #[serde(default = "default_ascending")]
    pub(super) direction: String,
}

impl TypedOrder {
    pub(super) fn parse_list(raw: &str, default_order: &[TypedOrder]) -> Result<Vec<Self>, Status> {
        if raw.trim().is_empty() {
            return Ok(default_order.to_vec());
        }
        let parsed: JsonValue = serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid typed_order_json: {e}")))?;
        Self::parse_json_array(&parsed)
    }

    pub(super) fn parse_json_array(value: &JsonValue) -> Result<Vec<Self>, Status> {
        let JsonValue::Array(items) = value else {
            return Err(Status::invalid_argument("typed order must be an array"));
        };
        items
            .iter()
            .map(|item| {
                let field = json_optional_string_field(item, "field")
                    .or_else(|| json_optional_string_field(item, "field_name"))
                    .ok_or_else(|| Status::invalid_argument("typed order requires field"))?;
                let direction = json_optional_string_field(item, "direction")
                    .unwrap_or_else(|| "asc".to_string())
                    .to_ascii_lowercase();
                if direction != "asc" && direction != "desc" {
                    return Err(Status::invalid_argument(
                        "typed order direction must be asc or desc",
                    ));
                }
                Ok(Self { field, direction })
            })
            .collect()
    }
}

pub(super) fn compare_typed_rows(
    left: &TypedIndexRow,
    right: &TypedIndexRow,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            left.values.get(&term.field).unwrap_or(&JsonValue::Null),
            right.values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    left.source_identity.cmp(&right.source_identity)
}

pub(super) fn compare_typed_row_to_cursor(
    row: &TypedIndexRow,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
    order: &[TypedOrder],
) -> std::cmp::Ordering {
    for term in order {
        let ordering = compare_json_values(
            row.values.get(&term.field).unwrap_or(&JsonValue::Null),
            cursor_values.get(&term.field).unwrap_or(&JsonValue::Null),
        );
        let ordering = if term.direction == "desc" {
            ordering.reverse()
        } else {
            ordering
        };
        if !ordering.is_eq() {
            return ordering;
        }
    }
    row.source_identity.as_str().cmp(cursor_source_identity)
}

pub(super) fn compare_json_values(left: &JsonValue, right: &JsonValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (JsonValue::Number(left), JsonValue::Number(right)) => left
            .as_f64()
            .partial_cmp(&right.as_f64())
            .unwrap_or(Ordering::Equal),
        (JsonValue::String(left), JsonValue::String(right)) => left.cmp(right),
        (JsonValue::Bool(left), JsonValue::Bool(right)) => left.cmp(right),
        (JsonValue::Null, JsonValue::Null) => Ordering::Equal,
        (JsonValue::Null, _) => Ordering::Less,
        (_, JsonValue::Null) => Ordering::Greater,
        _ => left.to_string().cmp(&right.to_string()),
    }
}

pub(super) fn json_optional_string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

pub(super) fn default_ascending() -> String {
    "asc".to_string()
}

pub(super) fn validate_typed_extractor(source_kind: &str, extractor: &str) -> Result<(), Status> {
    let pointer_valid = |value: &str| value.starts_with('/');
    match (source_kind, extractor) {
        (_, "created_at") => Ok(()),
        ("object_current" | "object_version", "object_key" | "object_content_type") => Ok(()),
        ("object_current" | "object_version", value) if pointer_valid(value) => Ok(()),
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_body_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        (
            "append_record",
            "append_stream_key" | "append_record_sequence" | "append_content_type",
        ) => Ok(()),
        ("append_record", value) if pointer_valid(value) => Ok(()),
        ("append_record", value)
            if value
                .strip_prefix("append_payload_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("append_record", value)
            if value
                .strip_prefix("append_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        _ => Err(Status::invalid_argument(
            "Invalid typed_json field extractor",
        )),
    }
}

pub(super) fn stable_json_hash(raw: &str) -> String {
    let canonical = if raw.trim().is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_str(raw).unwrap_or(JsonValue::String(raw.to_string()))
    };
    blake3::hash(canonical.to_string().as_bytes())
        .to_hex()
        .to_string()
}

pub(super) fn stable_prefixed_json_hash(value: &JsonValue) -> String {
    let canonical = page_token_canonical_json(value);
    format!(
        "blake3:{}",
        blake3::hash(canonical.to_string().as_bytes()).to_hex()
    )
}

pub(super) fn stable_string_hash(value: &str) -> String {
    format!("blake3:{}", blake3::hash(value.as_bytes()).to_hex())
}

pub(super) fn stable_json_hash_checked(raw: &str, field_name: &str) -> Result<String, Status> {
    let canonical = if raw.trim().is_empty() {
        JsonValue::Null
    } else {
        serde_json::from_str(raw)
            .map_err(|e| Status::invalid_argument(format!("Invalid {field_name}: {e}")))?
    };
    Ok(stable_prefixed_json_hash(&canonical))
}

pub(super) fn authz_aware_query_scope_hash(
    hash_kind: &str,
    authz_scope: &QueryAuthzScope,
    shape: JsonValue,
) -> String {
    stable_prefixed_json_hash(&serde_json::json!({
        "schema": "anvil.query.scope_hash.v1",
        "hash_kind": hash_kind,
        "authz": {
            "realm_id": authz_scope.realm_id.clone(),
            "object_namespace": authz_scope.object_namespace.clone(),
            "relation": authz_scope.relation.clone(),
            "authorization_mode": authz_scope.authorization_mode.clone(),
            "principal_hash": authz_scope.principal_hash.clone(),
            "scope_hash": authz_scope.scope_hash.clone(),
            "revision": authz_scope.revision,
        },
        "shape": shape,
    }))
}

pub(super) fn ensure_algorithm_prefixed_hash(value: &str, field_name: &str) -> Result<(), Status> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        return Err(Status::invalid_argument(format!(
            "{field_name} must be algorithm-prefixed"
        )));
    };
    let expected_len = match algorithm {
        "blake3" | "sha256" => 64,
        _ => {
            return Err(Status::invalid_argument(format!(
                "{field_name} uses unsupported hash algorithm"
            )));
        }
    };
    if digest.len() != expected_len
        || !digest
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Status::invalid_argument(format!(
            "{field_name} has invalid hash digest"
        )));
    }
    Ok(())
}

pub(super) fn ensure_no_direct_boundary_predicates(req: &QueryIndexRequest) -> Result<(), Status> {
    if BoundaryPredicate::parse_list(&req.boundary_predicates_json)?.is_empty() {
        Ok(())
    } else {
        Err(Status::failed_precondition("IndexCapabilityMissing"))
    }
}

pub(super) fn metadata_backed_predicate_hash(
    index_kind: &str,
    req: &QueryIndexRequest,
    authz_scope: &QueryAuthzScope,
) -> Result<String, Status> {
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash_checked(
            &req.metadata_filters_json,
            "metadata_filters_json"
        )?,
        "boundary_predicates_hash": stable_json_hash_checked(
            &req.boundary_predicates_json,
            "boundary_predicates_json"
        )?,
    });
    Ok(authz_aware_query_scope_hash(
        "predicate",
        authz_scope,
        shape,
    ))
}

pub(super) fn typed_json_predicate_hash(
    req: &QueryIndexRequest,
    authz_scope: &QueryAuthzScope,
) -> Result<String, Status> {
    let shape = serde_json::json!({
        "path_prefix": req.path_prefix,
        "typed_predicates_hash": stable_json_hash_checked(
            &req.typed_predicates_json,
            "typed_predicates_json"
        )?,
        "boundary_predicates_hash": stable_json_hash_checked(
            &req.boundary_predicates_json,
            "boundary_predicates_json"
        )?,
    });
    Ok(authz_aware_query_scope_hash(
        "predicate",
        authz_scope,
        shape,
    ))
}

pub(super) fn score_based_predicate_hash(
    index_kind: &str,
    req: &QueryIndexRequest,
    authz_scope: &QueryAuthzScope,
) -> Result<String, Status> {
    let query_vector_bits = req
        .query_vector
        .iter()
        .map(|value| value.to_bits())
        .collect::<Vec<_>>();
    let shape = serde_json::json!({
        "index_kind": index_kind,
        "query_text": req.query_text,
        "query_vector_bits": query_vector_bits,
        "phrase": req.phrase,
        "path_prefix": req.path_prefix,
        "metadata_filters_hash": stable_json_hash_checked(
            &req.metadata_filters_json,
            "metadata_filters_json"
        )?,
        "boundary_predicates_hash": stable_json_hash_checked(
            &req.boundary_predicates_json,
            "boundary_predicates_json"
        )?,
        "typed_predicates_hash": stable_json_hash_checked(
            &req.typed_predicates_json,
            "typed_predicates_json"
        )?,
    });
    Ok(authz_aware_query_scope_hash(
        "predicate",
        authz_scope,
        shape,
    ))
}

pub(super) fn score_order_hash(authz_scope: &QueryAuthzScope) -> String {
    authz_aware_query_scope_hash(
        "order",
        authz_scope,
        serde_json::json!({
            "schema": "anvil.query.order.v1",
            "terms": [
                {"field": "score", "direction": "desc"},
                {"field": "object_version_id", "direction": "asc"},
            ],
        }),
    )
}

pub(super) fn object_key_order_hash(authz_scope: &QueryAuthzScope) -> String {
    authz_aware_query_scope_hash(
        "order",
        authz_scope,
        serde_json::json!({
            "schema": "anvil.query.order.v1",
            "terms": [
                {"field": "object_key", "direction": "asc"},
                {"field": "source_identity", "direction": "asc"},
            ],
        }),
    )
}

pub(super) fn typed_order_hash(
    order: &[TypedOrder],
    authz_scope: &QueryAuthzScope,
) -> Result<String, Status> {
    Ok(authz_aware_query_scope_hash(
        "order",
        authz_scope,
        serde_json::json!({
            "schema": "anvil.query.order.v1",
            "terms": serde_json::to_value(order)
                .map_err(|e| Status::internal(format!("Serialize typed order: {e}")))?,
        }),
    ))
}

pub(super) fn record_query_plan_metrics(
    index_kind: &str,
    authz_mode: &str,
    input_candidate_count: u64,
    boundary_candidate_count: u64,
    authz_candidate_count: u64,
    index_candidate_count: u64,
    intersection_candidate_count: u64,
) {
    let labels = [("index_kind", index_kind), ("authz_mode", authz_mode)];
    crate::perf::record_counter(
        "anvil_query_input_candidate_count",
        &labels,
        input_candidate_count,
    );
    crate::perf::record_counter(
        "anvil_query_boundary_candidate_count",
        &labels,
        boundary_candidate_count,
    );
    crate::perf::record_counter(
        "anvil_query_authz_candidate_count",
        &labels,
        authz_candidate_count,
    );
    crate::perf::record_counter(
        "anvil_query_index_candidate_count",
        &labels,
        index_candidate_count,
    );
    crate::perf::record_counter(
        "anvil_query_intersection_candidate_count",
        &labels,
        intersection_candidate_count,
    );
    crate::perf::record_duration(
        "anvil_query_plan_duration_ms",
        &[
            ("index_kind", index_kind),
            (
                "authz_pruned",
                if authz_candidate_count < boundary_candidate_count {
                    "true"
                } else {
                    "false"
                },
            ),
            (
                "boundary_pruned",
                if boundary_candidate_count < input_candidate_count {
                    "true"
                } else {
                    "false"
                },
            ),
        ],
        Duration::ZERO,
    );
    crate::perf::record_float_gauge(
        "anvil_boundary_prune_ratio",
        &[
            ("boundary_dimension", "query"),
            ("writer_family", index_kind),
            ("index_kind", index_kind),
        ],
        prune_ratio(input_candidate_count, boundary_candidate_count),
    );
    crate::perf::record_float_gauge(
        "anvil_authz_candidate_prune_ratio",
        &[
            ("object_namespace", "object"),
            ("relation", authz_mode),
            ("index_kind", index_kind),
        ],
        prune_ratio(boundary_candidate_count, authz_candidate_count),
    );
    crate::perf::record_trace_event(crate::perf::TraceEvent {
        trace_id: "query-plan",
        span_id: "boundary-prune",
        parent_span_id: None,
        request_id: None,
        component: "query",
        operation: "query.boundary_prune",
        writer_family: Some(index_kind),
        bucket_hash: None,
        boundary_schema_generation: None,
        duration: Duration::ZERO,
        bytes_in: input_candidate_count,
        bytes_out: boundary_candidate_count,
        fsync_count: 0,
        status: "ok",
    });
    crate::perf::record_trace_event(crate::perf::TraceEvent {
        trace_id: "query-plan",
        span_id: "authz-prune",
        parent_span_id: Some("boundary-prune"),
        request_id: None,
        component: "query",
        operation: "query.authz_prune",
        writer_family: Some(index_kind),
        bucket_hash: None,
        boundary_schema_generation: None,
        duration: Duration::ZERO,
        bytes_in: boundary_candidate_count,
        bytes_out: authz_candidate_count,
        fsync_count: 0,
        status: "ok",
    });
}

fn prune_ratio(before: u64, after: u64) -> f64 {
    if before == 0 {
        return 0.0;
    }
    let pruned = before.saturating_sub(after);
    pruned as f64 / before as f64
}

pub(super) fn score_sort_values(
    score: f32,
    object_version_id: &str,
) -> BTreeMap<String, JsonValue> {
    let mut values = BTreeMap::new();
    values.insert(
        "score".to_string(),
        serde_json::Number::from_f64(f64::from(score))
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
    );
    values.insert(
        "object_version_id".to_string(),
        JsonValue::String(object_version_id.to_string()),
    );
    values
}

pub(super) fn score_after_cursor(
    score: f32,
    object_version_id: &str,
    token: &IndexPageToken,
) -> Result<bool, Status> {
    let cursor_score = token
        .last_sort_values
        .get("score")
        .and_then(JsonValue::as_f64)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))? as f32;
    let cursor_object_version_id = token
        .last_sort_values
        .get("object_version_id")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| Status::invalid_argument("InvalidPageToken"))?;
    Ok(score
        .partial_cmp(&cursor_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .is_lt()
        || (score == cursor_score && object_version_id > cursor_object_version_id))
}

pub(super) fn compare_score_hits(
    left_score: f32,
    left_object_version_id: &str,
    right_score: f32,
    right_object_version_id: &str,
) -> std::cmp::Ordering {
    right_score
        .partial_cmp(&left_score)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left_object_version_id.cmp(right_object_version_id))
}

pub(super) fn object_key_after_cursor(
    object_key: &str,
    source_identity: &str,
    cursor_values: &BTreeMap<String, JsonValue>,
    cursor_source_identity: &str,
) -> bool {
    let cursor_key = cursor_values
        .get("object_key")
        .and_then(JsonValue::as_str)
        .unwrap_or_default();
    object_key
        .cmp(cursor_key)
        .then(source_identity.cmp(cursor_source_identity))
        .is_gt()
}

pub(super) fn parse_metadata_filters(value: &str) -> Result<Vec<MetadataFilter>, Status> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    let parsed: JsonValue = serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid metadata_filters_json: {e}")))?;
    if parsed.is_null() {
        return Ok(Vec::new());
    }
    let JsonValue::Object(entries) = parsed else {
        return Err(Status::invalid_argument(
            "metadata_filters_json must be a JSON object",
        ));
    };
    let mut filters = Vec::with_capacity(entries.len());
    for (field, expected) in entries {
        if field.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json field names must not be empty",
            ));
        }
        filters.push(MetadataFilter { field, expected });
    }
    Ok(filters)
}

pub(super) fn metadata_filter_value<'a>(
    metadata: &'a JsonValue,
    field: &str,
) -> Option<&'a JsonValue> {
    if field.starts_with('/') {
        metadata.pointer(field)
    } else {
        metadata.get(field)
    }
}
