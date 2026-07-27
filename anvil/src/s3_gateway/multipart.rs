use super::*;

#[derive(Debug, Deserialize)]
pub(super) struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    parts: Vec<CompleteMultipartUploadXmlPart>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CompleteMultipartUploadXmlPart {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}

pub(super) async fn list_multipart_parts_response(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    q: &HashMap<String, String>,
) -> Response {
    let part_number_marker: i32 = q
        .get("part-number-marker")
        .or_else(|| q.get("partNumberMarker"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let max_parts: i32 = q
        .get("max-parts")
        .or_else(|| q.get("maxParts"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    match state
        .object_manager
        .list_multipart_parts(
            &claims,
            &bucket,
            &key,
            upload_id,
            part_number_marker,
            max_parts,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n  <PartNumberMarker>{}</PartNumberMarker>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                upload_id,
                part_number_marker
            );
            if let Some(next_part_number_marker) = page.next_part_number_marker {
                xml.push_str(&format!(
                    "  <NextPartNumberMarker>{next_part_number_marker}</NextPartNumberMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <MaxParts>{}</MaxParts>\n  <IsTruncated>{}</IsTruncated>\n",
                max_parts,
                if page.is_truncated { "true" } else { "false" }
            ));
            for part in page.parts {
                xml.push_str("  <Part>\n");
                xml.push_str(&format!(
                    "    <PartNumber>{}</PartNumber>\n",
                    part.part_number
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    part.created_at.to_rfc3339()
                ));
                xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", part.etag));
                xml.push_str(&format!("    <Size>{}</Size>\n", part.size));
                xml.push_str("  </Part>\n");
            }
            xml.push_str("</ListPartsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) async fn initiate_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
) -> Response {
    match state
        .object_manager
        .initiate_multipart_upload(&claims, &bucket, &key, None, None)
        .await
    {
        Ok(result) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                result.upload_id
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) async fn upload_part(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    part_number: i32,
    body_stream: impl Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + Unpin,
) -> Response {
    match state
        .object_manager
        .upload_part(
            &claims,
            &bucket,
            &key,
            upload_id,
            part_number,
            body_stream,
            None,
            None,
        )
        .await
    {
        Ok(result) => Response::builder()
            .status(200)
            .header("ETag", format!("\"{}\"", result.etag))
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

pub(super) async fn complete_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    body: axum::body::Bytes,
) -> Response {
    let completed = match quick_xml::de::from_reader::<_, CompleteMultipartUploadXml>(&body[..]) {
        Ok(completed) => completed,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid CompleteMultipartUpload body: {}", error),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };
    let parts = completed
        .parts
        .into_iter()
        .map(|part| anvil_core::object_manager::CompleteMultipartPart {
            part_number: part.part_number,
            etag: part.etag,
        })
        .collect();

    let (transaction, principal) = match begin_s3_write_transaction(
        &state,
        &claims,
        "complete-multipart",
        &bucket,
        &key,
    )
    .await
    {
        Ok(binding) => binding,
        Err(status) => {
            return s3_status_to_response_for_auth(
                status,
                true,
                "ServiceUnavailable",
                state.config.cross_region_routing_policy,
            );
        }
    };
    let completion = state
        .object_manager
        .complete_multipart_upload(
            &claims,
            &bucket,
            &key,
            upload_id,
            parts,
            Some(&transaction.transaction_id),
            Some(&principal),
        )
        .await;
    match completion {
        Ok(object) => {
            if let Err(status) = commit_s3_write_transaction(&state, &transaction, &principal).await
            {
                return s3_status_to_response_for_auth(
                    status,
                    true,
                    "ServiceUnavailable",
                    state.config.cross_region_routing_policy,
                );
            }
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Location>/{}/{}</Location>\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                xml_escape(&bucket),
                xml_escape(&key),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => {
            let _ = state.mvcc.open_transactions.rollback(
                &transaction.transaction_id,
                &principal,
                s3_now_unix_ms().unwrap_or_default(),
            );
            s3_status_to_response_for_auth(
                status,
                true,
                "NoSuchUpload",
                state.config.cross_region_routing_policy,
            )
        }
    }
}

pub(super) async fn abort_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
) -> Response {
    match state
        .object_manager
        .abort_multipart_upload(&claims, &bucket, &key, upload_id, None, None)
        .await
    {
        Ok(_) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}
