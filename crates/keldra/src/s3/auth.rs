use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation, SigningParams,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use axum::body::Body;
use axum::http::{self, Request};
use subtle::ConstantTimeEq as _;
use time::{Date, Month, PrimitiveDateTime, Time};

use super::{S3Error, S3State};
use crate::authentication::Caller;
use crate::v05::GatewayIdentity;

const MAX_CLOCK_SKEW: Duration = Duration::from_secs(15 * 60);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Authorization {
    access_key: String,
    date: String,
    region: String,
    service: String,
    signed_headers: Vec<String>,
    signature: String,
}

#[derive(Clone, Debug)]
pub(super) struct AwsChunkedVerification {
    pub(super) signing_key: Vec<u8>,
    pub(super) timestamp: String,
    pub(super) credential_scope: String,
    pub(super) previous_signature: String,
}

pub(super) async fn authenticate(
    state: &S3State,
    request: &mut Request<Body>,
) -> Result<Option<GatewayIdentity>, S3Error> {
    state
        .serving
        .require(tonic::Request::new(()))
        .map_err(|error| {
            S3Error::new(
                "ServiceUnavailable",
                error.message(),
                http::StatusCode::SERVICE_UNAVAILABLE,
            )
        })?;
    state.rate_limits.check_gateway_global().map_err(|error| {
        S3Error::new(
            "SlowDown",
            error.message(),
            http::StatusCode::TOO_MANY_REQUESTS,
        )
    })?;
    let Some(authorization) = request_authorization(request.headers())? else {
        return if matches!(*request.method(), http::Method::GET | http::Method::HEAD) {
            Ok(None)
        } else {
            Err(S3Error::access_denied("Missing Authorization"))
        };
    };
    let credential = state
        .control
        .resolve_sigv4_credential(&authorization.access_key)
        .await
        .map_err(|_| S3Error::invalid_access_key())?;
    let envelope = credential.sigv4_secret().ok_or_else(|| {
        S3Error::access_denied(
            "This credential predates S3 signing support; rotate it before using the S3 gateway",
        )
    })?;
    let secret = state
        .tokens
        .open_sigv4_secret(
            credential.storage_tenant(),
            credential.app_id(),
            credential.client_id(),
            envelope,
        )
        .map_err(|_| S3Error::internal("Credential envelope could not be opened"))?;
    let streaming = verify_signature(request, &authorization, &secret)?;
    let caller = Caller::from_authenticated_application(
        credential.storage_tenant().clone(),
        credential.app_id(),
    )
    .map_err(|_| S3Error::internal("Credential identity is invalid"))?;
    state
        .rate_limits
        .check_gateway_identity(&caller)
        .map_err(|error| {
            S3Error::new(
                "SlowDown",
                error.message(),
                http::StatusCode::TOO_MANY_REQUESTS,
            )
        })?;
    let identity = GatewayIdentity::authenticated(&state.tokens, caller)
        .map_err(|_| S3Error::internal("Gateway identity could not be established"))?;
    if let Some(streaming) = streaming {
        request.extensions_mut().insert(streaming);
    }
    Ok(Some(identity))
}

fn request_authorization(headers: &http::HeaderMap) -> Result<Option<Authorization>, S3Error> {
    let values = headers.get_all(http::header::AUTHORIZATION);
    let mut values = values.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(S3Error::signature_mismatch());
    }
    let value = value.to_str().map_err(|_| S3Error::signature_mismatch())?;
    parse_authorization(value).map(Some)
}

fn verify_signature(
    request: &Request<Body>,
    authorization: &Authorization,
    secret: &str,
) -> Result<Option<AwsChunkedVerification>, S3Error> {
    if authorization.service != "s3" {
        return Err(S3Error::signature_mismatch());
    }
    let signing_time = request
        .headers()
        .get("x-amz-date")
        .and_then(|value| value.to_str().ok())
        .and_then(parse_amz_time)
        .ok_or_else(|| S3Error::invalid_request("Missing or invalid x-amz-date"))?;
    if !fresh(signing_time, SystemTime::now(), MAX_CLOCK_SKEW)
        || !request
            .headers()
            .get("x-amz-date")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with(&authorization.date))
    {
        return Err(S3Error::access_denied(
            "Request timestamp is outside the SigV4 window",
        ));
    }
    let host = request
        .headers()
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| S3Error::invalid_request("Host header is required"))?;
    let scheme = request
        .headers()
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| matches!(*value, "http" | "https"))
        .unwrap_or("http");
    let path_query = request
        .uri()
        .path_and_query()
        .map_or("/", |value| value.as_str());
    let url = format!("{scheme}://{host}{path_query}");

    let signed: HashSet<&str> = authorization
        .signed_headers
        .iter()
        .map(String::as_str)
        .collect();
    if !signed.contains("host") || !signed.contains("x-amz-date") {
        return Err(S3Error::signature_mismatch());
    }
    let mut headers = HashMap::<String, String>::new();
    for (name, value) in request.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), value.to_owned());
        }
    }
    headers.insert("host".into(), host.into());
    let selected = headers
        .iter()
        .filter(|(name, _)| signed.contains(name.as_str()))
        .map(|(name, value)| (name.as_str(), value.as_str()));
    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(EMPTY_SHA256);
    let signable = SignableRequest::new(
        request.method().as_str(),
        &url,
        selected,
        SignableBody::Precomputed(payload_hash.to_owned()),
    )
    .map_err(|_| S3Error::invalid_request("Request cannot be canonicalised"))?;
    let identity: Identity = Credentials::new(
        &authorization.access_key,
        secret,
        None,
        None,
        "keldra-sigv4-verify",
    )
    .into();
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::Headers;
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    settings.excluded_headers = Some(vec![Cow::Borrowed("authorization")]);
    let params: SigningParams = v4::SigningParams::builder()
        .identity(&identity)
        .region(&authorization.region)
        .name("s3")
        .time(signing_time)
        .settings(settings)
        .build()
        .map_err(|_| S3Error::signature_mismatch())?
        .into();
    let (_, computed) = sign(signable, &params)
        .map_err(|_| S3Error::signature_mismatch())?
        .into_parts();
    if computed
        .as_str()
        .as_bytes()
        .ct_eq(authorization.signature.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(S3Error::signature_mismatch());
    }
    let payload_hash = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .unwrap_or(EMPTY_SHA256);
    Ok(
        (payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD").then(|| AwsChunkedVerification {
            signing_key: derive_signing_key(
                secret,
                &authorization.date,
                &authorization.region,
                &authorization.service,
            ),
            timestamp: request
                .headers()
                .get("x-amz-date")
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned(),
            credential_scope: format!(
                "{}/{}/{}/aws4_request",
                authorization.date, authorization.region, authorization.service
            ),
            previous_signature: authorization.signature.clone(),
        }),
    )
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac as _};
    use sha2::Sha256;

    fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts every key length");
        mac.update(value);
        mac.finalize().into_bytes().to_vec()
    }

    let date_key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let region_key = hmac(&date_key, region.as_bytes());
    let service_key = hmac(&region_key, service.as_bytes());
    hmac(&service_key, b"aws4_request")
}

fn parse_authorization(value: &str) -> Result<Authorization, S3Error> {
    let fields = value
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(S3Error::signature_mismatch)?;
    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;
    for field in fields.split(',').map(str::trim) {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(S3Error::signature_mismatch)?;
        match name {
            "Credential" => credential = Some(value),
            "SignedHeaders" => signed_headers = Some(value),
            "Signature" => signature = Some(value),
            _ => return Err(S3Error::signature_mismatch()),
        }
    }
    let mut scope = credential
        .ok_or_else(S3Error::signature_mismatch)?
        .split('/');
    let access_key = scope.next().unwrap_or_default();
    let date = scope.next().unwrap_or_default();
    let region = scope.next().unwrap_or_default();
    let service = scope.next().unwrap_or_default();
    if scope.next() != Some("aws4_request")
        || scope.next().is_some()
        || access_key.is_empty()
        || date.len() != 8
        || region.is_empty()
    {
        return Err(S3Error::signature_mismatch());
    }
    let signed_headers = signed_headers
        .ok_or_else(S3Error::signature_mismatch)?
        .split(';')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if signed_headers.is_empty()
        || signed_headers
            .windows(2)
            .any(|pair| pair[0].as_str() >= pair[1].as_str())
    {
        return Err(S3Error::signature_mismatch());
    }
    let signature = signature.ok_or_else(S3Error::signature_mismatch)?;
    if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(S3Error::signature_mismatch());
    }
    Ok(Authorization {
        access_key: access_key.to_owned(),
        date: date.to_owned(),
        region: region.to_owned(),
        service: service.to_owned(),
        signed_headers,
        signature: signature.to_ascii_lowercase(),
    })
}

fn parse_amz_time(value: &str) -> Option<SystemTime> {
    if value.len() != 16 || &value[8..9] != "T" || &value[15..] != "Z" {
        return None;
    }
    let year = value[0..4].parse().ok()?;
    let month = Month::try_from(value[4..6].parse::<u8>().ok()?).ok()?;
    let day = value[6..8].parse().ok()?;
    let hour = value[9..11].parse().ok()?;
    let minute = value[11..13].parse().ok()?;
    let second = value[13..15].parse().ok()?;
    let timestamp = PrimitiveDateTime::new(
        Date::from_calendar_date(year, month, day).ok()?,
        Time::from_hms(hour, minute, second).ok()?,
    )
    .assume_utc()
    .unix_timestamp();
    (timestamp >= 0).then(|| UNIX_EPOCH + Duration::from_secs(timestamp as u64))
}

fn fresh(value: SystemTime, now: SystemTime, tolerance: Duration) -> bool {
    value
        .duration_since(now)
        .or_else(|_| now.duration_since(value))
        .is_ok_and(|difference| difference <= tolerance)
}

pub(super) const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_AUTHORIZATION: &str = "AWS4-HMAC-SHA256 Credential=client/20260804/eu-west-2/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn parses_strict_sigv4_authorization() {
        let parsed = parse_authorization(VALID_AUTHORIZATION).unwrap();
        assert_eq!(parsed.access_key, "client");
        assert_eq!(parsed.region, "eu-west-2");
    }

    #[test]
    fn only_an_absent_authorization_header_selects_anonymous() {
        let headers = http::HeaderMap::new();
        assert_eq!(request_authorization(&headers).unwrap(), None);

        let mut invalid = http::HeaderMap::new();
        invalid.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("not-sigv4"),
        );
        assert!(request_authorization(&invalid).is_err());
    }

    #[test]
    fn malformed_or_duplicate_authorization_headers_fail_closed() {
        let mut malformed = http::HeaderMap::new();
        malformed.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_bytes(&[0xff]).unwrap(),
        );
        assert!(request_authorization(&malformed).is_err());

        let mut duplicate = http::HeaderMap::new();
        duplicate.append(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static(VALID_AUTHORIZATION),
        );
        duplicate.append(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static(VALID_AUTHORIZATION),
        );
        assert!(request_authorization(&duplicate).is_err());
    }

    #[test]
    fn parses_amazon_timestamp_without_accepting_loose_shapes() {
        assert!(parse_amz_time("20260804T123456Z").is_some());
        assert!(parse_amz_time("2026-08-04T12:34:56Z").is_none());
    }
}
