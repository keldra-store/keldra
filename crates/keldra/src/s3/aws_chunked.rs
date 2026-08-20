use axum::body::Body;
use axum::http;
use bytes::{Buf as _, Bytes, BytesMut};
use hmac::{Hmac, Mac as _};
use sha2::{Digest as _, Sha256};
use subtle::ConstantTimeEq as _;
use tokio_stream::StreamExt as _;

use super::S3Error;
use super::auth::AwsChunkedVerification;

const MAX_HEADER_BYTES: usize = 1_024;
const MAX_CHUNK_BYTES: usize = 16 * 1024 * 1024;

pub(super) fn decode(
    body: Body,
    verification: AwsChunkedVerification,
    expected_length: Option<u64>,
) -> Body {
    let mut source = body.into_data_stream();
    let stream = async_stream::try_stream! {
        let mut buffer = BytesMut::new();
        let mut previous_signature = verification.previous_signature.clone();
        let mut decoded_length = 0_u64;
        loop {
            let header_end = loop {
                if let Some(position) = buffer.windows(2).position(|window| window == b"\r\n") {
                    break position;
                }
                if buffer.len() > MAX_HEADER_BYTES {
                    Err(io_error("aws-chunked header exceeds its limit"))?;
                }
                let Some(next) = source.next().await else {
                    Err(io_error("aws-chunked body ended inside a chunk header"))?;
                    unreachable!();
                };
                buffer.extend_from_slice(&next.map_err(|error| io_error(error.to_string()))?);
            };
            let header = std::str::from_utf8(&buffer[..header_end])
                .map_err(|_| io_error("aws-chunked header is not UTF-8"))?;
            let (chunk_length, supplied_signature) = parse_header(header)?;
            buffer.advance(header_end + 2);
            if chunk_length > MAX_CHUNK_BYTES {
                Err(io_error("aws-chunked data chunk exceeds 16 MiB"))?;
            }
            while buffer.len() < chunk_length + 2 {
                let Some(next) = source.next().await else {
                    Err(io_error("aws-chunked body ended inside chunk data"))?;
                    unreachable!();
                };
                buffer.extend_from_slice(&next.map_err(|error| io_error(error.to_string()))?);
            }
            let chunk = buffer.split_to(chunk_length).freeze();
            if &buffer[..2] != b"\r\n" {
                Err(io_error("aws-chunked data is missing its trailing CRLF"))?;
            }
            buffer.advance(2);
            verify_chunk(
                &verification,
                &previous_signature,
                &supplied_signature,
                &chunk,
            )?;
            previous_signature = supplied_signature;
            if chunk_length == 0 {
                require_body_end(&mut source, &mut buffer).await?;
                if let Some(expected_length) = expected_length
                    && decoded_length != expected_length
                {
                    Err(io_error("x-amz-decoded-content-length does not match the payload"))?;
                }
                break;
            }
            decoded_length = decoded_length
                .checked_add(chunk_length as u64)
                .ok_or_else(|| io_error("decoded payload length overflow"))?;
            yield chunk;
        }
    };
    let stream = stream.map(|result: Result<Bytes, std::io::Error>| result);
    Body::from_stream(stream)
}

async fn require_body_end(
    source: &mut axum::body::BodyDataStream,
    buffer: &mut BytesMut,
) -> Result<(), std::io::Error> {
    if !buffer.is_empty() {
        return Err(io_error("aws-chunked body has bytes after its final chunk"));
    }
    while let Some(next) = source.next().await {
        let next = next.map_err(|error| io_error(error.to_string()))?;
        if !next.is_empty() {
            return Err(io_error("aws-chunked body has bytes after its final chunk"));
        }
    }
    Ok(())
}

fn parse_header(value: &str) -> Result<(usize, String), std::io::Error> {
    let mut fields = value.split(';');
    let length = usize::from_str_radix(fields.next().unwrap_or_default(), 16)
        .map_err(|_| io_error("aws-chunked length is invalid"))?;
    let mut signature = None;
    for field in fields {
        if let Some((name, value)) = field.split_once('=')
            && name.eq_ignore_ascii_case("chunk-signature")
        {
            signature = Some(value.to_ascii_lowercase());
        }
    }
    let signature = signature.ok_or_else(|| io_error("aws-chunked signature is missing"))?;
    if signature.len() != 64 || !signature.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(io_error("aws-chunked signature is invalid"));
    }
    Ok((length, signature))
}

fn verify_chunk(
    verification: &AwsChunkedVerification,
    previous_signature: &str,
    supplied_signature: &str,
    chunk: &Bytes,
) -> Result<(), std::io::Error> {
    let empty_hash = hex::encode(Sha256::digest([]));
    let chunk_hash = hex::encode(Sha256::digest(chunk));
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        verification.timestamp,
        verification.credential_scope,
        previous_signature,
        empty_hash,
        chunk_hash
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(&verification.signing_key)
        .expect("HMAC accepts every key length");
    mac.update(string_to_sign.as_bytes());
    let expected = hex::encode(mac.finalize().into_bytes());
    if expected
        .as_bytes()
        .ct_eq(supplied_signature.as_bytes())
        .unwrap_u8()
        != 1
    {
        return Err(io_error("aws-chunked signature does not match"));
    }
    Ok(())
}

fn io_error(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

pub(super) fn prepare_body(
    request: &mut axum::extract::Request,
) -> Result<Option<[u8; 32]>, S3Error> {
    let value = request
        .headers()
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| S3Error::invalid_request("PutObject requires x-amz-content-sha256"))?;
    if value == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
        let verification = request
            .extensions_mut()
            .remove::<AwsChunkedVerification>()
            .ok_or_else(|| S3Error::signature_mismatch())?;
        if request
            .headers()
            .get(http::header::CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            != Some("aws-chunked")
        {
            return Err(S3Error::invalid_request(
                "Streaming SigV4 payload requires content-encoding: aws-chunked",
            ));
        }
        let expected_length = request
            .headers()
            .get("x-amz-decoded-content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok());
        let body = std::mem::replace(request.body_mut(), Body::empty());
        *request.body_mut() = decode(body, verification, expected_length);
        request.headers_mut().remove(http::header::CONTENT_ENCODING);
        return Ok(None);
    }
    if value == "UNSIGNED-PAYLOAD" {
        return Err(S3Error::invalid_request(
            "PutObject requires a signed payload hash",
        ));
    }
    if value.starts_with("STREAMING-") {
        return Err(S3Error::not_implemented(
            "This aws-chunked payload variant is not implemented",
        ));
    }
    let bytes = hex::decode(value)
        .map_err(|_| S3Error::invalid_request("x-amz-content-sha256 is invalid"))?;
    bytes
        .try_into()
        .map(Some)
        .map_err(|_| S3Error::invalid_request("x-amz-content-sha256 must contain 32 bytes"))
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;

    use super::*;

    #[test]
    fn chunk_header_requires_a_canonical_signature() {
        let signature = "a".repeat(64);
        assert_eq!(
            parse_header(&format!("10;chunk-signature={signature}"))
                .unwrap()
                .0,
            16
        );
        assert!(parse_header("10").is_err());
    }

    #[tokio::test]
    async fn standard_terminal_chunk_does_not_require_an_extra_crlf() {
        let verification = AwsChunkedVerification {
            signing_key: vec![7; 32],
            timestamp: "20260804T123456Z".into(),
            credential_scope: "20260804/eu-west-2/s3/aws4_request".into(),
            previous_signature: "a".repeat(64),
        };
        let payload = Bytes::from_static(b"hello");
        let payload_signature =
            test_signature(&verification, &verification.previous_signature, &payload);
        let terminal_signature = test_signature(&verification, &payload_signature, &Bytes::new());
        let encoded = format!(
            "5;chunk-signature={payload_signature}\r\nhello\r\n0;chunk-signature={terminal_signature}\r\n\r\n"
        );

        let decoded = decode(Body::from(encoded), verification, Some(5))
            .collect()
            .await
            .unwrap()
            .to_bytes();

        assert_eq!(decoded, payload);
    }

    fn test_signature(
        verification: &AwsChunkedVerification,
        previous_signature: &str,
        chunk: &Bytes,
    ) -> String {
        let empty_hash = hex::encode(Sha256::digest([]));
        let chunk_hash = hex::encode(Sha256::digest(chunk));
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
            verification.timestamp,
            verification.credential_scope,
            previous_signature,
            empty_hash,
            chunk_hash
        );
        let mut mac = Hmac::<Sha256>::new_from_slice(&verification.signing_key).unwrap();
        mac.update(string_to_sign.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }
}
