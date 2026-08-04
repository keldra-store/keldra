use std::path::Path;
use std::process::Stdio;

use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use bytes::BytesMut;
use http_body_util::BodyExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

use super::{GitError, Target};
use crate::v05::GatewayIdentity;

pub(super) async fn bounded_body(body: &mut Body, maximum: u64) -> Result<Vec<u8>, GitError> {
    let mut body = std::mem::replace(body, Body::empty());
    let mut bytes = BytesMut::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|error| GitError::bad_request(error.to_string()))?;
        let data = frame
            .into_data()
            .map_err(|_| GitError::bad_request("Git request trailers are not supported"))?;
        let next = bytes
            .len()
            .checked_add(data.len())
            .ok_or_else(|| GitError::bad_request("Git request length overflow"))?;
        if next as u64 > maximum {
            return Err(GitError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Git request exceeds the configured object maximum",
            ));
        }
        bytes.extend_from_slice(&data);
    }
    Ok(bytes.to_vec())
}

pub(super) async fn execute(
    target: &Target,
    identity: &GatewayIdentity,
    repository: &Path,
    method: &str,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> Result<Response, GitError> {
    let root = repository
        .parent()
        .ok_or_else(|| GitError::internal("Git cache repository has no parent"))?;
    let mut command = Command::new("git");
    command
        .arg("http-backend")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("GIT_PROJECT_ROOT", root)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("PATH_INFO", &target.path_info)
        .env("QUERY_STRING", &target.query)
        .env("REQUEST_METHOD", method)
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("CONTENT_TYPE", content_type.unwrap_or_default())
        .env(
            "REMOTE_USER",
            identity
                .caller()
                .and_then(|caller| caller.authenticated_app_id().ok())
                .unwrap_or_default(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| GitError::internal(format!("start git-http-backend: {error}")))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(&body).await.map_err(|error| {
            GitError::internal(format!("write git-http-backend input: {error}"))
        })?;
    }
    let output = child
        .wait_with_output()
        .await
        .map_err(|error| GitError::internal(format!("wait for git-http-backend: {error}")))?;
    if !output.status.success() {
        return Err(GitError::internal(format!(
            "git-http-backend failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    parse_cgi(output.stdout)
}

fn parse_cgi(output: Vec<u8>) -> Result<Response, GitError> {
    let (header_end, delimiter_length) = output
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, 4))
        .or_else(|| {
            output
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|position| (position, 2))
        })
        .ok_or_else(|| GitError::internal("git-http-backend returned no CGI header block"))?;
    if header_end > 64 * 1024 {
        return Err(GitError::internal(
            "git-http-backend CGI headers exceed 64 KiB",
        ));
    }
    let headers = std::str::from_utf8(&output[..header_end])
        .map_err(|_| GitError::internal("git-http-backend returned non-UTF-8 headers"))?;
    let mut status = StatusCode::OK;
    let mut builder = Response::builder();
    for line in headers.lines() {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| GitError::internal("git-http-backend returned a malformed header"))?;
        if name.eq_ignore_ascii_case("Status") {
            let code = value
                .trim()
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u16>().ok())
                .and_then(|value| StatusCode::from_u16(value).ok())
                .ok_or_else(|| GitError::internal("git-http-backend returned an invalid status"))?;
            status = code;
            continue;
        }
        let name = HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| GitError::internal("git-http-backend returned an invalid header name"))?;
        let value = HeaderValue::from_str(value.trim())
            .map_err(|_| GitError::internal("git-http-backend returned an invalid header value"))?;
        builder = builder.header(name, value);
    }
    builder
        .status(status)
        .body(Body::from(
            output[(header_end + delimiter_length)..].to_vec(),
        ))
        .map_err(|error| GitError::internal(format!("build Git response: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_git_cgi_response_without_treating_body_as_text() {
        let response =
            parse_cgi(b"Status: 200 OK\r\nContent-Type: x-git/test\r\n\r\n\0body".to_vec())
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "x-git/test");
    }
}
