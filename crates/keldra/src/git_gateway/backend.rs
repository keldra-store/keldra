use std::io;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use axum::body::Body;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt as _;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWriteExt as _};
use tokio::process::{ChildStdin, Command};
use tokio::sync::Semaphore;

use super::{GIT_PROCESS_TIMEOUT, GitError, Target};
use crate::object_service::GatewayIdentity;

const MAX_CGI_HEADER_BYTES: usize = 64 * 1024;
const MAX_CGI_ERROR_BYTES: u64 = 64 * 1024;

pub(super) struct ExecutedGit {
    pub(super) response: Response,
    pub(super) inbound_bytes: Arc<AtomicU64>,
    pub(super) request_complete: Arc<AtomicBool>,
}

pub(super) async fn execute(
    target: &Target,
    identity: &GatewayIdentity,
    repository: &Path,
    method: &str,
    content_type: Option<&str>,
    content_length: Option<u64>,
    body: Body,
    process_admission: Arc<Semaphore>,
) -> Result<ExecutedGit, GitError> {
    let process_permit = process_admission
        .acquire_owned()
        .await
        .map_err(|_| GitError::internal("Git process admission is closed"))?;
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
        .env(
            "CONTENT_LENGTH",
            content_length
                .map(|value| value.to_string())
                .unwrap_or_default(),
        )
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
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| GitError::internal("git-http-backend stdin is unavailable"))?;
    let inbound_bytes = Arc::new(AtomicU64::new(0));
    let request_complete = Arc::new(AtomicBool::new(false));
    let pump = tokio::spawn(pump_request(
        body,
        stdin,
        inbound_bytes.clone(),
        request_complete.clone(),
    ));

    if !target.operation.streams_response() {
        let output = tokio::time::timeout(GIT_PROCESS_TIMEOUT, child.wait_with_output())
            .await
            .map_err(|_| GitError::internal("git-http-backend exceeded its operation deadline"))?
            .map_err(|error| GitError::internal(format!("wait for git-http-backend: {error}")))?;
        pump.await
            .map_err(|error| GitError::internal(format!("join Git request stream: {error}")))??;
        if !output.status.success() {
            return Err(GitError::internal(format!(
                "git-http-backend failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        return Ok(ExecutedGit {
            response: parse_cgi(output.stdout)?,
            inbound_bytes,
            request_complete,
        });
    }

    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| GitError::internal("git-http-backend stdout is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| GitError::internal("git-http-backend stderr is unavailable"))?;
    let (status, headers, prefix) =
        tokio::time::timeout(GIT_PROCESS_TIMEOUT, read_cgi_head(&mut stdout))
            .await
            .map_err(|_| GitError::internal("git-http-backend exceeded its header deadline"))??;
    let error_output = tokio::spawn(async move {
        let mut bytes = Vec::new();
        stderr
            .take(MAX_CGI_ERROR_BYTES)
            .read_to_end(&mut bytes)
            .await?;
        Ok::<_, io::Error>(bytes)
    });
    let stream = async_stream::stream! {
        let _process_permit = process_permit;
        let deadline = tokio::time::Instant::now() + GIT_PROCESS_TIMEOUT;
        if !prefix.is_empty() {
            yield Ok::<Bytes, io::Error>(prefix);
        }
        let mut buffer = vec![0_u8; 1024 * 1024];
        loop {
            match tokio::time::timeout_at(deadline, stdout.read(&mut buffer)).await {
                Err(_) => {
                    pump.abort();
                    error_output.abort();
                    yield Err(io::Error::new(io::ErrorKind::TimedOut, "git-http-backend exceeded its operation deadline"));
                    return;
                }
                Ok(Ok(0)) => break,
                Ok(Ok(read)) => yield Ok(Bytes::copy_from_slice(&buffer[..read])),
                Ok(Err(error)) => {
                    pump.abort();
                    error_output.abort();
                    yield Err(error);
                    return;
                }
            }
        }
        let request_result = match tokio::time::timeout_at(deadline, pump).await {
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "Git request stream exceeded its operation deadline")),
            Ok(Ok(result)) => result.map_err(|error| io::Error::other(error.message)),
            Ok(Err(error)) => Err(io::Error::other(format!("join Git request stream: {error}"))),
        };
        if let Err(error) = request_result {
            yield Err(error);
            return;
        }
        let process_status = match tokio::time::timeout_at(deadline, child.wait()).await {
            Err(_) => {
                error_output.abort();
                yield Err(io::Error::new(io::ErrorKind::TimedOut, "git-http-backend exceeded its operation deadline"));
                return;
            }
            Ok(Ok(status)) => status,
            Ok(Err(error)) => {
                error_output.abort();
                yield Err(error);
                return;
            }
        };
        let stderr = match tokio::time::timeout_at(deadline, error_output).await {
            Err(_) => {
                yield Err(io::Error::new(io::ErrorKind::TimedOut, "Git error stream exceeded its operation deadline"));
                return;
            }
            Ok(Ok(Ok(stderr))) => stderr,
            Ok(Ok(Err(error))) => {
                yield Err(error);
                return;
            }
            Ok(Err(error)) => {
                yield Err(io::Error::other(format!("join Git error stream: {error}")));
                return;
            }
        };
        if !process_status.success() {
            yield Err(io::Error::other(format!(
                "git-http-backend failed: {}",
                String::from_utf8_lossy(&stderr).trim()
            )));
        }
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    let response = builder
        .body(Body::from_stream(stream))
        .map_err(|error| GitError::internal(format!("build Git response: {error}")))?;
    Ok(ExecutedGit {
        response,
        inbound_bytes,
        request_complete,
    })
}

async fn pump_request(
    mut body: Body,
    mut stdin: ChildStdin,
    bytes: Arc<AtomicU64>,
    complete: Arc<AtomicBool>,
) -> Result<(), GitError> {
    while let Some(frame) = body.frame().await {
        let data = frame
            .map_err(|error| GitError::bad_request(format!("Git request stream failed: {error}")))?
            .into_data()
            .map_err(|_| GitError::bad_request("Git request trailers are not supported"))?;
        stdin.write_all(&data).await.map_err(|error| {
            GitError::internal(format!("write git-http-backend input: {error}"))
        })?;
        bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
    }
    stdin
        .shutdown()
        .await
        .map_err(|error| GitError::internal(format!("finish git-http-backend input: {error}")))?;
    complete.store(true, Ordering::Release);
    Ok(())
}

async fn read_cgi_head<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<(StatusCode, Vec<(HeaderName, HeaderValue)>, Bytes), GitError> {
    let mut output = BytesMut::new();
    let (header_end, delimiter_length) = loop {
        if let Some(found) = header_boundary(&output) {
            break found;
        }
        if output.len() >= MAX_CGI_HEADER_BYTES {
            return Err(GitError::internal(
                "git-http-backend CGI headers exceed 64 KiB",
            ));
        }
        let read = reader
            .read_buf(&mut output)
            .await
            .map_err(|error| GitError::internal(format!("read Git CGI headers: {error}")))?;
        if read == 0 {
            return Err(GitError::internal(
                "git-http-backend returned no CGI header block",
            ));
        }
    };
    let (status, headers) = parse_headers(&output[..header_end])?;
    let prefix = output.split_off(header_end + delimiter_length).freeze();
    Ok((status, headers, prefix))
}

fn parse_cgi(output: Vec<u8>) -> Result<Response, GitError> {
    let (header_end, delimiter_length) = header_boundary(&output)
        .ok_or_else(|| GitError::internal("git-http-backend returned no CGI header block"))?;
    if header_end > MAX_CGI_HEADER_BYTES {
        return Err(GitError::internal(
            "git-http-backend CGI headers exceed 64 KiB",
        ));
    }
    let (status, headers) = parse_headers(&output[..header_end])?;
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder
        .body(Body::from(
            output[(header_end + delimiter_length)..].to_vec(),
        ))
        .map_err(|error| GitError::internal(format!("build Git response: {error}")))
}

fn header_boundary(output: &[u8]) -> Option<(usize, usize)> {
    output
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, 4))
        .or_else(|| {
            output
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|position| (position, 2))
        })
}

fn parse_headers(bytes: &[u8]) -> Result<(StatusCode, Vec<(HeaderName, HeaderValue)>), GitError> {
    let headers = std::str::from_utf8(bytes)
        .map_err(|_| GitError::internal("git-http-backend returned non-UTF-8 headers"))?;
    let mut status = StatusCode::OK;
    let mut parsed = Vec::new();
    for line in headers.lines() {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| GitError::internal("git-http-backend returned a malformed header"))?;
        if name.eq_ignore_ascii_case("Status") {
            status = value
                .split_whitespace()
                .next()
                .and_then(|value| value.parse::<u16>().ok())
                .and_then(|value| StatusCode::from_u16(value).ok())
                .ok_or_else(|| GitError::internal("git-http-backend returned an invalid status"))?;
            continue;
        }
        let name = HeaderName::from_bytes(name.trim().as_bytes())
            .map_err(|_| GitError::internal("git-http-backend returned an invalid header name"))?;
        let value = HeaderValue::from_str(value.trim())
            .map_err(|_| GitError::internal("git-http-backend returned an invalid header value"))?;
        parsed.push((name, value));
    }
    Ok((status, parsed))
}

#[cfg(test)]
mod tests {
    use http_body_util::BodyExt as _;

    use super::*;

    #[tokio::test]
    async fn parses_git_cgi_response_without_treating_body_as_text() {
        let response =
            parse_cgi(b"Status: 200 OK\r\nContent-Type: x-git/test\r\n\r\n\0body".to_vec())
                .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], "x-git/test");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"\0body")
        );
    }
}
