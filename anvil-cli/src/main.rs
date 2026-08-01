use std::path::PathBuf;

use anvil_storage::v1::object_chunk::Value as ObjectChunkValue;
use anvil_storage::v1::object_head::State as ObjectHeadState;
use anvil_storage::v1::write_condition::Condition;
use anvil_storage::v1::{
    BlobRef, BucketPolicy, DeleteObjectRequest, GetObjectRequest, HeadObjectRequest,
    InvokeProgramRequest, ObjectAddress, PublishObjectRequest, SetBucketPolicyRequest,
    UploadBlobChunk, WriteCondition,
};
use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_stream::wrappers::ReceiverStream;

const CHUNK_BYTES: usize = 256 * 1024;

#[derive(Debug, Parser)]
#[command(name = "anvil", version, about = "Anvil 0.5 client")]
struct Arguments {
    #[arg(long, env = "ANVIL_ENDPOINT", default_value = "http://127.0.0.1:50051")]
    endpoint: String,

    #[arg(long, env = "ANVIL_API_TOKEN", hide_env_values = true)]
    token: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Put {
        tenant: String,
        bucket: String,
        path: String,
        file: PathBuf,
        #[arg(long)]
        content_type: Option<String>,
        #[arg(long)]
        command_id: String,
        #[arg(long)]
        durability_class: String,
        #[arg(long)]
        if_absent: bool,
        #[arg(long)]
        if_version: Option<u64>,
    },
    Get {
        tenant: String,
        bucket: String,
        path: String,
        #[arg(long)]
        version: Option<u64>,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    Head {
        tenant: String,
        bucket: String,
        path: String,
    },
    Delete {
        tenant: String,
        bucket: String,
        path: String,
        #[arg(long)]
        command_id: String,
        #[arg(long)]
        durability_class: String,
        #[arg(long)]
        if_version: Option<u64>,
    },
    SetPolicy {
        tenant: String,
        bucket: String,
        #[arg(long = "immutable")]
        immutable: Vec<String>,
        #[arg(long = "program-only")]
        program_only: Vec<String>,
    },
    InvokeProgram {
        tenant: String,
        bucket: String,
        program_path: String,
        invocation_id: String,
        #[arg(long)]
        program_hash: String,
        #[arg(long)]
        durability_class: String,
        input: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let arguments = Arguments::parse();
    let mut client = anvil_storage::connect(&arguments.endpoint, &arguments.token)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    match arguments.command {
        Command::Put {
            tenant,
            bucket,
            path,
            file,
            content_type,
            command_id,
            durability_class,
            if_absent,
            if_version,
        } => {
            if if_absent && if_version.is_some() {
                bail!("--if-absent and --if-version are mutually exclusive");
            }
            let blob = upload(&mut client, file).await?;
            let receipt = client
                .publish_object(PublishObjectRequest {
                    address: Some(address(tenant, bucket, path)),
                    blob: Some(blob),
                    content_type: content_type.unwrap_or_default(),
                    condition: Some(condition(if_absent, if_version)),
                    command_id,
                    durability_class,
                })
                .await?
                .into_inner();
            println!("{}", receipt.version);
        }
        Command::Get {
            tenant,
            bucket,
            path,
            version,
            output,
        } => {
            let mut stream = client
                .get_object(GetObjectRequest {
                    address: Some(address(tenant, bucket, path)),
                    version,
                })
                .await?
                .into_inner();

            let first = stream
                .message()
                .await?
                .ok_or_else(|| anyhow::anyhow!("server returned an empty object stream"))?;
            let present = match first.value {
                Some(ObjectChunkValue::Head(head)) => match head.state {
                    Some(ObjectHeadState::Present(present)) => present,
                    Some(ObjectHeadState::Deleted(deleted)) => {
                        bail!("object is deleted at version {}", deleted.version)
                    }
                    Some(ObjectHeadState::NeverExisted(_)) => bail!("object never existed"),
                    None => bail!("server returned an empty object state"),
                },
                Some(ObjectChunkValue::Bytes(_)) => {
                    bail!("server sent object bytes before the object state")
                }
                None => bail!("server returned an empty object chunk"),
            };
            let expected_bytes = present
                .blob
                .ok_or_else(|| anyhow::anyhow!("present object has no payload reference"))?
                .length;
            let mut file = match output {
                Some(path) => Some(tokio::fs::File::create(path).await?),
                None => None,
            };
            let mut stdout = tokio::io::stdout();
            let mut observed_bytes = 0_u64;
            while let Some(chunk) = stream.message().await? {
                match chunk.value {
                    Some(ObjectChunkValue::Bytes(bytes)) => {
                        observed_bytes = observed_bytes
                            .checked_add(bytes.len() as u64)
                            .ok_or_else(|| anyhow::anyhow!("object length overflow"))?;
                        if observed_bytes > expected_bytes {
                            bail!("server streamed more bytes than the object head declared");
                        }
                        match file.as_mut() {
                            Some(file) => file.write_all(&bytes).await?,
                            None => stdout.write_all(&bytes).await?,
                        }
                    }
                    Some(ObjectChunkValue::Head(_)) => {
                        bail!("server sent more than one object state")
                    }
                    None => bail!("server returned an empty object chunk"),
                }
            }
            if observed_bytes != expected_bytes {
                bail!(
                    "server streamed {observed_bytes} bytes but the object head declared {expected_bytes}"
                );
            }
        }
        Command::Head {
            tenant,
            bucket,
            path,
        } => {
            let head = client
                .head_object(HeadObjectRequest {
                    address: Some(address(tenant, bucket, path)),
                })
                .await?
                .into_inner();
            match head.state {
                Some(ObjectHeadState::Present(present)) => println!(
                    "present version={} bytes={}",
                    present.version,
                    present.blob.map_or(0, |blob| blob.length)
                ),
                Some(ObjectHeadState::Deleted(deleted)) => {
                    println!("deleted version={}", deleted.version)
                }
                Some(ObjectHeadState::NeverExisted(_)) => println!("never-existed"),
                None => bail!("server returned an empty object state"),
            }
        }
        Command::Delete {
            tenant,
            bucket,
            path,
            command_id,
            durability_class,
            if_version,
        } => {
            let receipt = client
                .delete_object(DeleteObjectRequest {
                    address: Some(address(tenant, bucket, path)),
                    condition: Some(condition(false, if_version)),
                    command_id,
                    durability_class,
                })
                .await?
                .into_inner();
            println!("{}", receipt.version);
        }
        Command::SetPolicy {
            tenant,
            bucket,
            mut immutable,
            mut program_only,
        } => {
            immutable.sort();
            immutable.dedup();
            program_only.sort();
            program_only.dedup();
            client
                .set_bucket_policy(SetBucketPolicyRequest {
                    tenant,
                    bucket,
                    policy: Some(BucketPolicy {
                        immutable_path_prefixes: immutable,
                        program_only_path_prefixes: program_only,
                    }),
                })
                .await?;
        }
        Command::InvokeProgram {
            tenant,
            bucket,
            program_path,
            invocation_id,
            program_hash,
            durability_class,
            input,
        } => {
            let input_json = tokio::fs::read(input).await?;
            let response = client
                .invoke_program(InvokeProgramRequest {
                    program: Some(address(tenant, bucket, program_path)),
                    invocation_id,
                    program_hash: parse_hex(&program_hash)?,
                    input_json,
                    durability_class,
                })
                .await?
                .into_inner();
            tokio::io::stdout().write_all(&response.output_json).await?;
            tokio::io::stdout().write_all(b"\n").await?;
        }
    }
    Ok(())
}

async fn upload(client: &mut anvil_storage::RawClient, path: PathBuf) -> Result<BlobRef> {
    let mut file = tokio::fs::File::open(&path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let producer = tokio::spawn(async move {
        loop {
            let mut bytes = vec![0_u8; CHUNK_BYTES];
            let read = file.read(&mut bytes).await?;
            if read == 0 {
                break;
            }
            bytes.truncate(read);
            if sender.send(UploadBlobChunk { bytes }).await.is_err() {
                break;
            }
        }
        Ok::<(), std::io::Error>(())
    });
    let response = client
        .upload_blob(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    producer.await.context("upload task panicked")??;
    Ok(response)
}

fn address(tenant: String, bucket: String, path: String) -> ObjectAddress {
    ObjectAddress {
        tenant,
        bucket,
        path,
    }
}

fn condition(if_absent: bool, if_version: Option<u64>) -> WriteCondition {
    WriteCondition {
        condition: Some(match (if_absent, if_version) {
            (true, _) => Condition::Absent(true),
            (false, Some(version)) => Condition::Version(version),
            (false, None) => Condition::Any(true),
        }),
    }
}

fn parse_hex(value: &str) -> Result<Vec<u8>> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("program hash must be the 64-digit BLAKE3 hash of the stored definition bytes");
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).expect("hexadecimal input is ASCII");
            u8::from_str_radix(pair, 16).context("invalid program hash")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_hex;

    #[test]
    fn program_hash_is_exactly_one_blake3_digest() {
        assert_eq!(parse_hex(&"ab".repeat(32)).unwrap(), vec![0xab; 32]);
        for invalid in [
            String::new(),
            "ab".repeat(31),
            "ab".repeat(33),
            "gg".repeat(32),
        ] {
            assert!(parse_hex(&invalid).is_err());
        }
    }
}
