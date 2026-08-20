use std::time::Duration;

use tonic::metadata::MetadataMap;

pub(super) fn effective_timeout(metadata: &MetadataMap, server_maximum: Duration) -> Duration {
    client_grpc_timeout(metadata).map_or(server_maximum, |client| client.min(server_maximum))
}

fn client_grpc_timeout(metadata: &MetadataMap) -> Option<Duration> {
    let encoded = metadata.get("grpc-timeout")?.to_str().ok()?;
    if encoded.is_empty() {
        return None;
    }
    let (value, unit) = encoded.split_at(encoded.len() - 1);
    if value.is_empty() || value.len() > 8 {
        return None;
    }
    let value = value.parse::<u64>().ok()?;
    match unit {
        "H" => Some(Duration::from_secs(value.checked_mul(60 * 60)?)),
        "M" => Some(Duration::from_secs(value.checked_mul(60)?)),
        "S" => Some(Duration::from_secs(value)),
        "m" => Some(Duration::from_millis(value)),
        "u" => Some(Duration::from_micros(value)),
        "n" => Some(Duration::from_nanos(value)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_timeout_is_clamped_to_the_server_maximum() {
        let mut metadata = MetadataMap::new();
        metadata.insert("grpc-timeout", "2S".parse().unwrap());
        assert_eq!(
            effective_timeout(&metadata, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
    }
}
