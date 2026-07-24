use super::*;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{HeadObjectRequest, HeadObjectResponse, PutObjectResponse, ReadConsistency};
use tonic::{Code, Request, Status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerObjectObservation {
    pub etag: String,
    pub version_id: String,
    pub record_hash: String,
    pub size: i64,
}

impl DockerObjectObservation {
    pub fn from_put_response(response: &PutObjectResponse, size: usize) -> Self {
        Self {
            etag: response.etag.clone(),
            version_id: response.version_id.clone(),
            record_hash: response.record_hash.clone(),
            size: i64::try_from(size).expect("test object size fits i64"),
        }
    }
}

impl From<HeadObjectResponse> for DockerObjectObservation {
    fn from(response: HeadObjectResponse) -> Self {
        Self {
            etag: response.etag,
            version_id: response.version_id,
            record_hash: response.record_hash,
            size: response.size,
        }
    }
}

impl DockerTestCluster {
    pub async fn head_object_at_peer(
        &self,
        actor: &DockerTestStorageActor,
        peer_ordinal: u8,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<DockerObjectObservation, Status> {
        let peer = self.equal_peer(peer_ordinal);
        let mut client = ObjectServiceClient::connect(peer.grpc_addr)
            .await
            .map_err(|error| Status::unavailable(error.to_string()))?;
        let mut request = Request::new(HeadObjectRequest {
            bucket_name: bucket_name.to_string(),
            object_key: object_key.to_string(),
            version_id: None,
            consistency: Some(ReadConsistency {
                mode: Some(anvil::anvil_api::read_consistency::Mode::Latest(true)),
            }),
        });
        add_actor_bearer(&mut request, actor);
        client
            .head_object(request)
            .await
            .map(|response| response.into_inner().into())
    }

    pub async fn wait_for_metadata_replica_convergence(
        &self,
        actor: &DockerTestStorageActor,
        bucket_name: &str,
        object_key: &str,
        expected: &DockerObjectObservation,
        timeout: Duration,
    ) {
        let peers = self
            .selected_metadata_replicas()
            .into_iter()
            .map(|peer| peer.ordinal)
            .collect::<Vec<_>>();
        self.wait_for_object_convergence(actor, bucket_name, object_key, expected, &peers, timeout)
            .await;
    }

    pub async fn wait_for_all_peer_convergence(
        &self,
        actor: &DockerTestStorageActor,
        bucket_name: &str,
        object_key: &str,
        expected: &DockerObjectObservation,
        timeout: Duration,
    ) {
        let peers = self
            .equal_peers()
            .into_iter()
            .map(|peer| peer.ordinal)
            .collect::<Vec<_>>();
        self.wait_for_object_convergence(actor, bucket_name, object_key, expected, &peers, timeout)
            .await;
    }

    pub async fn wait_for_object_convergence(
        &self,
        actor: &DockerTestStorageActor,
        bucket_name: &str,
        object_key: &str,
        expected: &DockerObjectObservation,
        peer_ordinals: &[u8],
        timeout: Duration,
    ) {
        assert!(
            !peer_ordinals.is_empty(),
            "object convergence requires at least one Docker peer"
        );
        let deadline = Instant::now() + timeout;
        let mut last_observations = Vec::new();
        while Instant::now() < deadline {
            last_observations.clear();
            let mut converged = true;
            for ordinal in peer_ordinals {
                match self
                    .head_object_at_peer(actor, *ordinal, bucket_name, object_key)
                    .await
                {
                    Ok(observation) if &observation == expected => {
                        last_observations.push(format!("peer {ordinal}: {observation:?}"));
                    }
                    Ok(observation) => {
                        converged = false;
                        last_observations.push(format!("peer {ordinal}: {observation:?}"));
                    }
                    Err(status) => {
                        converged = false;
                        last_observations.push(format!(
                            "peer {ordinal}: code={:?} message={}",
                            status.code(),
                            status.message()
                        ));
                    }
                }
            }
            if converged {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!(
            "object {bucket_name}/{object_key} did not converge to {expected:?}: {}",
            last_observations.join("; ")
        );
    }

    pub async fn wait_for_object_absent(
        &self,
        actor: &DockerTestStorageActor,
        bucket_name: &str,
        object_key: &str,
        peer_ordinals: &[u8],
        timeout: Duration,
    ) {
        assert!(
            !peer_ordinals.is_empty(),
            "absence convergence requires at least one Docker peer"
        );
        let deadline = Instant::now() + timeout;
        let mut last_observations = Vec::new();
        while Instant::now() < deadline {
            last_observations.clear();
            let mut absent = true;
            for ordinal in peer_ordinals {
                match self
                    .head_object_at_peer(actor, *ordinal, bucket_name, object_key)
                    .await
                {
                    Err(status) if status.code() == Code::NotFound => {
                        last_observations.push(format!("peer {ordinal}: not found"));
                    }
                    Ok(observation) => {
                        absent = false;
                        last_observations.push(format!("peer {ordinal}: {observation:?}"));
                    }
                    Err(status) => {
                        absent = false;
                        last_observations.push(format!(
                            "peer {ordinal}: code={:?} message={}",
                            status.code(),
                            status.message()
                        ));
                    }
                }
            }
            if absent {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!(
            "object {bucket_name}/{object_key} became visible or peers stayed unavailable: {}",
            last_observations.join("; ")
        );
    }
}

fn add_actor_bearer<T>(request: &mut Request<T>, actor: &DockerTestStorageActor) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token)
            .parse()
            .expect("actor bearer metadata is valid"),
    );
}
