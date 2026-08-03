fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/data_peer.proto");
    println!("cargo:rerun-if-changed=proto/cluster_peer.proto");
    println!("cargo:rerun-if-changed=proto/join_peer.proto");
    println!("cargo:rerun-if-changed=proto/payload_peer.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".anvil.v1", "::anvil_api::v1")
        .compile_protos(
            &[
                "proto/data_peer.proto",
                "proto/cluster_peer.proto",
                "proto/join_peer.proto",
                "proto/payload_peer.proto",
            ],
            &["proto", "../anvil-api/proto"],
        )?;
    Ok(())
}
