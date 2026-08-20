fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/data_peer.proto");
    println!("cargo:rerun-if-changed=proto/cluster_peer.proto");
    println!("cargo:rerun-if-changed=proto/join_peer.proto");
    println!("cargo:rerun-if-changed=proto/payload_peer.proto");
    println!("cargo:rerun-if-changed=../keldra-api/proto/personaldb.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .extern_path(".keldra.v1", "::keldra_api::v1")
        .compile_protos(
            &[
                "proto/data_peer.proto",
                "proto/cluster_peer.proto",
                "proto/join_peer.proto",
                "proto/payload_peer.proto",
            ],
            &["proto", "../keldra-api/proto"],
        )?;
    Ok(())
}
