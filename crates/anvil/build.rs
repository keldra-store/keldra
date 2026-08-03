fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/data_peer.proto");
    println!("cargo:rerun-if-changed=proto/join_peer.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/data_peer.proto", "proto/join_peer.proto"],
            &["proto"],
        )?;
    Ok(())
}
