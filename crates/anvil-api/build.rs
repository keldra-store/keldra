fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/anvil.proto");
    println!("cargo:rerun-if-changed=proto/personaldb.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/anvil.proto", "proto/personaldb.proto"], &["proto"])?;
    Ok(())
}
