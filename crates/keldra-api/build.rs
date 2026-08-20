fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/keldra.proto");
    println!("cargo:rerun-if-changed=proto/personaldb.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["proto/keldra.proto", "proto/personaldb.proto"],
            &["proto"],
        )?;
    Ok(())
}
