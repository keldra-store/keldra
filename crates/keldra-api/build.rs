fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path =
        std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("keldra_descriptor.bin");
    println!("cargo:rerun-if-changed=proto/keldra.proto");
    println!("cargo:rerun-if-changed=proto/personaldb.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(
            &["proto/keldra.proto", "proto/personaldb.proto"],
            &["proto"],
        )?;
    Ok(())
}
