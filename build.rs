fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    tonic_prost_build::configure()
        .build_client(false)
        .build_server(true)
        .compile_protos(
            &[
                "proto/build/bazel/remote/execution/v2/remote_execution.proto",
                "proto/google/bytestream/bytestream.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
