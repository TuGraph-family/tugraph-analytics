fn main() {
    prost_build::compile_protos(
        &[
            "meta.proto",
            "pushdown.proto",
            "manifest.proto",
            "memory_index_manifest.proto",
        ],
        &["proto/"],
    )
    .unwrap();
}
