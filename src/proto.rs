//! Generated REAPI / ByteStream protobuf bindings.

// Generated code carries the full REAPI surface; the cache subset only uses
// part of it.
#![allow(dead_code, clippy::doc_lazy_continuation, clippy::doc_overindented_list_items)]

pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
    pub mod longrunning {
        include!(concat!(env!("OUT_DIR"), "/google.longrunning.rs"));
    }
    pub mod bytestream {
        include!(concat!(env!("OUT_DIR"), "/google.bytestream.rs"));
    }
    pub mod api {
        include!(concat!(env!("OUT_DIR"), "/google.api.rs"));
    }
}

pub mod build {
    pub mod bazel {
        pub mod semver {
            include!(concat!(env!("OUT_DIR"), "/build.bazel.semver.rs"));
        }
        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    include!(concat!(env!("OUT_DIR"), "/build.bazel.remote.execution.v2.rs"));
                }
            }
        }
    }
}

pub use build::bazel::remote::execution::v2 as reapi;
pub use build::bazel::semver::SemVer;
pub use google::bytestream;
pub use google::rpc::Status as RpcStatus;
