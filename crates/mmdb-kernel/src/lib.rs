//! Storage primitives shared by MMDB's managed stores.

pub mod store_format;

mod kernel;

pub use kernel::{
    Batch, Bytes, EraId, EraPartitions, EraState, Kernel, KernelError, KernelResult, ModuleId,
    ModuleSpec, OperationId, Partition, WriteGuard,
};
