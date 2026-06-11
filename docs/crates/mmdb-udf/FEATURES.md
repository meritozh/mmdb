# mmdb-udf Features

`mmdb-udf` provides WASM UDF registration metadata and a small Wasmtime runtime.
It is separate from `mmdb-query` so the query crate does not depend on Wasmtime.

## Responsibilities

- Validate WASM bytes.
- Hash registered UDF definitions with BLAKE3.
- Store typed signatures and sandbox limits.
- Reject conflicting duplicate registrations.
- Execute simple WASM exports under fuel and memory limits.

## Registry

`UdfRegistry` registers `UdfDefinition` values:

- name
- BLAKE3 hash
- `UdfSignature`
- `WasmLimits`

Registering the same name and identical definition is idempotent. Registering
the same name with a different signature, hash, or limits is rejected.

## Types

`UdfType` supports:

- `F32`
- `F64`
- `I64`
- `Bool`

`UdfSignature` records argument and return types.

`WasmLimits::default()` sets a 16 MiB memory cap and 1,000,000 fuel units.

## Runtime

`WasmRuntime` builds a Wasmtime engine with fuel consumption enabled.

`call_i64`:

1. validates the WASM magic/version;
2. compiles the module;
3. creates a store with memory and fuel limits;
4. invokes the named export with `i64` arguments;
5. returns an `i64` result or an `InvalidArgument` error.

This is intentionally narrow. Facade query execution currently uses
registered Rust closures for score UDFs; direct WASM execution remains available
from this crate without pulling Wasmtime into `mmdb-query`.

## Public Surface

- `UdfType`
- `UdfSignature`
- `WasmLimits`
- `UdfDefinition`
- `UdfRegistry`
- `WasmRuntime`

## Source Files

- `crates/mmdb-udf/src/lib.rs`: registry, validation, limits, and runtime.

