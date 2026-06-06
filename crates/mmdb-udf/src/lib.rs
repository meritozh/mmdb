//! WASM UDF registry and sandbox metadata.

use mmdb_core::{Error, Result};
use std::collections::BTreeMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use wasmtime::{Config, Engine, Instance, Module, Store, StoreLimitsBuilder, Val};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdfType {
    F32,
    F64,
    I64,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfSignature {
    pub args: Vec<UdfType>,
    pub returns: UdfType,
}

impl UdfSignature {
    pub fn new(args: Vec<UdfType>, returns: UdfType) -> Self {
        Self { args, returns }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmLimits {
    pub max_memory_bytes: u64,
    pub max_fuel: u64,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 16 * 1024 * 1024,
            max_fuel: 1_000_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdfDefinition {
    pub name: String,
    pub hash: [u8; 32],
    pub signature: UdfSignature,
    pub limits: WasmLimits,
}

#[derive(Default)]
pub struct UdfRegistry {
    inner: RwLock<BTreeMap<String, UdfDefinition>>,
}

impl UdfRegistry {
    pub fn register(
        &self,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        signature: UdfSignature,
    ) -> Result<UdfDefinition> {
        self.register_with_limits(name, wasm_bytes, signature, WasmLimits::default())
    }

    pub fn register_with_limits(
        &self,
        name: impl Into<String>,
        wasm_bytes: &[u8],
        signature: UdfSignature,
        limits: WasmLimits,
    ) -> Result<UdfDefinition> {
        validate_wasm(wasm_bytes)?;
        let name = name.into();
        let hash = *blake3::hash(wasm_bytes).as_bytes();
        let definition = UdfDefinition {
            name: name.clone(),
            hash,
            signature,
            limits,
        };

        let mut state = self.write()?;
        if let Some(existing) = state.get(&name) {
            if existing == &definition {
                return Ok(existing.clone());
            }
            return Err(Error::InvalidArgument(format!(
                "UDF `{name}` already registered with a different definition"
            )));
        }
        state.insert(name, definition.clone());
        Ok(definition)
    }

    pub fn get(&self, name: &str) -> Result<Option<UdfDefinition>> {
        Ok(self.read()?.get(name).cloned())
    }

    pub fn names(&self) -> Vec<String> {
        self.read()
            .map(|state| state.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn read(&self) -> Result<RwLockReadGuard<'_, BTreeMap<String, UdfDefinition>>> {
        self.inner
            .read()
            .map_err(|_| Error::Storage("UDF registry read lock poisoned".into()))
    }

    fn write(&self) -> Result<RwLockWriteGuard<'_, BTreeMap<String, UdfDefinition>>> {
        self.inner
            .write()
            .map_err(|_| Error::Storage("UDF registry write lock poisoned".into()))
    }
}

fn validate_wasm(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 8 || &bytes[..4] != b"\0asm" {
        return Err(Error::InvalidArgument(
            "UDF bytes must start with the wasm magic header".into(),
        ));
    }
    if bytes[4..8] != [1, 0, 0, 0] {
        return Err(Error::InvalidArgument(
            "only wasm binary format version 1 is supported".into(),
        ));
    }
    Ok(())
}

pub struct WasmRuntime {
    engine: Engine,
    limits: WasmLimits,
}

impl WasmRuntime {
    pub fn new(limits: WasmLimits) -> Result<Self> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(wasm_error)?;
        Ok(Self { engine, limits })
    }

    pub fn call_i64(&self, wasm_bytes: &[u8], export: &str, args: &[i64]) -> Result<i64> {
        validate_wasm(wasm_bytes)?;
        let module = Module::new(&self.engine, wasm_bytes).map_err(wasm_error)?;
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(self.limits.max_memory_bytes as usize)
            .build();
        let mut store = Store::new(&self.engine, store_limits);
        store.limiter(|limits| limits);
        store.set_fuel(self.limits.max_fuel).map_err(wasm_error)?;

        let instance = Instance::new(&mut store, &module, &[]).map_err(wasm_error)?;
        let func = instance.get_func(&mut store, export).ok_or_else(|| {
            Error::InvalidArgument(format!("WASM export `{export}` was not found"))
        })?;
        let params: Vec<_> = args.iter().copied().map(Val::I64).collect();
        let mut results = [Val::I64(0)];
        func.call(&mut store, &params, &mut results)
            .map_err(|err| {
                Error::InvalidArgument(format!("wasm execution failed or fuel exhausted: {err}"))
            })?;

        match results[0] {
            Val::I64(value) => Ok(value),
            ref other => Err(Error::InvalidArgument(format!(
                "WASM export `{export}` returned {other:?}, expected i64"
            ))),
        }
    }
}

fn wasm_error(err: impl std::fmt::Display) -> Error {
    Error::InvalidArgument(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_udf_hashes_bytes_and_rejects_invalid_wasm() {
        let registry = UdfRegistry::default();
        let wasm = b"\0asm\x01\0\0\0";
        let signature = UdfSignature::new(vec![UdfType::F32, UdfType::I64], UdfType::F32);

        let udf = registry
            .register("score_decay", wasm, signature.clone())
            .unwrap();
        assert_eq!(udf.name, "score_decay");
        assert_eq!(udf.signature, signature);
        assert_eq!(registry.get("score_decay").unwrap().unwrap().hash, udf.hash);

        let err = registry
            .register("bad", b"not wasm", signature)
            .unwrap_err();
        assert!(format!("{err}").contains("wasm magic"));
    }

    #[test]
    fn duplicate_name_must_match_existing_definition() {
        let registry = UdfRegistry::default();
        let wasm = b"\0asm\x01\0\0\0";
        let sig = UdfSignature::new(vec![UdfType::F32], UdfType::F32);
        registry.register("score", wasm, sig.clone()).unwrap();
        registry.register("score", wasm, sig).unwrap();

        let err = registry
            .register(
                "score",
                wasm,
                UdfSignature::new(vec![UdfType::I64], UdfType::I64),
            )
            .unwrap_err();
        assert!(format!("{err}").contains("already registered"));
    }

    #[test]
    fn sandbox_limits_have_safe_defaults() {
        let limits = WasmLimits::default();
        assert_eq!(limits.max_memory_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_fuel, 1_000_000);
    }

    #[test]
    fn runtime_executes_i64_udf_with_fuel() {
        let wasm = wat::parse_str(
            r#"
            (module
              (func (export "run") (param i64) (result i64)
                local.get 0
                i64.const 2
                i64.mul))
            "#,
        )
        .unwrap();
        let runtime = WasmRuntime::new(WasmLimits::default()).unwrap();

        let value = runtime.call_i64(&wasm, "run", &[21]).unwrap();

        assert_eq!(value, 42);
    }

    #[test]
    fn runtime_stops_when_fuel_is_exhausted() {
        let wasm = wat::parse_str(
            r#"
            (module
              (func (export "run") (param i64) (result i64)
                (loop br 0)
                local.get 0))
            "#,
        )
        .unwrap();
        let runtime = WasmRuntime::new(WasmLimits {
            max_memory_bytes: 64 * 1024,
            max_fuel: 10,
        })
        .unwrap();

        let err = runtime.call_i64(&wasm, "run", &[1]).unwrap_err();

        assert!(format!("{err}").contains("fuel"));
    }
}
