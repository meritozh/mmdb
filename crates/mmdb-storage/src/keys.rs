//! Big-endian key encoding for tenant-isolated, time-ordered scans.
use ulid::Ulid;

pub fn node_key(tenant: u32, id: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&id.0.to_be_bytes());
    k
}

pub fn time_key(tenant: u32, ts_ms: i64, id: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 8 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&(ts_ms as u64).to_be_bytes());
    k.extend_from_slice(&id.0.to_be_bytes());
    k
}

pub fn kind_key(tenant: u32, kind: u8, ts_ms: i64, id: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 1 + 8 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.push(kind);
    k.extend_from_slice(&(ts_ms as u64).to_be_bytes());
    k.extend_from_slice(&id.0.to_be_bytes());
    k
}

pub fn time_range(tenant: u32, from_ms: i64, to_ms: i64) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 8);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&(from_ms as u64).to_be_bytes());
    let mut hi = Vec::with_capacity(4 + 8);
    hi.extend_from_slice(&tenant.to_be_bytes());
    hi.extend_from_slice(&(to_ms as u64).to_be_bytes());
    (lo, hi)
}

pub fn id_from_time_key(k: &[u8]) -> Option<Ulid> {
    if k.len() != 4 + 8 + 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&k[12..28]);
    Some(Ulid(u128::from_be_bytes(buf)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_keys_sort_lexicographically() {
        let a = Ulid::new();
        let b = Ulid::new();
        let k1 = time_key(1, 100, a);
        let k2 = time_key(1, 200, b);
        assert!(k1 < k2);
    }

    #[test]
    fn tenants_are_isolated() {
        let id = Ulid::new();
        let k1 = time_key(1, 1000, id);
        let k2 = time_key(2, 0, id);
        assert!(k1 < k2);
    }

    #[test]
    fn roundtrip_id() {
        let id = Ulid::new();
        let k = time_key(7, 12345, id);
        assert_eq!(id_from_time_key(&k), Some(id));
    }
}
