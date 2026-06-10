use crate::IndexKey;
use ulid::Ulid;

pub(super) fn model_hash(model: &str) -> u32 {
    let mut h: u32 = 0x811C9DC5;
    for b in model.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

pub(super) fn model_hash_from_index_key(key: &IndexKey) -> u32 {
    key.model
        .strip_prefix("__h::")
        .and_then(|hex| u32::from_str_radix(hex, 16).ok())
        .unwrap_or_else(|| model_hash(&key.model))
}

pub(super) fn meta_key_bytes(tenant: u32, model: &str, node_id: Ulid) -> Vec<u8> {
    meta_key_bytes_for_hash(tenant, model_hash(model), node_id)
}

pub(super) fn meta_key_bytes_for_hash(tenant: u32, mh: u32, node_id: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&node_id.0.to_be_bytes());
    k
}

pub(super) fn meta_range_bytes(tenant: u32, model: &str) -> (Vec<u8>, Vec<u8>) {
    meta_range_bytes_for_hash(tenant, model_hash(model))
}

pub(super) fn meta_range_bytes_for_hash(tenant: u32, mh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&mh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 16]);
    (lo, hi)
}

pub(super) fn node_id_from_meta_key(key: &[u8]) -> Option<Ulid> {
    if key.len() != 4 + 4 + 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&key[8..24]);
    Some(Ulid(u128::from_be_bytes(buf)))
}

pub(super) fn decode_meta_value(value: &[u8]) -> Option<(u64, Vec<f32>)> {
    if value.len() < 12 {
        return None;
    }
    let internal_id = u64::from_be_bytes(value[0..8].try_into().ok()?);
    let dim = u32::from_be_bytes(value[8..12].try_into().ok()?);
    let expected = 12 + dim as usize * 4;
    if value.len() != expected {
        return None;
    }
    let vector = value[12..]
        .chunks_exact(4)
        .map(|chunk| Some(f32::from_le_bytes(chunk.try_into().ok()?)))
        .collect::<Option<Vec<_>>>()?;
    Some((internal_id, vector))
}

pub(super) fn rev_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    rev_key_bytes_for_hash(tenant, model_hash(model), internal_id)
}

pub(super) fn rev_key_bytes_for_hash(tenant: u32, mh: u32, internal_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 4 + 8);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&mh.to_be_bytes());
    k.extend_from_slice(&internal_id.to_be_bytes());
    k
}

pub(super) fn tomb_key_bytes(tenant: u32, model: &str, internal_id: u64) -> Vec<u8> {
    rev_key_bytes(tenant, model, internal_id)
}

pub(super) fn tomb_range_bytes_for_hash(tenant: u32, mh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&mh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff; 8]);
    (lo, hi)
}
