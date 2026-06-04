use mmdb_core::{MemoryNode, Result};

pub fn encode_node(n: &MemoryNode) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(n)?)
}

pub fn decode_node(b: &[u8]) -> Result<MemoryNode> {
    Ok(serde_json::from_slice(b)?)
}
