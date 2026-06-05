//! Bi-directional edge storage + BFS-style neighbour traversal.
//!
//! Persistence (P2):
//! - `edges_out` key=[tenant|src|label_hash|dst]  val=encoded Edge
//! - `edges_in`  key=[tenant|dst|label_hash|src]  val=[]   (presence marker)
//!
//! The two partitions together let us scan all outgoing edges of a node in
//! O(neighbours) time, optionally filtered by label, and symmetrically all
//! incoming edges. The edge payload itself lives only in `edges_out`; the
//! reverse partition stores just keys so updates stay cheap.
use fjall::{Keyspace, PartitionCreateOptions, PartitionHandle, PersistMode};
use mmdb_core::{Edge, Error, Result};
use std::collections::{HashSet, VecDeque};
use ulid::Ulid;

const PART_OUT: &str = "edges_out";
const PART_IN: &str = "edges_in";

/// Persistent property-graph store backed by two `fjall` partitions.
pub struct GraphStore {
    keyspace: Keyspace,
    out: PartitionHandle,
    inn: PartitionHandle,
}

/// Which side of an edge a traversal follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Follow outgoing edges (`src -> dst`).
    Out,
    /// Follow incoming edges (`dst <- src`).
    In,
    /// Follow both directions, deduplicating visited nodes.
    Both,
}

impl GraphStore {
    /// Open the graph store against an existing fjall keyspace.
    /// Sharing the keyspace with [`mmdb_storage::Storage`] lets writes from
    /// both crates land in the same WAL.
    pub fn open(keyspace: Keyspace) -> Result<Self> {
        let out = keyspace
            .open_partition(PART_OUT, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        let inn = keyspace
            .open_partition(PART_IN, PartitionCreateOptions::default())
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(Self { keyspace, out, inn })
    }

    /// Insert an edge, writing both forward and reverse index entries
    /// inside a single fjall batch.
    pub fn add_edge(&self, tenant: u32, edge: Edge) -> Result<()> {
        let lh = label_hash(&edge.label);
        let kout = out_key(tenant, edge.src, lh, edge.dst);
        let kin = in_key(tenant, edge.dst, lh, edge.src);
        let val = serde_json::to_vec(&edge).map_err(Error::from)?;

        let mut batch = self.keyspace.batch();
        batch.insert(&self.out, kout, val);
        batch.insert(&self.inn, kin, []);
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// Remove an edge identified by `(src, dst, label)` from both partitions.
    pub fn remove_edge(&self, tenant: u32, src: Ulid, dst: Ulid, label: &str) -> Result<()> {
        let lh = label_hash(label);
        let kout = out_key(tenant, src, lh, dst);
        let kin = in_key(tenant, dst, lh, src);
        let mut batch = self.keyspace.batch();
        batch.remove(&self.out, kout);
        batch.remove(&self.inn, kin);
        batch.commit().map_err(|e| Error::Storage(e.to_string()))?;
        self.keyspace
            .persist(PersistMode::SyncAll)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(())
    }

    /// All outgoing edges, optionally filtered by label.
    pub fn neighbours_out(
        &self,
        tenant: u32,
        node: Ulid,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let (lo, hi) = match label {
            Some(l) => out_label_range(tenant, node, label_hash(l)),
            None => out_node_range(tenant, node),
        };
        let mut out = Vec::new();
        for kv in self.out.range(lo..hi) {
            let (_, v) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            let e: Edge = serde_json::from_slice(&v).map_err(Error::from)?;
            if let Some(l) = label {
                if e.label != l {
                    continue;
                }
            }
            out.push(e);
        }
        Ok(out)
    }

    /// All incoming edges. We materialise the edges by going back to `out`
    /// using the (src, dst, label_hash) tuple recorded in the in-key.
    pub fn neighbours_in(
        &self,
        tenant: u32,
        node: Ulid,
        label: Option<&str>,
    ) -> Result<Vec<Edge>> {
        let (lo, hi) = match label {
            Some(l) => in_label_range(tenant, node, label_hash(l)),
            None => in_node_range(tenant, node),
        };
        let mut out = Vec::new();
        for kv in self.inn.range(lo..hi) {
            let (k, _) = kv.map_err(|e| Error::Storage(e.to_string()))?;
            // k = [tenant(4) | dst(16) | lh(4) | src(16)]
            if k.len() != 4 + 16 + 4 + 16 {
                continue;
            }
            let mut lh_buf = [0u8; 4];
            lh_buf.copy_from_slice(&k[20..24]);
            let lh = u32::from_be_bytes(lh_buf);
            let mut src_buf = [0u8; 16];
            src_buf.copy_from_slice(&k[24..40]);
            let src = Ulid(u128::from_be_bytes(src_buf));
            let kout = out_key(tenant, src, lh, node);
            if let Some(v) =
                self.out.get(&kout).map_err(|e| Error::Storage(e.to_string()))?
            {
                let e: Edge = serde_json::from_slice(&v).map_err(Error::from)?;
                if let Some(l) = label {
                    if e.label != l {
                        continue;
                    }
                }
                out.push(e);
            }
        }
        Ok(out)
    }

    /// BFS traversal up to `max_depth` hops. Returns node ids in discovery
    /// order (excluding the seed). `direction` controls which edges are
    /// followed at each frontier expansion.
    pub fn bfs(
        &self,
        tenant: u32,
        seed: Ulid,
        max_depth: usize,
        direction: Direction,
        label: Option<&str>,
    ) -> Result<Vec<Ulid>> {
        if max_depth == 0 {
            return Ok(Vec::new());
        }
        let mut visited: HashSet<Ulid> = HashSet::new();
        visited.insert(seed);
        let mut order: Vec<Ulid> = Vec::new();
        let mut frontier: VecDeque<(Ulid, usize)> = VecDeque::new();
        frontier.push_back((seed, 0));

        while let Some((node, depth)) = frontier.pop_front() {
            if depth >= max_depth {
                continue;
            }
            let mut next: Vec<Ulid> = Vec::new();
            if matches!(direction, Direction::Out | Direction::Both) {
                for e in self.neighbours_out(tenant, node, label)? {
                    next.push(e.dst);
                }
            }
            if matches!(direction, Direction::In | Direction::Both) {
                for e in self.neighbours_in(tenant, node, label)? {
                    next.push(e.src);
                }
            }
            for n in next {
                if visited.insert(n) {
                    order.push(n);
                    frontier.push_back((n, depth + 1));
                }
            }
        }
        Ok(order)
    }
}

fn label_hash(label: &str) -> u32 {
    let mut h: u32 = 0x811C9DC5;
    for b in label.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn out_key(tenant: u32, src: Ulid, lh: u32, dst: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 16 + 4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&src.0.to_be_bytes());
    k.extend_from_slice(&lh.to_be_bytes());
    k.extend_from_slice(&dst.0.to_be_bytes());
    k
}

fn in_key(tenant: u32, dst: Ulid, lh: u32, src: Ulid) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + 16 + 4 + 16);
    k.extend_from_slice(&tenant.to_be_bytes());
    k.extend_from_slice(&dst.0.to_be_bytes());
    k.extend_from_slice(&lh.to_be_bytes());
    k.extend_from_slice(&src.0.to_be_bytes());
    k
}

fn out_node_range(tenant: u32, src: Ulid) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 16);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&src.0.to_be_bytes());
    let mut hi = lo.clone();
    // bump last byte of src by appending 0xff*4 + 0xff*16 sentinel — easier:
    // hi prefix [tenant | src+1] if src not max; fall back to lo + 0xff*20.
    hi.extend_from_slice(&[0xff_u8; 20]);
    (lo, hi)
}

fn out_label_range(tenant: u32, src: Ulid, lh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 16 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&src.0.to_be_bytes());
    lo.extend_from_slice(&lh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff_u8; 16]);
    (lo, hi)
}

fn in_node_range(tenant: u32, dst: Ulid) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 16);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&dst.0.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff_u8; 20]);
    (lo, hi)
}

fn in_label_range(tenant: u32, dst: Ulid, lh: u32) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(4 + 16 + 4);
    lo.extend_from_slice(&tenant.to_be_bytes());
    lo.extend_from_slice(&dst.0.to_be_bytes());
    lo.extend_from_slice(&lh.to_be_bytes());
    let mut hi = lo.clone();
    hi.extend_from_slice(&[0xff_u8; 16]);
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::Config;
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    fn mk_edge(src: Ulid, dst: Ulid, label: &str) -> Edge {
        Edge {
            src, dst, label: label.into(), weight: 1.0,
            created_at_ms: 0, metadata: BTreeMap::new(),
        }
    }

    fn open() -> (tempfile::TempDir, GraphStore) {
        let dir = tempdir().unwrap();
        let ks = Config::new(dir.path()).open().unwrap();
        (dir, GraphStore::open(ks).unwrap())
    }

    #[test]
    fn add_then_list_out() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new(); let c = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "rel")).unwrap();
        g.add_edge(0, mk_edge(a, c, "rel")).unwrap();
        let outs = g.neighbours_out(0, a, None).unwrap();
        assert_eq!(outs.len(), 2);
        assert!(outs.iter().any(|e| e.dst == b));
        assert!(outs.iter().any(|e| e.dst == c));
    }

    #[test]
    fn label_filter_works() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "knows")).unwrap();
        g.add_edge(0, mk_edge(a, b, "likes")).unwrap();
        let knows = g.neighbours_out(0, a, Some("knows")).unwrap();
        assert_eq!(knows.len(), 1);
        assert_eq!(knows[0].label, "knows");
    }

    #[test]
    fn in_edges_round_trip() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "rel")).unwrap();
        let ins = g.neighbours_in(0, b, None).unwrap();
        assert_eq!(ins.len(), 1);
        assert_eq!(ins[0].src, a);
    }

    #[test]
    fn remove_drops_both_sides() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "rel")).unwrap();
        g.remove_edge(0, a, b, "rel").unwrap();
        assert!(g.neighbours_out(0, a, None).unwrap().is_empty());
        assert!(g.neighbours_in(0, b, None).unwrap().is_empty());
    }

    #[test]
    fn bfs_two_hop() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new(); let c = Ulid::new(); let d = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "r")).unwrap();
        g.add_edge(0, mk_edge(b, c, "r")).unwrap();
        g.add_edge(0, mk_edge(c, d, "r")).unwrap();

        let one = g.bfs(0, a, 1, Direction::Out, None).unwrap();
        assert_eq!(one, vec![b]);

        let two = g.bfs(0, a, 2, Direction::Out, None).unwrap();
        assert_eq!(two, vec![b, c]);

        let three = g.bfs(0, a, 3, Direction::Out, None).unwrap();
        assert_eq!(three, vec![b, c, d]);
    }

    #[test]
    fn tenant_isolation() {
        let (_d, g) = open();
        let a = Ulid::new(); let b = Ulid::new();
        g.add_edge(0, mk_edge(a, b, "r")).unwrap();
        assert!(g.neighbours_out(1, a, None).unwrap().is_empty());
        assert_eq!(g.neighbours_out(0, a, None).unwrap().len(), 1);
    }
}
