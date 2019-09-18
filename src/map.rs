use core::marker::PhantomData;
use dag_cbor_derive::DagCbor;
use libipld::{BlockStore, Cache, Cid, Hash, Ipld, Result, Store};

pub struct Map<TStore: Store, TCache: Cache, THash: Hash> {
    prefix: PhantomData<THash>,
    store: BlockStore<TStore, TCache>,
    root: Cid,
}

impl<TStore: Store, TCache: Cache, THash: Hash> Map<TStore, TCache, THash> {
    pub fn load(store: BlockStore<TStore, TCache>, root: Cid) -> Self {
        Self {
            prefix: PhantomData,
            store,
            root,
        }
    }

    pub async fn new(
        store: BlockStore<TStore, TCache>,
        hash: String,
        bit_width: u32,
        bucket_size: u32,
    ) -> Result<Self> {
        let root = Root::new(hash, bit_width, bucket_size);
        let root = store.write_cbor::<THash, _>(&root)?;
        Ok(Self {
            prefix: PhantomData,
            store,
            root,
        })
    }
}

#[derive(Debug, DagCbor)]
struct Root {
    #[ipld(name = "hashAlg")]
    hash_alg: String,
    #[ipld(name = "bitWidth")]
    bit_width: u32,
    #[ipld(name = "bucketSize")]
    bucket_size: u32,
    map: Vec<u8>,
    data: Vec<Element>,
}

impl Root {
    pub fn new(hash_alg: String, bit_width: u32, bucket_size: u32) -> Self {
        Self {
            hash_alg,
            bit_width,
            bucket_size,
            map: Default::default(),
            data: Default::default(),
        }
    }
}

#[derive(Debug, DagCbor)]
struct Node {
    map: Vec<u8>,
    data: Vec<Element>,
}

#[derive(Debug, DagCbor)]
#[ipld(repr = "kinded")]
enum Element {
    Node(Node),
    Link(Cid),
    Bucket(Vec<Entry>),
}

#[derive(Debug, DagCbor)]
#[ipld(repr = "list")]
struct Entry {
    key: Vec<u8>,
    value: Ipld,
}
