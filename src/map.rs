#![allow(dead_code)] // TODO
use crate::Result;
use core::marker::PhantomData;
use libipld::hash::Hash;
use libipld::store::{Store, StoreCborExt};
use libipld::{Cid, DagCbor, Ipld};

pub struct Map<TStore: Store, THash: Hash> {
    prefix: PhantomData<THash>,
    store: TStore,
    root: Cid,
}

impl<TStore: Store, THash: Hash> Map<TStore, THash> {
    pub fn load(store: TStore, root: Cid) -> Self {
        Self {
            prefix: PhantomData,
            store,
            root,
        }
    }

    pub async fn new(
        store: TStore,
        hash: String,
        bit_width: u32,
        bucket_size: u32,
    ) -> Result<Self> {
        let root = Root::new(hash, bit_width, bucket_size);
        let root = store.write_cbor::<THash, _>(&root).await?;
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
