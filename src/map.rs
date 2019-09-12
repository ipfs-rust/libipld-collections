use ipld_derive::Ipld;
use libipld::{Cid, Ipld, IpldStore, Prefix, Result};
use core::marker::PhantomData;

#[derive(Debug)]
pub struct Map<TPrefix: Prefix, TStore: IpldStore> {
    prefix: PhantomData<TPrefix>,
    store: TStore,
    root: Cid,
}

impl<TPrefix: Prefix, TStore: IpldStore> Map<TPrefix, TStore> {
    pub fn load(root: Cid) -> Self {
        let store: TStore = Default::default();
        Self {
            prefix: PhantomData,
            store,
            root,
        }
    }

    pub fn new(hash: String, bit_width: u32, bucket_size: u32) -> Result<Self> {
        let mut store: TStore = Default::default();
        let root = Root::new(hash, bit_width, bucket_size);
        let root = store.write::<TPrefix, _>(&root)?;
        Ok(Self {
            prefix: PhantomData,
            store,
            root,
        })
    }
}

#[derive(Debug, Ipld)]
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

#[derive(Debug, Ipld)]
struct Node {
    map: Vec<u8>,
    data: Vec<Element>,
}

#[derive(Debug, Ipld)]
//#[ipld(repr = "kinded")]
enum Element {
    Node(Node),
    Link(Cid),
    Bucket(Vec<Entry>),
}

#[derive(Debug, Ipld)]
#[ipld(repr = "list")]
struct Entry {
    key: Vec<u8>,
    value: Ipld
}
