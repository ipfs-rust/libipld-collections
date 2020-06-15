#![allow(unused_imports, dead_code)]
use futures::future::{FutureExt, LocalBoxFuture};
use ipld_block_builder::{Cache, Codec};
use libipld::cbor::DagCbor;
use libipld::cid::Cid;
use libipld::error::Result;
use libipld::store::Store;
use libipld::DagCbor;
use sha2::{Digest, Sha256};
use std::collections::hash_map::DefaultHasher;
use std::future::Future;
use std::iter::once;

static BUCKET_SIZE: usize = 3;
static HASH_ALG: &str = "sha2";

macro_rules! bttf {
    ($res:expr) => {
        match $res {
            Ok(block) => block,
            Err(e) => return Box::pin(futures::future::err(e)),
        }
    };
}

enum Bit {
    Zero,
    One,
}

// #bits from the left up to and excluding bit itself equal 1
fn popcount(map: &[u8], bit: u8) -> u8 {
    debug_assert_ne!(bit, 0);
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = ((bit - 1) / 8) as usize;
    let shifts = (8 - bit + 1) % 8;
    let shifted = map[in_byte] >> shifts;
    let mut count_ones = 0x00;
    for &byte in map[0..in_byte].iter().chain(once(&shifted)) {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

// assumes bit != 0
fn get_bit(map: &[u8], bit: u8) -> Bit {
    debug_assert_ne!(bit, 0);
    debug_assert!(map.len() * 8 >= bit.into());
    let which_byte = ((bit - 1) / 8) as usize;
    let shifts = (8 - bit) % 8;
    let byte = map[which_byte];
    let bit = byte >> shifts & 0x01;
    if bit == 0x01 {
        Bit::One
    } else {
        Bit::Zero
    }
}

fn set_bit(map: &mut [u8], bit: u8) {
    debug_assert!(bit != 0);
    let which_byte = ((bit - 1) / 8) as usize;
    let shifts = (8 - bit) % 8;
    let bit = 0x01 << shifts;
    map[which_byte] |= bit;
}

pub struct Hamt<S, T: Clone + DagCbor> {
    hash_alg: String,
    bit_width: usize,
    bucket_size: usize,
    nodes: Cache<S, Codec, Node<T>>,
    root: Cid,
}

#[derive(Clone, Debug, DagCbor)]
pub enum Data<T: DagCbor> {
    Value(T),
    Link(Cid),
}

impl<T: DagCbor> Data<T> {
    fn value(&self) -> Option<&T> {
        if let Self::Value(value) = self {
            Some(value)
        } else {
            None
        }
    }

    fn cid(&self) -> Option<&Cid> {
        if let Self::Link(cid) = self {
            Some(cid)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, DagCbor)]
struct Node<T: DagCbor> {
    // map as 2.pow(bit_width) bits, here 256, so 32 bytes
    map: Vec<u8>,
    data: Vec<Element<T>>,
}

#[derive(Clone, Debug, DagCbor)]
enum Element<T: DagCbor> {
    HashNode(Cid),
    Bucket(Vec<Entry<T>>),
}

#[derive(Clone, Debug, DagCbor)]
struct Entry<T: DagCbor> {
    key: Vec<u8>,
    value: Data<T>,
}

impl<S: Store, T: Clone + DagCbor> Hamt<S, T> {
    pub async fn new(store: S, cache_size: usize) -> Result<Self> {
        let mut nodes = Cache::new(store, Codec::new(), cache_size);
        let root = nodes.insert(Node::new()).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: BUCKET_SIZE,
            bit_width: 3,
            nodes,
            root,
        })
    }

    pub async fn open(store: S, cache_size: usize, root: Cid) -> Result<Self> {
        let mut nodes = Cache::new(store, Codec::new(), cache_size);
        // warm up the cache and make sure it's available
        nodes.get(&root).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: 3,
            bit_width: 3,
            nodes,
            root,
        })
    }

    pub async fn get(&mut self, search_key: Vec<u8>) -> Result<Option<Data<T>>> {
        // TODO calculate correct hash
        let mut hasher = Sha256::new();
        hasher.update(&search_key);
        let hash = hasher.finalize();

        let mut current = self.nodes.get(&self.root).await?;
        for index in &hash {
            let bit = get_bit(&current.map, *index);
            if let Bit::Zero = bit {
                return Ok(None);
            }
            let data_index = popcount(&current.map, *index) as usize;
            let Node { mut data, .. } = current;
            current = match data.remove(data_index) {
                Element::HashNode(cid) => self.nodes.get(&cid).await?,
                Element::Bucket(bucket) => {
                    for elt in bucket {
                        if elt.key == search_key {
                            let Entry { value, .. } = elt;
                            return Ok(Some(value));
                        }
                    }
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }
    pub fn root(&self) -> &Cid {
        &self.root
    }

    pub async fn set<'a>(
        &'a mut self,
        key: Vec<u8>,
        value: Data<T>,
    ) -> LocalBoxFuture<'a, Result<()>> {
        let nodes = &mut self.nodes;
        // TODO calculate correct hash
        let mut hasher = Sha256::new();
        hasher.update(&key);
        let hash = hasher.finalize();

        // start from root going down
        // cid points to block
        let mut block = bttf!(nodes.get(&self.root).await);
        let mut cid_next = self.root.clone();
        let mut blks = vec![];
        let mut reinsert = vec![];

        for (count, &byte_index) in hash.iter().enumerate() {
            let data_index = popcount(&block.map, byte_index) as usize;
            let bit = get_bit(&block.map, byte_index);
            match bit {
                Bit::Zero => {
                    block
                        .data
                        .insert(data_index, Element::Bucket(vec![Entry { key, value }]));
                    set_bit(&mut block.map, byte_index);
                    blks.push((data_index, block));
                    break;
                }
                Bit::One => {
                    if count == hash.len() {
                        // return Err(());
                        todo!("Output error due to maximum collision depth reached");
                    }
                    let elt = block
                        .data
                        .get_mut(data_index)
                        .expect("data_index points past the data array");
                    match elt {
                        Element::HashNode(cid) => {
                            cid_next = cid.clone();
                            blks.push((
                                data_index,
                                std::mem::replace(&mut block, bttf!(nodes.get(&cid_next).await)),
                            ));
                            continue;
                        }
                        Element::Bucket(ref mut bucket) => {
                            if bucket.len() < self.bucket_size {
                                // todo!("Inserting place has to be sorted.");
                                println!("Insert not yet sorted.");
                                bucket.push(Entry { key, value });
                            } else {
                                // mutate entry to empty node
                                reinsert.append(bucket);
                            }
                        }
                    }
                    blks.push((data_index, block));
                    break;
                }
            }
        }
        // first block special
        let (mut data_index, block) = blks.pop().unwrap();
        let mut replace_with = Element::HashNode(bttf!(nodes.insert(block).await));
        // recalculate cids recursively
        for (parent_data_index, mut parent_block) in blks.into_iter().rev() {
            *parent_block.data.get_mut(data_index).unwrap() = replace_with;
            data_index = parent_data_index;
            replace_with = Element::HashNode(bttf!(nodes.insert(parent_block).await));
        }
        async move {
            let Entry { key, value } = reinsert.pop().expect("There should be three elements.");
            let first = self.set(key, value).await.await;
            let Entry { key, value } = reinsert.pop().expect("There should be three elements.");
            let second = self.set(key, value).await.await;
            let Entry { key, value } = reinsert.pop().expect("There should be three elements.");
            let third = self.set(key, value).await.await;

            first.or(second).or(third)
        }
        .boxed_local()
    }
}

impl<T: DagCbor> Node<T> {
    fn new() -> Self {
        Node {
            map: vec![0; 32],
            data: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // use async_std::task;
    // use libipld::mem::MemStore;
    // use model::*;
    #[test]
    fn poptest() {
        assert_eq!(popcount(&[0x01], 8), 0);
        assert_eq!(popcount(&[0x02], 8), 1);
        assert_eq!(popcount(&[0x03], 8), 1);
        assert_eq!(popcount(&[0x04], 8), 1);
        assert_eq!(popcount(&[0x05], 8), 1);
        assert_eq!(popcount(&[0x06], 8), 2);
        assert_eq!(popcount(&[0x07], 8), 2);
        assert_eq!(popcount(&[0x08], 8), 1);
    }
}
