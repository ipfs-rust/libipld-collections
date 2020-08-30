#![allow(unused_imports, dead_code, unused_macros)]
use self::InsertError::*;
// use futures::executor::block_on;
// use futures::future::{FutureExt, LocalBoxFuture};
// use ipld_block_builder::{Cache, Codec};
use libipld::cache::{Cache, CacheConfig, IpldCache, ReadonlyCache};
use libipld::cbor::error::LengthOutOfRange;
use libipld::cbor::DagCbor;
use libipld::cbor::DagCborCodec;
use libipld::cid::Cid;
use libipld::error::Result;
use libipld::mem::MemStore;
use libipld::multihash;
use libipld::multihash::Hasher;
use libipld::multihash::Multihash;
use libipld::multihash::MultihashDigest;
use libipld::raw::RawCodec;
use libipld::store::Store;
use libipld::DagCbor;
use std::cmp::PartialEq;
use std::collections::hash_map::DefaultHasher;
use std::convert::TryInto;
use std::fmt::{self, Debug, Display};
use std::iter::once;
use std::sync::{Arc, Mutex};

const BUCKET_SIZE: usize = 1;
const BIT_WIDTH: usize = 3;
const MAP_LEN: usize = 1;
static HASH_ALG: &str = "sha2";
// for debugging purposes
const HASH_LEN: usize = 3;

const fn bytes() -> usize {
    if cfg!(test) {
        MAP_LEN
    } else {
        2_usize.pow(2_usize.pow(BIT_WIDTH as u32) as u32) / 8
    }
}

// For testing need a hash with easy collisions
fn multihash(bytes: &[u8]) -> multihash::Multihash {
    use multihash::{IdentityHasher, Multihash, Sha2_256};
    if cfg!(test) {
        Multihash::from(IdentityHasher::digest(bytes))
    } else {
        Multihash::from(Sha2_256::digest(bytes))
    }
}

macro_rules! validate {
    ($block:expr) => {
        if $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize {
            todo!("Return error: Malformed block");
        }
    };
}

macro_rules! validate_or_empty {
    ($block:expr) => {
        if $block.data.len() == 0 && *$block.map != [0; MAP_LEN]
            || $block.data.len() != 0 && $block.data.len() != popcount_all(&$block.map) as usize
        {
            todo!("Return error: Malformed block");
        }
    };
}

fn expect_next<T: DagCbor>(path_node: &PathNode<T>) -> PathHusk {
    use PathNode::*;
    match path_node {
        Idx(_) => PathHusk::Block,
        Block(_) => PathHusk::Idx,
    }
}

impl<T: DagCbor> PartialEq<PathHusk> for PathNode<T> {
    fn eq(&self, other: &PathHusk) -> bool {
        match self {
            PathNode::Block(_) if *other == PathHusk::Block => true,
            PathNode::Idx(_) if *other == PathHusk::Idx => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathHusk {
    Idx,
    Block,
}

macro_rules! valid_path {
    ($path:expr) => {
        let mut expected = PathHusk::Block;
        for elt in $path.iter() {
            if elt != expected {
                panic!("Invariant broken.");
            } else {
                expected = expect_next(elt);
            }
        }
    };
}

macro_rules! all_hashnodes_but_last {
    ($vec:expr) => {
        // let (mut data_index, _) = $vec.get($vec.len() - 1).unwrap();
        // if let Element::HashNode(_) = current.data.get(data_index) {
        //     println!("blks is {:?}", $vec);
        //     unreachable!();
        // }
        // for (parent_data_index, parent_block) in $vec[..$vec.len() - 1].iter().rev() {
        //     match parent_block.data.get(data_index).unwrap() {
        //         Element::HashNode(_) => {}
        //         Element::Bucket(_) => {
        //             println!("blks is {:?}", $vec);
        //             unreachable!();
        //         }
        //     };
        //     data_index = *parent_data_index;
        // }
    };
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PathNode<T: DagCbor> {
    Idx(usize),
    Block(Node<T>),
}

// all the information needed to retrace the path down the tree, to "bubble up" changes
// elements should switch from cid -> block -> idx -> cid -> ...
#[derive(Clone, Debug, PartialEq, Eq)]
struct Path<T: DagCbor>(Vec<PathNode<T>>);

impl<T: DagCbor> IntoIterator for Path<T> {
    type Item = PathNode<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<T: DagCbor> Path<T> {
    fn pop(&mut self) -> Option<PathNode<T>> {
        self.0.pop()
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn initialise_bubble(&mut self) -> (Node<T>, usize) {
        assert!(self.len() >= 2);
        let block = if let Some(PathNode::Block(block)) = self.0.pop() {
            block
        } else {
            unreachable!("First element to pop should be a hamt node.")
        };
        let index = if let Some(PathNode::Idx(index)) = self.0.pop() {
            index
        } else {
            unreachable!("First element to pop should be a hamt node.")
        };
        (block, index)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Bit {
    Zero,
    One,
}

// #[derive(Clone, Debug, PartialEq, Eq)]
// struct Overflow<T: DagCbor>(Vec<Entry<T>>);

#[derive(Clone, Debug, PartialEq, Eq)]
enum InsertError<T: DagCbor> {
    Id(Cid),
    Overflow(Vec<Entry<T>>),
}

impl<T: DagCbor> InsertError<T> {
    fn is_id(&self) -> bool {
        if let Id(_) = self {
            true
        } else {
            false
        }
    }
    fn is_overflow(&self) -> bool {
        if let Overflow(_) = self {
            true
        } else {
            false
        }
    }
}

// number of bits from the left up to and excluding bit itself equal 1
fn popcount(map: &[u8], bit: u8) -> u8 {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifted = if bit % 8 == 0 {
        0
    } else {
        let shifts = (7 - bit % 8) % 8 + 1;
        map[in_byte] >> shifts
    };
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

fn popcount_all(map: &[u8]) -> u8 {
    // if not true, can overflow
    debug_assert!(map.len() * 8 <= 256);
    let mut count_ones = 0x00;
    for &byte in map.iter() {
        let mut shifted = byte;
        for _bit in 0..=7 {
            count_ones += 0x01 & shifted;
            shifted >>= 1;
        }
    }
    count_ones
}

fn get_bit(map: &[u8], bit: u8) -> Bit {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let byte = map[in_byte];
    let bit = byte >> shifts & 0x01;
    if bit == 0x01 {
        Bit::One
    } else {
        Bit::Zero
    }
}

fn set_bit(map: &mut [u8], bit: u8, val: Bit) {
    debug_assert!(map.len() * 8 >= bit.into());
    let in_byte = (bit / 8) as usize;
    let shifts = (7 - bit % 8) % 8;
    let bit = 0x01 << shifts;
    match val {
        Bit::One => {
            map[in_byte] |= bit;
        }
        Bit::Zero => {
            map[in_byte] &= !bit;
        }
    }
}

#[derive(Debug)]
pub struct Hamt<S, T: DagCbor + Clone> {
    hash_alg: String,
    bit_width: usize,
    bucket_size: usize,
    nodes: IpldCache<S, DagCborCodec, Node<T>>,
    root: Cid,
}
#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub struct Node<T: DagCbor> {
    // map has 2.pow(bit_width) bits, here 256
    map: Box<[u8]>,
    data: Vec<Element<T>>,
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub enum Element<T: DagCbor> {
    HashNode(Cid),
    Bucket(Vec<Entry<T>>),
}

#[derive(Clone, Debug, Eq, PartialEq, DagCbor)]
pub struct Entry<T: DagCbor> {
    key: Vec<u8>,
    value: Data<T>,
}

struct Queue<T: DagCbor> {
    entries: Vec<EntryWithHash<T>>,
}

impl<T: DagCbor> Queue<T> {
    fn new() -> Self {
        Self { entries: vec![] }
    }

    fn pop(&mut self) -> Option<EntryWithHash<T>> {
        self.entries.pop()
    }

    fn add(&mut self, entry: Entry<T>) {
        let hash = multihash(&entry.key);
        self.entries.push(EntryWithHash { entry, hash });
    }
}

// impl<T: DagCbor>

struct EntryWithHash<T: DagCbor> {
    entry: Entry<T>,
    hash: Multihash,
}

#[derive(Clone, Debug, DagCbor, PartialEq, Eq)]
pub enum Data<T: DagCbor> {
    Value(T),
    Link(Cid),
}

impl<T: DagCbor> Data<T> {
    pub fn value(&self) -> Option<&T> {
        if let Self::Value(value) = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn cid(&self) -> Option<&Cid> {
        if let Self::Link(cid) = self {
            Some(cid)
        } else {
            None
        }
    }
}

impl<S: Store, T: Clone + DagCbor + Send + Sync> Hamt<S, T>
where
    S::Codec: Into<DagCborCodec>,
    <S as libipld::store::ReadonlyStore>::Codec: std::convert::From<DagCborCodec>,
{
    // retrace the path traveled backwards, "bubbling up" the changes
    async fn bubble_up(&mut self, mut path: Path<T>) -> Result<Cid> {
        let (mut block, mut index) = path.initialise_bubble();
        let iter = path.into_iter().rev();
        let mut cid = self.nodes.insert(block).await?;
        for elt in iter {
            match elt {
                PathNode::Idx(idx) => {
                    index = idx;
                }
                PathNode::Block(node) => {
                    block = node;
                    block.data[index] = Element::HashNode(cid);
                    cid = self.nodes.insert(block).await?;
                }
            }
        }
        Ok(cid)
    }
    pub async fn new(config: CacheConfig<S, DagCborCodec>) -> Result<Self> {
        let cache = IpldCache::new(config);
        let root = cache.insert(Node::new()).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: BUCKET_SIZE,
            bit_width: BIT_WIDTH,
            nodes: cache,
            root,
        })
    }

    pub async fn open(config: CacheConfig<S, DagCborCodec>, root: Cid) -> Result<Self> {
        let cache = IpldCache::new(config);
        // warm up the cache and make sure it's available
        cache.get(&root).await?;
        Ok(Self {
            hash_alg: HASH_ALG.to_string(),
            bucket_size: 3,
            bit_width: BIT_WIDTH,
            nodes: cache,
            root,
        })
    }

    pub async fn get(&mut self, key: Vec<u8>) -> Result<Option<Data<T>>> {
        // TODO calculate correct hash
        let hash = multihash(&key);
        let digest = hash.digest();

        let mut current = self.nodes.get(&self.root).await?;
        validate_or_empty!(current);
        // TODO remove HASH_LEN restriction
        for index in digest[0..HASH_LEN].iter() {
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
                        if elt.key == key {
                            let Entry { value, .. } = elt;
                            return Ok(Some(value));
                        }
                    }
                    return Ok(None);
                }
            };
            validate!(current);
        }
        Ok(None)
    }

    pub fn root(&self) -> &Cid {
        &self.root
    }


    // pub async fn insert(&mut self, key: Vec<u8>, value: Data<T>) -> Result<()> {
    //     // let nodes = &mut self.nodes;
    //     // TODO calculate correct hash
    //     let hash = multihash(&key);
    //     let digest = &hash.digest()[0..HASH_LEN];
    //     // start from root going down
    //     // cid points to block
    //     let mut entry_queue: Queue<T> = vec![EntryWithHash {
    //         digest,
    //         entry: Entry { digest, key, value },
    //     }];
    //     let mut hash_queue = vec![hash];
    //     let mut current = self.nodes.get(&self.root).await?;
    //     // validate_or_empty!(current);
    //     // hash_queue.push(hash);
    //     // let mut cid_next = self.root.clone();
    //     let mut blks = vec![];
    //     // let mut reinsert = vec![];
    //     // let entry = ;
    //     for lvl in 0..HASH_LEN {
    //         match status {
    //             Success => {
    //                 blks.push((data_index, current));
    //                 break;
    //             }
    //             Id(cid) => {
    //                 // fetch next block, put on stack
    //                 blks.push((
    //                     data_index,
    //                     std::mem::replace(&mut current, self.nodes.get(&cid).await?),
    //                 ));
    //                 validate!(current);
    //             }
    //             Overflow(mut overflowed) => {
    //                 // extract blocks
    //                 // mutate to empty HashNode, put on stack
    //                 // TODO: reinsert the elements
    //                 entry_queue.append(&mut overflowed);
    //                 // hash_queue.extend(overflowed.iter().map(|elt| {
    //                 //     let Entry { key, .. } = elt;
    //                 //     self::digest(key)
    //                 //     // &hash.digest()[0..HASH_LEN]
    //                 // }));
    //                 blks.push((data_index, std::mem::replace(&mut current, Node::new())));
    //             }
    //         }
    //         //TODO: move this up into insert fn on hamt
    //         if lvl == HASH_LEN - 1 {
    //             // return Err(());
    //             todo!("Output error due to maximum collision depth reached");
    //         }
    //     }
    //     // insert the extracted elements
    //     // first block special
    //     let (mut data_index, current) = blks.pop().unwrap();

    //     // let mut replace_with = Element::HashNode(propagate!(nodes.insert(current).await));
    //     let mut new_cid = self.nodes.insert(current).await?;
    //     // recalculate cids recursively
    //     for (parent_data_index, mut parent_block) in blks.into_iter().rev() {
    //         match parent_block.data.get_mut(data_index).unwrap() {
    //             Element::HashNode(old_cid) => {
    //                 *old_cid = new_cid;
    //             }
    //             new_hashnode @ Element::Bucket(_) => {
    //                 *new_hashnode = Element::HashNode(new_cid);
    //             }
    //         };
    //         data_index = parent_data_index;
    //         new_cid = self.nodes.insert(parent_block).await?;
    //     }
    //     self.root = new_cid;
    //     Ok(())
    // }
}

impl<T: DagCbor> Node<T> {
    fn new() -> Self {
        Self {
            map: Box::new([0; bytes()]),
            data: vec![],
        }
    }

    // return Overflow with the removed elements if inserting element would overflow bucket
    fn insert(
        &mut self,
        level: usize,
        entry_with_hash: EntryWithHash<T>,
        bucket_size: usize,
    ) -> Result<(),InsertError<T>> {
        use Bit::*;
        use Element::*;
        use InsertError::*;
        let hash = entry_with_hash.hash.digest();
        let map_index = hash[level];
        let bit = get_bit(&self.map, map_index);
        let data_index = popcount(&self.map, map_index) as usize;
        let EntryWithHash { entry, .. } = entry_with_hash;
        match bit {
            Zero => {
                self.data.insert(data_index, Bucket(vec![entry]));
                set_bit(&mut self.map, map_index, One);
                Ok(())
            }
            One => {
                let elt = self
                    .data
                    .get_mut(data_index)
                    .expect("data_index points past the data array");
                match elt {
                    HashNode(cid) => Err(Id(cid.clone())),
                    Bucket(ref mut bucket) => {
                        if bucket.len() < bucket_size {
                            let found = bucket
                                .iter_mut()
                                .find(|elt_mut_ref| elt_mut_ref.key == entry.key);
                            match found {
                                Some(elt) => elt.value = entry.value,
                                None => bucket.push(entry),
                            }
                            // todo!("Inserting place has to be sorted.");
                            println!("Insert not yet sorted.");
                            Ok(())
                        } else {
                            // add to elements later extracted
                            let mut overflowed = vec![entry];
                            overflowed.append(bucket);
                            Err(Overflow(overflowed))
                        }
                    }
                }
            }
        }
    }

    fn insert_all(&mut self, level: usize,queue: &mut Queue<T>) -> Result<(),InsertError<T>> {
        while let Some(entry_with_hash) = queue.pop() {
            self.insert(level,entry_with_hash,3)?
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use libipld::block::Block;
    use libipld::mem::MemStore;
    use libipld::store::ReadonlyStore;
    use libipld::store::StoreResult;
    // use libipld::store::Visibility;
    use std::collections::HashMap;
    // fn create_hamt() -> Hamt {
    //     let store = MemStore::new();
    //     let config = CacheConfig::new();
    //     Hamt::new(store,Codec::new(),)
    // }

    // use proptest_derive::Arbitrary;
    // use model::*;

    #[test]
    fn test_popcount() {
        assert_eq!(popcount(&[0b0000_0001], 0), 0);
        assert_eq!(popcount(&[0b0000_0001], 7), 0);
        assert_eq!(popcount(&[0b0000_0010], 7), 1);
        assert_eq!(popcount(&[0b0000_0011], 7), 1);
        assert_eq!(popcount(&[0b0000_0100], 7), 1);
        assert_eq!(popcount(&[0b0000_0101], 7), 1);
        assert_eq!(popcount(&[0b0000_0110], 7), 2);
        assert_eq!(popcount(&[0b0000_0111], 7), 2);
        assert_eq!(popcount(&[0b0000_1000], 7), 1);
    }
    use proptest::prelude::*;

    fn strat_bit_value() -> impl Strategy<Value = Bit> {
        prop_oneof![Just(Bit::Zero), Just(Bit::One),]
    }

    fn strat_vec_and_bit() -> impl Strategy<Value = (Vec<u8>, u8)> {
        prop::collection::vec(0..255u8, 2..32).prop_flat_map(|vec| {
            let len = vec.len();
            (Just(vec), 8..(len * 8) as u8)
        })
    }

    fn first_nonzero_to_zero(bytes: &mut [u8]) {
        for byte in bytes {
            if *byte != 0 {
                let mut shifts = 0u8;
                let original = *byte;
                let mut larger_copy = *byte as u16;
                loop {
                    *byte <<= 1;
                    larger_copy <<= 1;
                    shifts += 1;
                    if larger_copy != *byte as u16 {
                        break;
                    }
                }
                *byte >>= shifts;
                assert!((original - *byte) % 2 == 0);
            }
        }
    }

    proptest! {
        #[test]
        fn test_popcount_invariant((vec, bit) in strat_vec_and_bit()) {
            let mut shift = vec.clone();

            shift.pop();
            shift.insert(0, 0);
            assert_eq!(popcount(&shift, bit), popcount(&vec, bit - 8));
        }

        #[test]
        fn test_popcount_shift((vec, bit) in strat_vec_and_bit()) {
            let mut set_one_zero = vec.clone();
            set_bit(&mut set_one_zero, bit - 1, Bit::Zero);
            assert_eq!(popcount(&set_one_zero, bit), popcount(&vec, bit - 1));
        }

        #[test]
        fn test_set_and_get((mut vec, bit) in strat_vec_and_bit(),val in strat_bit_value()) {
            set_bit(&mut vec, bit, val.clone());
            assert_eq!(get_bit(&vec, bit), val);
        }
    }

    #[test]
    fn test_get_bit() {
        assert_eq!(get_bit(&[0b0000_0001], 7), Bit::One);
        assert_eq!(get_bit(&[0b0000_0010], 6), Bit::One);
        assert_eq!(get_bit(&[0b0000_0100], 5), Bit::One);
        assert_eq!(get_bit(&[0b0000_1000], 4), Bit::One);
        assert_eq!(get_bit(&[0b0001_0000], 3), Bit::One);
        assert_eq!(get_bit(&[0b0010_0000], 2), Bit::One);
        assert_eq!(get_bit(&[0b0100_0000], 1), Bit::One);
        assert_eq!(get_bit(&[0b1000_0000], 0), Bit::One);
    }
    // fn insert(
    //     &mut self,
    //     level: usize,
    //     entry_with_hash: EntryWithHash<T>,
    //     bucket_size: usize,
    // ) -> Insert<T>
    fn create_entry(val: u8) -> EntryWithHash<u8> {
        let entry = Entry {
            key: vec![val],
            value: Data::Value(val),
        };
        let multihash = multihash(&entry.key);
        EntryWithHash {
            hash: multihash,
            entry,
        }
    }
    fn create_colliding_entry(val: u8) -> EntryWithHash<u8> {
        let multihash = multihash(&[0_u8]);
        let entry = Entry {
            key: vec![val],
            value: Data::Value(val),
        };
        EntryWithHash {
            hash: multihash,
            entry,
        }
    }
    #[test]
    fn test_insert_into_node() {
        use super::InsertError::*;
        let mut node = Node::<u8> {
            map: Box::new([0_u8]),
            data: vec![],
        };
        assert_eq!(node.insert(0, create_colliding_entry(0), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(1), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(1), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(2), 3), Ok(()));
        assert!(node.insert(0, create_colliding_entry(2), 3).unwrap_err().is_overflow());
    }
    #[test]
    fn test_insert_into_hamt() {
        use super::InsertError::*;
        let store = MemStore::<DagCborCodec, Multihash>::new();
        let config = CacheConfig::new(store, DagCborCodec);
        // let cache = IpldCache::<_, DagCborCodec, Multihash>::new(config);
        // let mut hamt = Hamt::new(config);
        // let node = Node {data:};
        // let cid =
        // let cache = IpldCache::new(config);
        let mut node = Node::<u8> {
            map: Box::new([0_u8]),
            data: vec![],
        };
        assert_eq!(node.insert(0, create_colliding_entry(0), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(1), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(1), 3), Ok(()));
        assert_eq!(node.insert(0, create_colliding_entry(2), 3), Ok(()));
        assert!(node.insert(0, create_colliding_entry(2), 3).unwrap_err().is_overflow());
        // dbg!(node.insert(0, create_colliding_entry(1), 3));
        // dbg!(node.insert(0, create_entry(1), 3));
        // dbg!(node);
        // node.insert(0, create_colliding_entry(0), 1);
        // dbg!(node.insert(0, create_colliding_entry(1), 1));
        // dbg!(multihash);
    }

    #[derive(Debug, Clone)]
    struct MemCache<T> {
        entries: Vec<T>,
    }

    // proptest! {
    //     #[test]
    //     fn test_hamt_set_and_get(batch in prop::collection::vec((prop::collection::vec(0..2u8,HASH_LEN),0..1u64), HASH_LEN)) {
    //         let store = DummyStore::new();
    //         let mut hamt = block_on(Hamt::new(store, 5)).unwrap();
    //         let _ = block_on(test_batch_hamt_set_and_get(&mut hamt,batch)).unwrap();
    //         dbg!(hamt.nodes);
    //     }
    // }

    // #[test]
    // fn debug_hamt_set_and_get() {
    //     let batch = vec![(vec![0, 0, 0], 0), (vec![0, 0, 1], 0), (vec![0, 0, 1], 0)];
    //     // let batch = vec![(vec![0, 0, 0], 0)];
    //     let store = DummyStore::new();
    //     let mut hamt = block_on(Hamt::new(store, 5)).unwrap();
    //     let _ = block_on(test_batch_hamt_set_and_get(&mut hamt, batch)).unwrap();
    //     // dbg!(hamt.nodes);
    // }
    // #[async_std::test]
    // async fn test_batch_hamt_set_and_get<S: Store>(
    //     hamt: &mut Hamt<S, u64>,
    //     batch: Vec<(Vec<u8>, u64)>,
    // ) -> Result<()> {
    //     for elt in batch.into_iter() {
    //         let key = elt.0;
    //         let val = elt.1.clone();
    //         hamt.insert(key.clone(), Data::Value(val)).await?;
    // println!("{:?}",hamt);
    // let elt = hamt.get(key).await?;
    // assert_eq!(elt, Some(Data::Value(val)));
    //     }
    //     Ok(())
    // }
}

// pub async fn del(&mut self, key: Vec<u8>) -> Result<()> {
//     let nodes = &mut self.nodes;
//     // TODO calculate correct hash
//     let mut hasher = Sha256::new();
//     hasher.update(&key);
//     let hash = hasher.finalize();

//     // start from root going down
//     // cid points to block
//     let mut block = nodes.get(&self.root).await?;
//     let mut cid_next = self.root.clone();
//     let mut blks = vec![];

//     for (count, &byte_index) in hash.iter().enumerate() {
//         let data_index = popcount(&block.map, byte_index) as usize;
//         let bit = get_bit(&block.map, byte_index);
//         match bit {
//             Bit::Zero => {
//                 return Ok(());
//             }
//             Bit::One => {
//                 if count == hash.len() {
//                     // return Err(());
//                     todo!("Output error due to maximum collision depth reached");
//                 }
//                 let elt = block
//                     .data
//                     .get_mut(data_index)
//                     .expect("data_index points past the data array");
//                 match elt {
//                     Element::HashNode(cid) => {
//                         cid_next = cid.clone();
//                         blks.push((
//                             data_index,
//                             std::mem::replace(&mut block, nodes.get(&cid_next).await?),
//                         ));
//                         continue;
//                     }
//                     Element::Bucket(ref mut bucket) => {
//                         if bucket.len() < self.bucket_size {
//                             // todo!("Inserting place has to be sorted.");
//                             println!("Insert not yet sorted.");
//                             bucket.push(Entry { key, value });
//                         } else {
//                             // mutate entry to empty node
//                             reinsert.append(bucket);
//                         }
//                     }
//                 }
//                 blks.push((data_index, block));
//                 break;
//             }
//         }
//     }
// }

// #[test]
// fn popdbg() {
//     let (mut vec, mut bit) = (vec![0, 0], 14);
//     let mut set_one_zero = vec.clone();

//     set_bit(&mut set_one_zero, bit, Bit::Zero);
//     let left = popcount(&set_one_zero, bit);
//     let right = popcount(&vec, bit - 1);
//     let mut binary_left = set_one_zero[0] as u16;
//     binary_left <<= 8;
//     // println!("{:b}",binary_left);
//     binary_left |= set_one_zero[1] as u16;
//     // println!("{:b}",binary_left);
//     let mut binary_right = vec[0] as u16;
//     binary_right <<= 8;
//     // println!("{:b}",binary_right);
//     binary_right |= vec[1] as u16;
//     // println!("{:b}",binary_right);
//     println!(
//         "left: ({0:016b},{1}), right: ({2:016b},{3})",
//         binary_left,
//         bit,
//         binary_right,
//         bit - 1
//     );
//     dbg!(left);
//     dbg!(right);
// }
